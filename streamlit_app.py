# streamlit_app.py  – Prebid Integration Monitor (orjson-optional, fixed)

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, json
from collections import Counter
from typing import List, Dict, Any

# ---------- optional fast JSON ----------
try:
    import orjson

    def jloads(b: bytes | str):
        return orjson.loads(b if isinstance(b, bytes) else b.encode())

    def jdumps(o):
        return orjson.dumps(o, option=orjson.OPT_INDENT_2)
except ModuleNotFoundError:
    import json as _stdjson

    def jloads(b: bytes | str):
        return _stdjson.loads(b if isinstance(b, str) else b.decode())

    def jdumps(o):
        return _stdjson.dumps(o, indent=2).encode()

# -------------------------------------------------
# 🎨  Page config & CSS
# -------------------------------------------------
st.set_page_config(page_title="Prebid Integration Monitor",
                   page_icon="📊", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown(
    """
    <style>
    html,body,[class*='css']{font-family:"Helvetica Neue",Arial,sans-serif;}
    .block-container{padding-top:3rem;padding-bottom:2rem;}
    [data-testid='stMetricValue']{font-size:1.75rem;font-weight:600;}
    footer{visibility:hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

import matplotlib as mpl
mpl.rcParams["text.usetex"] = False
mpl.rcParams["mathtext.default"] = "regular"

# -------------------------------------------------
# 📦  HTTP session & constants
# -------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-Monitor-App"})

token = st.secrets.get("github_token")  # type: ignore[attr-defined]
if token:
    SESSION.headers["Authorization"] = f"token {token}"

ORG, REPO = "prebid", "prebid-integration-monitor"
COMBINED_URL = (
    "https://raw.githubusercontent.com/"
    f"{ORG}/{REPO}/main/output/prebid_combined.json.gz"
)
# legacy month-walk (branch jlist)
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"
RAW_BASE = f"https://raw.githubusercontent.com/{ORG}/{REPO}/jlist/"

# -------------------------------------------------
# ⚡  Slim-object helper
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "siteKey": it.get("site") or it.get("domain")
                 or it.get("url") or it.get("pageUrl"),
        "version": it.get("version"),
        "modules": it.get("modules", []),
        "libraries": it.get("libraries", []),
        "prebidInstances": [
            {
                "version": inst.get("version"),
                "modules": inst.get("modules", []),
                "globalVarName": inst.get("globalVarName"),
            }
            for inst in it.get("prebidInstances", [])
        ],
    }

# -------------------------------------------------
# 📥  Data loaders
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_combined_feed() -> List[Dict[str, Any]]:
    r = SESSION.get(COMBINED_URL, timeout=60)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = jloads(gz.read())
    return [slim_item(x) for x in raw]

@st.cache_data(show_spinner=True)
def load_all_months(limit_months: int | None = None) -> List[Dict[str, Any]]:
    top = SESSION.get(f"{API_BASE}?ref=jlist", timeout=30)
    if top.status_code == 403 and not token:
        st.error("GitHub API rate-limit hit – add a token or switch to combined feed.")
        return []
    top.raise_for_status()

    months = sorted(
        [m for m in top.json() if m["type"] == "dir"],
        key=lambda m: m["name"], reverse=True
    )
    if limit_months:
        months = months[:limit_months]

    combined: list[dict] = []
    for month in months:
        try:
            files = SESSION.get(month["url"], timeout=30).json()
        except Exception as e:
            st.warning(f"⚠️  skip {month['name']}: {e}")
            continue
        for f in [f for f in files if f["type"] == "file" and f["name"].endswith(".json")]:
            raw_url = f.get("download_url") or RAW_BASE + f["path"]
            try:
                combined.extend(slim_item(x) for x in SESSION.get(raw_url, timeout=30).json())
            except Exception as e:
                st.warning(f"   ↳ skip {f['name']} ({month['name']}) → {e}")
    return combined

def load_uploaded_json(file):
    try:
        return [slim_item(x) for x in json.load(file)]
    except json.JSONDecodeError:
        st.error("Uploaded file is not valid JSON.")
        return None

# -------------------------------------------------
# 🏷️  Extraction helpers
# -------------------------------------------------
def categorize_version(v: str | None) -> str:
    if not v:
        return "Other"
    v = v.lstrip("v")
    try:
        major = int(re.split(r"[.-]", v)[0])
    except ValueError:
        return "Other"
    if major <= 2:  return "0.x-2.x"
    if 3 <= major <= 5: return "3.x-5.x"
    if 6 <= major <= 7: return "6.x-7.x"
    if major == 8: return "8.x"
    if major == 9: return "9.x"
    return "Other"

def classify_module(n: str) -> str:
    if "BidAdapter" in n:                          return "Bid Adapter"
    if any(x in n for x in ("RtdProvider","rtdModule")): return "RTD Module"
    if any(x in n for x in ("IdSystem","userId")):       return "ID System"
    if any(x in n for x in ("Analytics","analyticsAdapter")): return "Analytics Adapter"
    return "Other"

def extract_versions(it):
    v = [it["version"]] if it.get("version") else []
    v += [i["version"] for i in it["prebidInstances"] if i.get("version")]
    return v

def extract_modules(it):
    mods = list(it["modules"])
    for inst in it["prebidInstances"]:
        mods.extend(inst["modules"])
    return mods

def count_prebid_instances(it):
    return len(it["prebidInstances"]) or (1 if it.get("version") else 0)

def extract_libraries(it):       return it["libraries"]
def extract_global_names(it):    return [i["globalVarName"] for i in it["prebidInstances"] if i.get("globalVarName")]

# -------------------------------------------------
# 📊  DataFrames
# -------------------------------------------------
VERSION_ORDER    = ["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","Other"]
INSTANCE_BUCKETS = ["0","1","2","3","4","5","6+"]

@st.cache_data(show_spinner=False)
def df_versions(data):
    b = [categorize_version(v) for it in data for v in extract_versions(it)]
    return (pd.Series(b,name="bucket").value_counts()
            .reindex(VERSION_ORDER,fill_value=0).reset_index(name="count")
            .rename(columns={"index":"bucket"}))

@st.cache_data(show_spinner=False)
def df_instances(data):
    cnts = [count_prebid_instances(d) for d in data]
    bins = pd.cut(cnts,[-.1,0,1,2,3,4,5,float("inf")],
                  labels=INSTANCE_BUCKETS,include_lowest=True)
    return (pd.Series(bins,name="instances").value_counts()
            .reindex(INSTANCE_BUCKETS,fill_value=0).reset_index(name="count")
            .rename(columns={"index":"instances"}))

@st.cache_data(show_spinner=False)
def df_libraries(data):
    libs = [l for d in data for l in extract_libraries(d)]
    return (pd.Series(libs,name="library").value_counts()
            .reset_index(name="count").rename(columns={"index":"library"}))

@st.cache_data(show_spinner=False)
def df_globals(data):
    g = [n for d in data for n in extract_global_names(d)]
    return (pd.Series(g,name="global").value_counts()
            .reset_index(name="count").rename(columns={"index":"global"}))

@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_ctr = {k: Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    inst_ctr = {k: Counter() for k in site_ctr}
    total = 0
    for it in data:
        insts = it["prebidInstances"] or ([it] if it.get("version") else [])
        total += len(insts)
        site_mods = set()
        for ins in insts:
            mods = set(ins.get("modules", []))
            site_mods.update(mods)
            for m in mods:
                inst_ctr[classify_module(m)][m] += 1
        for m in site_mods:
            site_ctr[classify_module(m)][m] += 1
    return site_ctr, inst_ctr, total

# -------------------------------------------------
# 📥  Sidebar
# -------------------------------------------------
st.sidebar.header("Data source")

mode = st.sidebar.radio("Select dataset", ("Combined feed (default)", "Aggregate recent months"))
if mode.startswith("Aggregate"):
    months = st.sidebar.slider("Months to load", 1, 12, 3, 1)
else:
    months = None

upload_json = st.sidebar.file_uploader("…or upload JSON", type="json")
MAX_MODULES = st.sidebar.slider("Ignore sites with > N modules", 50, 500, 300, 25)

# -------------------------------------------------
# 🚀  Load data
# -------------------------------------------------
with st.spinner("Loading data …"):
    if upload_json:
        raw = load_uploaded_json(upload_json)
    elif mode.startswith("Combined"):
        raw = load_combined_feed()
    else:
        raw = load_all_months(limit_months=months)

if not raw:
    st.stop()

data = [d for d in raw if len(extract_modules(d)) <= MAX_MODULES]
if not data:
    st.warning("No records after filtering.")
    st.stop()

# -------------------------------------------------
# 🏷️  Metrics & download
# -------------------------------------------------
sites = len(data)
sites_pb = sum(1 for d in data if count_prebid_instances(d))
inst_total = sum(count_prebid_instances(d) for d in data)
avg_mods = sum(len(extract_modules(d)) for d in data) / max(inst_total,1)

c1,c2,c3,c4 = st.columns(4)
c1.metric("Total sites scanned", f"{sites:,}")
c2.metric("Sites w/ Prebid.js", f"{sites_pb:,}")
c3.metric("Total Prebid instances", f"{inst_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

st.download_button(
    "💾 Download slim JSON",
    jdumps(data),
    "prebid_slim.json",
    "application/json",
)

st.divider()

# -------------------------------------------------
# 📊  Charts
# -------------------------------------------------
vers_df   = df_versions(data)
inst_df   = df_instances(data)
lib_df    = df_libraries(data)
glob_df   = df_globals(data)
mod_site, mod_inst, _ = build_module_stats(data)

tabs = st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

with tabs[0]:
    st.subheader("Prebid.js version buckets")
    st.altair_chart(
        alt.Chart(vers_df).mark_bar().encode(
            x=alt.X("bucket:N", sort=VERSION_ORDER),
            y="count:Q",
            tooltip=["count"],
        ).properties(height=400), use_container_width=True
    )

with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    st.altair_chart(
        alt.Chart(inst_df).mark_bar().encode(
            x=alt.X("instances:N", sort=INSTANCE_BUCKETS),
            y="count:Q",
            tooltip=["count"],
        ).properties(height=400), use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(inst_df, use_container_width=True)
        st.download_button("CSV", inst_df.to_csv(index=False).encode(), "instances.csv", "text/csv")

with tabs[2]:
    st.subheader("Popularity of external libraries")
    topN = st.slider("Show top N", 10, 100, 30, 5)
    st.altair_chart(
        alt.Chart(lib_df.head(topN)).mark_bar().encode(
            y=alt.Y("library:N", sort="-x"),
            x="count:Q",
            tooltip=["count"],
        ).properties(height=600), use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(lib_df, use_container_width=True)
        st.download_button("CSV", lib_df.to_csv(index=False).encode(), "libraries.csv", "text/csv")

with tabs[3]:
    st.subheader("Popularity of global Prebid object names")
    st.altair_chart(
        alt.Chart(glob_df).mark_bar().encode(
            y=alt.Y("global:N", sort="-x"),
            x="count:Q",
            tooltip=["count"],
        ).properties(height=500), use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(glob_df, use_container_width=True)
        st.download_button("CSV", glob_df.to_csv(index=False).encode(), "global_names.csv", "text/csv")

with tabs[4]:
    st.subheader("Module popularity")
    cat = st.selectbox("Module category", list(mod_site.keys()))
    topN_mod = st.slider("Bar chart – top N", 5, 100, 20, 5)

    full_df = pd.DataFrame({
        "Module": list(mod_site[cat].keys()),
        "Sites":  list(mod_site[cat].values()),
        "Instances": [mod_inst[cat][m] for m in mod_site[cat]],
    }).sort_values("Sites", ascending=False).reset_index(drop=True)

    st.altair_chart(
        alt.Chart(full_df.head(topN_mod)).mark_bar().encode(
            y=alt.Y("Module:N", sort="-x"),
            x="Sites:Q",
            tooltip=["Sites","Instances"],
        ).properties(height=600), use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(full_df, use_container_width=True)
        st.download_button("CSV", full_df.to_csv(index=False).encode(),
                           f"modules_{cat.replace(' ','_').lower()}.csv", "text/csv")

# -------------------------------------------------
# 🤝  Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? "
    "<a href='mailto:support@prebid.org'>support@prebid.org</a></center>",
    unsafe_allow_html=True,
)
