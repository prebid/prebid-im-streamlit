# streamlit_app.py  – Prebid Integration Monitor  (slim-object version)

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, orjson, json
from collections import Counter
from typing import List, Dict, Any

# -------------------------------------------------
# 🎨  Page & Global Config
# -------------------------------------------------
st.set_page_config(
    page_title="Prebid Integration Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

token = st.secrets.get("github_token")           # type: ignore[attr-defined]
if token:
    SESSION.headers["Authorization"] = f"token {token}"

ORG, REPO = "prebid", "prebid-integration-monitor"

COMBINED_URL = (
    "https://raw.githubusercontent.com/"
    f"{ORG}/{REPO}/main/output/prebid_combined.json.gz"
)

# month-walk (legacy, branch jlist)
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"
RAW_BASE = f"https://raw.githubusercontent.com/{ORG}/{REPO}/jlist/"

# -------------------------------------------------
# ⚡  Slim-object helper
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    """Retain only fields used in plots to save RAM."""
    slim = {
        "siteKey": it.get("site")
                or it.get("domain")
                or it.get("url")
                or it.get("pageUrl"),
        "version": it.get("version"),
        "modules": it.get("modules", []),
        "libraries": it.get("libraries", []),
    }
    if "prebidInstances" in it:
        slim["prebidInstances"] = [
            {
                "version": inst.get("version"),
                "modules": inst.get("modules", []),
                "globalVarName": inst.get("globalVarName"),
            }
            for inst in it["prebidInstances"]
        ]
    return slim

# -------------------------------------------------
# 📥  Data loaders (all slimmed)
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_combined_feed() -> List[Dict[str, Any]]:
    r = SESSION.get(COMBINED_URL, timeout=60)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = orjson.loads(gz.read())
    return [slim_item(x) for x in raw]

@st.cache_data(show_spinner=True)
def load_all_months(limit_months: int | None = None) -> List[Dict[str, Any]]:
    top = SESSION.get(f"{API_BASE}?ref=jlist", timeout=30)
    if top.status_code == 403 and not token:
        st.error("GitHub API rate-limit hit – add a token or switch to the combined feed.")
        return []
    top.raise_for_status()

    months_meta = [m for m in top.json() if m["type"] == "dir"]
    months_meta.sort(key=lambda m: m["name"], reverse=True)
    if limit_months:
        months_meta = months_meta[:limit_months]

    combined: List[Dict[str, Any]] = []
    for month in months_meta:
        try:
            files_meta = SESSION.get(month["url"], timeout=30).json()
        except Exception as e:
            st.warning(f"⚠️  skip {month['name']}: {e}")
            continue
        for f in [f for f in files_meta if f["type"] == "file" and f["name"].endswith(".json")]:
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
# 🏷️  Classification & extractors
# -------------------------------------------------
def categorize_version(v: str) -> str:
    v = (v or "").lstrip("v")
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
    if "prebidInstances" in it:
        v += [i["version"] for i in it["prebidInstances"] if i.get("version")]
    return v

def extract_modules(it):
    mods = list(it.get("modules", []))
    for inst in it.get("prebidInstances", []):
        mods.extend(inst.get("modules", []))
    return mods

def count_prebid_instances(it):
    return len(it.get("prebidInstances", [])) or (1 if it.get("version") else 0)

def extract_libraries(it):      return it.get("libraries", [])
def extract_global(it):         return [i.get("globalVarName") for i in it.get("prebidInstances", []) if i.get("globalVarName")]

# -------------------------------------------------
# 📊  Cached DataFrames
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
    counts = [count_prebid_instances(i) for i in data]
    bins = pd.cut(counts, [-.1,0,1,2,3,4,5,float("inf")],
                  labels=INSTANCE_BUCKETS, include_lowest=True)
    return (pd.Series(bins,name="instances").value_counts()
            .reindex(INSTANCE_BUCKETS,fill_value=0).reset_index(name="count")
            .rename(columns={"index":"instances"}))

@st.cache_data(show_spinner=False)
def df_libraries(data):
    libs = [l for it in data for l in extract_libraries(it)]
    return (pd.Series(libs,name="library").value_counts()
            .reset_index(name="count").rename(columns={"index":"library"}))

@st.cache_data(show_spinner=False)
def df_globals(data):
    g = [name for it in data for name in extract_global(it)]
    return (pd.Series(g,name="global").value_counts()
            .reset_index(name="count").rename(columns={"index":"global"}))

@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_ctr = {k: Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    inst_ctr = {k: Counter() for k in site_ctr}
    total = 0
    for it in data:
        insts = it.get("prebidInstances", []) or ([it] if it.get("version") else [])
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
# 📥  Sidebar – source controls
# -------------------------------------------------
st.sidebar.header("Data source")

source_mode = st.sidebar.radio(
    "Select dataset",
    ("Combined feed (default)", "Aggregate recent months")
)

if source_mode.startswith("Aggregate"):
    months_lim = st.sidebar.slider("Months to load", 1, 12, 3, 1)
else:
    months_lim = None

upload = st.sidebar.file_uploader("…or upload JSON", type="json")
MAX_MODULES = st.sidebar.slider("Ignore sites with > N modules", 50, 500, 300, 25)

# -------------------------------------------------
# 🚀  Load data
# -------------------------------------------------
with st.spinner("Fetching & parsing data …"):
    if upload:
        raw = load_uploaded_json(upload)
    elif source_mode.startswith("Combined"):
        raw = load_combined_feed()
    else:
        raw = load_all_months(limit_months=months_lim)

if not raw:
    st.stop()

data = [d for d in raw if len(extract_modules(d)) <= MAX_MODULES]
if not data:
    st.warning("No records after filtering.")
    st.stop()

# -------------------------------------------------
# 🏷️  Metrics & download
# -------------------------------------------------
site_cnt   = len(data)
sites_with = sum(1 for d in data if count_prebid_instances(d))
inst_total = sum(count_prebid_instances(d) for d in data)
avg_mods   = sum(len(extract_modules(d)) for d in data) / max(inst_total, 1)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total sites scanned", f"{site_cnt:,}")
c2.metric("Sites w/ Prebid.js", f"{sites_with:,}")
c3.metric("Total Prebid instances", f"{inst_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

st.download_button(
    "💾 Download slimmed JSON",
    orjson.dumps(data, option=orjson.OPT_INDENT_2),
    "prebid_slim.json",
    "application/json",
)

st.divider()

# -------------------------------------------------
# 📊  DataFrames & charts
# -------------------------------------------------
versions_df  = df_versions(data)
instances_df = df_instances(data)
libraries_df = df_libraries(data)
globals_df   = df_globals(data)
mod_site_ctr, mod_inst_ctr, _ = build_module_stats(data)

tabs = st.tabs(["Versions", "Instances/site", "Libraries", "Global names", "Modules"])

with tabs[0]:
    st.subheader("Prebid.js version buckets")
    st.altair_chart(
        alt.Chart(versions_df).mark_bar().encode(
            x=alt.X("bucket:N", sort=VERSION_ORDER, title="Version bucket"),
            y=alt.Y("count:Q", title="Occurrences"),
            tooltip=["count"],
        ).properties(height=400),
        use_container_width=True
    )

with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    st.altair_chart(
        alt.Chart(instances_df).mark_bar().encode(
            x=alt.X("instances:N", sort=INSTANCE_BUCKETS, title="Instances"),
            y=alt.Y("count:Q", title="Sites"),
            tooltip=["count"],
        ).properties(height=400),
        use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(instances_df, use_container_width=True)
        st.download_button("CSV", instances_df.to_csv(index=False).encode(),
                           "instances.csv", "text/csv")

with tabs[2]:
    st.subheader("Popularity of external libraries")
    topN = st.slider("Show top N", 10, 100, 30, 5)
    st.altair_chart(
        alt.Chart(libraries_df.head(topN)).mark_bar().encode(
            y=alt.Y("library:N", sort="-x"),
            x=alt.X("count:Q"),
            tooltip=["count"],
        ).properties(height=600),
        use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(libraries_df, use_container_width=True)
        st.download_button("CSV", libraries_df.to_csv(index=False).encode(),
                           "libraries.csv", "text/csv")

with tabs[3]:
    st.subheader("Popularity of global Prebid object names")
    st.altair_chart(
        alt.Chart(globals_df).mark_bar().encode(
            y=alt.Y("global:N", sort="-x"),
            x=alt.X("count:Q"),
            tooltip=["count"],
        ).properties(height=500),
        use_container_width=True
    )
    with st.expander("Raw table & download"):
        st.dataframe(globals_df, use_container_width=True)
        st.download_button("CSV", globals_df.to_csv(index=False).encode(),
                           "global_names.csv", "text/csv")

with tabs[4]:
    st.subheader("Module popularity")
    cat = st.selectbox("Module category", list(mod_site_ctr.keys()))
    topN_mod = st.slider("Bar chart – top N", 5, 100, 20, 5)

    full_df = pd.DataFrame({
        "Module": list(mod_site_ctr[cat].keys()),
        "Sites": list(mod_site_ctr[cat].values()),
        "Instances": [mod_inst_ctr[cat][m] for m in mod_site_ctr[cat]],
    }).sort_values("Sites", ascending=False).reset_index(drop=True)

    st.altair_chart(
        alt.Chart(full_df.head(topN_mod)).mark_bar().encode(
            y=alt.Y("Module:N", sort="-x"),
            x=alt.X("Sites:Q"),
            tooltip=["Sites", "Instances"],
        ).properties(height=600),
        use_container_width=True
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
