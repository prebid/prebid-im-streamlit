# streamlit_app.py  – Prebid Integration Monitor  (slim + dedupe)

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, json
from collections import Counter
from typing import List, Dict, Any

# ---------- optional fast JSON ----------
try:
    import orjson
    def jloads(b): return orjson.loads(b if isinstance(b, bytes) else b.encode())
    def jdumps(o): return orjson.dumps(o, option=orjson.OPT_INDENT_2)
except ModuleNotFoundError:
    import json as _stdjson
    def jloads(b): return _stdjson.loads(b if isinstance(b, str) else b.decode())
    def jdumps(o): return _stdjson.dumps(o, indent=2).encode()

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
COMBINED_URL = f"https://raw.githubusercontent.com/{ORG}/{REPO}/main/output/prebid_combined.json.gz"

# legacy month-walker (branch jlist) — optional
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"
RAW_BASE = f"https://raw.githubusercontent.com/{ORG}/{REPO}/jlist/"

# -------------------------------------------------
# ⚡  Slim-object helper
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "siteKey": it.get("site") or it.get("domain") or it.get("url") or it.get("pageUrl"),
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
# 🧹  Deduplication helper
# -------------------------------------------------
def dedupe_sites(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for r in rows:
        key = r["siteKey"] or ""
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out

# -------------------------------------------------
# 📥  Data loaders  (all return slimmed objects)
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_combined_feed() -> List[Dict[str, Any]]:
    r = SESSION.get(COMBINED_URL, timeout=60); r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = jloads(gz.read())
    return [slim_item(x) for x in raw]

@st.cache_data(show_spinner=True)
def load_all_months(limit: int | None = None) -> List[Dict[str, Any]]:
    top = SESSION.get(f"{API_BASE}?ref=jlist", timeout=30)
    if top.status_code == 403 and not token:
        st.error("GitHub API rate-limited. Add a token or use the combined feed."); return []
    top.raise_for_status()

    months = sorted((m for m in top.json() if m["type"] == "dir"),
                    key=lambda m: m["name"], reverse=True)
    if limit: months = months[:limit]

    merged: List[Dict[str, Any]] = []
    for m in months:
        try: files = SESSION.get(m["url"], timeout=30).json()
        except Exception as e:
            st.warning(f"⚠️ skip {m['name']}: {e}"); continue
        for f in (f for f in files if f["type"]=="file" and f["name"].endswith(".json")):
            url = f.get("download_url") or RAW_BASE+f["path"]
            try: merged.extend(slim_item(x) for x in SESSION.get(url, timeout=30).json())
            except Exception as e: st.warning(f"   ↳ skip {f['name']} ({m['name']}) → {e}")
    return merged

def load_uploaded_json(file):     # slim + dedupe
    try:  return [slim_item(x) for x in json.load(file)]
    except json.JSONDecodeError:
        st.error("Uploaded file is not valid JSON."); return None

# -------------------------------------------------
# 🏷️  Extraction helpers
# -------------------------------------------------
def categorize_version(v: str | None) -> str:
    if not v: return "Other"
    v=v.lstrip("v")
    try: major=int(re.split(r"[.-]",v)[0])
    except ValueError: return "Other"
    return {9:"9.x",8:"8.x",7:"6.x-7.x",6:"6.x-7.x",
            5:"3.x-5.x",4:"3.x-5.x",3:"3.x-5.x",
            2:"0.x-2.x",1:"0.x-2.x",0:"0.x-2.x"}.get(major,"Other")

def classify_module(n:str)->str:
    if "BidAdapter" in n: return "Bid Adapter"
    if any(x in n for x in ("RtdProvider","rtdModule")): return "RTD Module"
    if any(x in n for x in ("IdSystem","userId")): return "ID System"
    if any(x in n for x in ("Analytics","analyticsAdapter")): return "Analytics Adapter"
    return "Other"

def extract_versions(it):    return [it["version"]]*(it["version"] is not None) + \
                             [i["version"] for i in it["prebidInstances"] if i.get("version")]
def extract_modules(it):     return it["modules"] + [m for inst in it["prebidInstances"] for m in inst["modules"]]
def count_instances(it):     return len(it["prebidInstances"]) or (1 if it["version"] else 0)
def extract_libs(it):        return it["libraries"]
def extract_globals(it):     return [i["globalVarName"] for i in it["prebidInstances"] if i.get("globalVarName")]

# -------------------------------------------------
# 📊  Cached DataFrames
# -------------------------------------------------
VER_ORDER = ["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","Other"]
INST_BINS = ["0","1","2","3","4","5","6+"]

@st.cache_data(show_spinner=False)
def df_versions(data):
    buckets=[categorize_version(v) for d in data for v in extract_versions(d)]
    return (pd.Series(buckets,name="bucket").value_counts()
            .reindex(VER_ORDER,fill_value=0).reset_index(name="count")
            .rename(columns={"index":"bucket"}))

@st.cache_data(show_spinner=False)
def df_instances(data):
    counts=[count_instances(d) for d in data]
    bins=pd.cut(counts,[-.1,0,1,2,3,4,5,float("inf")],labels=INST_BINS,include_lowest=True)
    return (pd.Series(bins,name="instances").value_counts()
            .reindex(INST_BINS,fill_value=0).reset_index(name="count")
            .rename(columns={"index":"instances"}))

@st.cache_data(show_spinner=False)
def df_libraries(data):
    libs=[l for d in data for l in extract_libs(d)]
    return (pd.Series(libs,name="library").value_counts()
            .reset_index(name="count").rename(columns={"index":"library"}))

@st.cache_data(show_spinner=False)
def df_globals(data):
    g=[n for d in data for n in extract_globals(d)]
    return (pd.Series(g,name="global").value_counts()
            .reset_index(name="count").rename(columns={"index":"global"}))

@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_ctr={k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    inst_ctr={k:Counter() for k in site_ctr}
    total=0
    for d in data:
        insts=d["prebidInstances"] or ([d] if d["version"] else [])
        total+=len(insts)
        site_mods=set()
        for ins in insts:
            mods=set(ins["modules"])
            site_mods|=mods
            for m in mods: inst_ctr[classify_module(m)][m]+=1
        for m in site_mods: site_ctr[classify_module(m)][m]+=1
    return site_ctr,inst_ctr,total

# -------------------------------------------------
# 📥  Sidebar
# -------------------------------------------------
st.sidebar.header("Data source")
mode=st.sidebar.radio("Select dataset",("Combined feed (default)","Aggregate recent months"))
months=st.sidebar.slider("Months to load",1,12,3,1) if mode.startswith("Aggregate") else None
upload=st.sidebar.file_uploader("…or upload JSON",type="json")
MAX_MODS=st.sidebar.slider("Ignore sites with > N modules",50,500,300,25)

# -------------------------------------------------
# 🚀  Load + dedupe
# -------------------------------------------------
with st.spinner("Fetching data …"):
    if upload: raw=load_uploaded_json(upload)
    elif mode.startswith("Combined"): raw=load_combined_feed()
    else: raw=load_all_months(limit_months=months)

if not raw: st.stop()

filtered=[r for r in raw if len(extract_modules(r))<=MAX_MODS]
data=dedupe_sites(filtered)

if not data: st.warning("No records after filtering."); st.stop()

# -------------------------------------------------
# 🏷️  Metrics & download
# -------------------------------------------------
sites=len(data); sites_pb=sum(1 for d in data if count_instances(d))
inst_total=sum(count_instances(d) for d in data)
avg_mods=sum(len(extract_modules(d)) for d in data)/max(inst_total,1)

c1,c2,c3,c4=st.columns(4)
c1.metric("Total sites scanned",f"{sites:,}")
c2.metric("Sites w/ Prebid.js",f"{sites_pb:,}")
c3.metric("Total Prebid instances",f"{inst_total:,}")
c4.metric("Avg modules / instance",f"{avg_mods:.1f}")

st.download_button(
    "💾 Download deduped slim JSON",
    jdumps(data),
    "prebid_slim_deduped.json",
    "application/json",
)

st.divider()

# -------------------------------------------------
# 📊  Charts
# -------------------------------------------------
vers_df=df_versions(data); inst_df=df_instances(data)
lib_df=df_libraries(data); glob_df=df_globals(data)
mod_site,mod_inst,_=build_module_stats(data)

tabs=st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

with tabs[0]:
    st.subheader("Prebid.js version buckets")
    st.altair_chart(alt.Chart(vers_df).mark_bar().encode(
        x=alt.X("bucket:N",sort=VER_ORDER), y="count:Q", tooltip=["count"]).properties(height=400),
        use_container_width=True)

with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    st.altair_chart(alt.Chart(inst_df).mark_bar().encode(
        x=alt.X("instances:N",sort=INST_BINS), y="count:Q", tooltip=["count"]).properties(height=400),
        use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(inst_df, use_container_width=True)
        st.download_button("CSV", inst_df.to_csv(index=False).encode(), "instances.csv","text/csv")

with tabs[2]:
    st.subheader("Popularity of external libraries")
    topN=st.slider("Show top N",10,100,30,5)
    st.altair_chart(alt.Chart(lib_df.head(topN)).mark_bar().encode(
        y=alt.Y("library:N",sort="-x"), x="count:Q", tooltip=["count"]).properties(height=600),
        use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(lib_df, use_container_width=True)
        st.download_button("CSV", lib_df.to_csv(index=False).encode(), "libraries.csv","text/csv")

with tabs[3]:
    st.subheader("Popularity of global Prebid object names")
    st.altair_chart(alt.Chart(glob_df).mark_bar().encode(
        y=alt.Y("global:N",sort="-x"), x="count:Q", tooltip=["count"]).properties(height=500),
        use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(glob_df, use_container_width=True)
        st.download_button("CSV", glob_df.to_csv(index=False).encode(), "global_names.csv","text/csv")

with tabs[4]:
    st.subheader("Module popularity")
    cat=st.selectbox("Module category",list(mod_site.keys()))
    topN_mod=st.slider("Bar chart – top N",5,100,20,5)
    full_df=pd.DataFrame({
        "Module":list(mod_site[cat]),
        "Sites":[mod_site[cat][m] for m in mod_site[cat]],
        "Instances":[mod_inst[cat][m] for m in mod_site[cat]]}).sort_values("Sites",ascending=False)
    st.altair_chart(alt.Chart(full_df.head(topN_mod)).mark_bar().encode(
        y=alt.Y("Module:N",sort="-x"), x="Sites:Q", tooltip=["Sites","Instances"]).properties(height=600),
        use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(full_df, use_container_width=True)
        st.download_button("CSV",full_df.to_csv(index=False).encode(),
                           f"modules_{cat.replace(' ','_').lower()}.csv","text/csv")

# -------------------------------------------------
# 🤝  Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? <a href='mailto:support@prebid.org'>support@prebid.org</a></center>",
    unsafe_allow_html=True,
)
