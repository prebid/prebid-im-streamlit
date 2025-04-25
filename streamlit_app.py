
# streamlit_app.py  – Prebid Integration Monitor
import streamlit as st
import pandas as pd
import altair as alt
import json, re, requests
from collections import Counter
from itertools import chain
from typing import List, Dict, Any

# -------------------------------------------------
# 🎨 Page & Global Config
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
    html, body, [class*='css'] {font-family:"Helvetica Neue", Arial, sans-serif;}
    /* extra top-padding so first metric isn’t hidden by Streamlit header */
    .block-container {padding-top:3rem; padding-bottom:2rem;}
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
# 📦  Data-loading helpers
# -------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-Monitor-App"})

# use a PAT in secrets to avoid GitHub rate-limit
token = st.secrets.get("github_token")          # type: ignore[attr-defined]
if token:
    SESSION.headers["Authorization"] = f"token {token}"

@st.cache_data(show_spinner=False)
def load_json_from_url(url: str) -> List[Dict[str, Any]]:
    """Fetch raw JSON list from GitHub CDN (or any URL)."""
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

# GitHub paths
ORG, REPO, BRANCH = "prebid", "prebid-integration-monitor", "jlist"
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"
RAW_BASE = f"https://raw.githubusercontent.com/{ORG}/{REPO}/{BRANCH}/"

@st.cache_data(show_spinner=True)
def load_all_months(limit_months: int | None = None) -> List[Dict[str, Any]]:
    """Combine daily results from each month under /output/.

    - Auth with PAT if provided.
    - `limit_months` keeps memory modest (e.g. last 3).
    """
    # list month folders (rate-limit-protected)
    top = SESSION.get(f"{API_BASE}?ref={BRANCH}", timeout=30)
    if top.status_code == 403 and not token:
        st.error("GitHub API rate-limit reached – set `github_token` in secrets.")
        return []
    top.raise_for_status()

    months_meta = [m for m in top.json() if m["type"] == "dir"]
    months_meta.sort(key=lambda m: m["name"], reverse=True)  # newest first
    if limit_months:
        months_meta = months_meta[:limit_months]

    combined: list[Dict[str, Any]] = []
    for month in months_meta:
        month_api = month["url"]          # already has ?ref=branch
        try:
            files_meta = SESSION.get(month_api, timeout=30).json()
        except Exception as e:
            st.warning(f"⚠️ Skip {month['name']} (list error {e})")
            continue

        daily_files = [f for f in files_meta if f["type"] == "file" and f["name"].endswith(".json")]
        for fmeta in daily_files:
            raw = fmeta.get("download_url") or RAW_BASE + fmeta["path"]
            try:
                combined.extend(load_json_from_url(raw))
            except Exception as e:
                st.warning(f"   ↳ skip {fmeta['name']} ({month['name']}) → {e}")

    return combined

def load_uploaded_json(file):
    try:
        return json.load(file)
    except json.JSONDecodeError:
        st.error("Uploaded file is not valid JSON.")
        return None

# -------------------------------------------------
# 🗂️  Data cleanup helpers
# -------------------------------------------------
def dedupe_sites(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """De-duplicate by site/domain/url/pageUrl."""
    seen: set[str] = set()
    out:  List[Dict[str, Any]] = []
    for item in data:
        key = (item.get("site") or item.get("domain") or
               item.get("url") or item.get("pageUrl") or
               json.dumps(item, sort_keys=True)[:100])
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out

# -------------------------------------------------
# 🏷️  Classifiers / extractors
# -------------------------------------------------
def categorize_version(v: str) -> str:
    v = v.lstrip("v")
    try:
        major = int(re.split(r"[.-]", v)[0])
    except ValueError:
        return "Other"
    if major <= 2:          return "0.x-2.x"
    if 3 <= major <= 5:     return "3.x-5.x"
    if 6 <= major <= 7:     return "6.x-7.x"
    if major == 8:          return "8.x"
    if major == 9:          return "9.x"
    return "Other"

def classify_module(n: str) -> str:
    if "BidAdapter" in n:                         return "Bid Adapter"
    if any(x in n for x in ("RtdProvider","rtdModule")):  return "RTD Module"
    if any(x in n for x in ("IdSystem","userId")):        return "ID System"
    if any(x in n for x in ("Analytics","analyticsAdapter")): return "Analytics Adapter"
    return "Other"

def extract_versions(item):  # -> list[str]
    vers = [item["version"]] if "version" in item else []
    vers += [inst["version"] for inst in item.get("prebidInstances", []) if "version" in inst]
    return vers

def extract_modules(item):
    mods = list(item.get("modules", []))
    for inst in item.get("prebidInstances", []):
        mods.extend(inst.get("modules", []))
    return mods

def count_prebid_instances(itm):
    return len(itm.get("prebidInstances", [])) or (1 if "version" in itm else 0)

def extract_libraries(item):       return item.get("libraries", [])
def extract_global_names(data):
    names = []
    for itm in data:
        if "prebidInstances" in itm:
            names += [i["globalVarName"] for i in itm["prebidInstances"]
                       if "globalVarName" in i]
        elif "globalVarName" in itm:
            names.append(itm["globalVarName"])
    return names

# -------------------------------------------------
# 📊 Cached dataframe builders
# -------------------------------------------------
VERSION_ORDER, INSTANCE_BUCKETS = ["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","Other"], ["0","1","2","3","4","5","6+"]

@st.cache_data(show_spinner=False)
def df_versions(data):
    b = [categorize_version(v) for itm in data for v in extract_versions(itm)]
    return (pd.Series(b,name="bucket").value_counts()
            .reindex(VERSION_ORDER, fill_value=0).reset_index(name="count")
            .rename(columns={"index":"bucket"}))

@st.cache_data(show_spinner=False)
def df_instances(data):
    cnts = [count_prebid_instances(i) for i in data]
    buckets = pd.cut(cnts, [-.1,0,1,2,3,4,5,float("inf")],
                     labels=INSTANCE_BUCKETS, include_lowest=True)
    return (pd.Series(buckets,name="instances").value_counts()
            .reindex(INSTANCE_BUCKETS, fill_value=0).reset_index(name="count")
            .rename(columns={"index":"instances"}))

@st.cache_data(show_spinner=False)
def df_libraries(data):
    libs = [lib for itm in data for lib in extract_libraries(itm)]
    return (pd.Series(libs,name="library").value_counts()
            .reset_index(name="count").rename(columns={"index":"library"}))

@st.cache_data(show_spinner=False)
def df_globals(data):
    g = extract_global_names(data)
    return (pd.Series(g,name="global").value_counts()
            .reset_index(name="count").rename(columns={"index":"global"}))

@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_ctr = {k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    inst_ctr = {k:Counter() for k in site_ctr}
    total_instances = 0

    for itm in data:
        insts = itm.get("prebidInstances", []) or ([itm] if "version" in itm else [])
        total_instances += len(insts)
        site_mods = set()
        for inst in insts:
            mods = set(inst.get("modules", []))
            site_mods.update(mods)
            for m in mods: inst_ctr[classify_module(m)][m] += 1
        for m in site_mods: site_ctr[classify_module(m)][m] += 1

    return site_ctr, inst_ctr, total_instances

# -------------------------------------------------
# 📥  Sidebar – user controls
# -------------------------------------------------
st.sidebar.header("Data source")

months_to_fetch = st.sidebar.slider("Months to load (newest first)",
                                    1, 12, 3, 1, help="Keep this small to fit resource limits.")
de_dupe         = st.sidebar.checkbox("De-duplicate sites", True)
upload_json     = st.sidebar.file_uploader("...or upload a JSON file", type="json")

with st.spinner("Loading data ..."):
    if upload_json:
        raw = load_uploaded_json(upload_json)
    else:
        raw = load_all_months(limit_months=months_to_fetch)

if not raw:
    st.stop()

if de_dupe:
    raw = dedupe_sites(raw)

MAX_MODULES = st.sidebar.slider("Ignore sites with > N modules", 50, 500, 300, 25)
data = [itm for itm in raw if len(extract_modules(itm)) <= MAX_MODULES]

if not data:
    st.warning("No records left after filtering.")
    st.stop()

# -------------------------------------------------
# 🏷️  Summary metrics
# -------------------------------------------------
site_cnt   = len(data)
sites_with = sum(1 for i in data if count_prebid_instances(i))
inst_total = sum(count_prebid_instances(i) for i in data)
avg_mods   = sum(len(extract_modules(i)) for i in data) / max(inst_total,1)

c1,c2,c3,c4 = st.columns(4)
c1.metric("Total sites scanned", f"{site_cnt:,}")
c2.metric("Sites w/ Prebid.js", f"{sites_with:,}")
c3.metric("Total Prebid instances", f"{inst_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

# Download merged JSON
st.download_button(
    "💾 Download merged JSON",
    json.dumps(data, indent=2).encode("utf-8"),
    "prebid_combined.json",
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

tabs = st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

# Versions
with tabs[0]:
    st.subheader("Prebid.js version buckets")
    st.altair_chart(
        alt.Chart(versions_df).mark_bar().encode(
            x=alt.X("bucket:N", sort=VERSION_ORDER, title="Version bucket"),
            y=alt.Y("count:Q", title="Occurrences"),
            tooltip=["count"],
        ).properties(height=400), use_container_width=True)

# Instances / site
with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    st.altair_chart(
        alt.Chart(instances_df).mark_bar().encode(
            x=alt.X("instances:N", sort=INSTANCE_BUCKETS, title="Instances"),
            y=alt.Y("count:Q", title="Sites"),
            tooltip=["count"],
        ).properties(height=400), use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(instances_df, use_container_width=True)
        st.download_button("CSV", instances_df.to_csv(index=False).encode(),
                           "instances_per_site.csv", "text/csv")

# Libraries
with tabs[2]:
    st.subheader("Popularity of external libraries")
    topN = st.slider("Show top N", 10, 100, 30, 5)
    st.altair_chart(
        alt.Chart(libraries_df.head(topN)).mark_bar().encode(
            y=alt.Y("library:N", sort="-x"),
            x=alt.X("count:Q"),
            tooltip=["count"],
        ).properties(height=600), use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(libraries_df, use_container_width=True)
        st.download_button("CSV", libraries_df.to_csv(index=False).encode(),
                           "libraries.csv", "text/csv")

# Global names
with tabs[3]:
    st.subheader("Popularity of global Prebid object names")
    st.altair_chart(
        alt.Chart(globals_df).mark_bar().encode(
            y=alt.Y("global:N", sort="-x"),
            x=alt.X("count:Q"),
            tooltip=["count"],
        ).properties(height=500), use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(globals_df, use_container_width=True)
        st.download_button("CSV", globals_df.to_csv(index=False).encode(),
                           "global_names.csv", "text/csv")

# Modules
with tabs[4]:
    st.subheader("Module popularity")
    cat = st.selectbox("Module category", list(mod_site_ctr.keys()))
    topN_mod = st.slider("Bar chart – top N", 5, 100, 20, 5)

    full_df = pd.DataFrame({
        "Module": list(mod_site_ctr[cat].keys()),
        "Sites": list(mod_site_ctr[cat].values()),
        "Instances": [mod_inst_ctr[cat][m] for m in mod_site_ctr[cat].keys()],
    }).sort_values("Sites", ascending=False).reset_index(drop=True)

    st.altair_chart(
        alt.Chart(full_df.head(topN_mod)).mark_bar().encode(
            y=alt.Y("Module:N", sort="-x"),
            x=alt.X("Sites:Q"),
            tooltip=["Sites","Instances"],
        ).properties(height=600), use_container_width=True)

    with st.expander("Raw table & download"):
        st.dataframe(full_df, use_container_width=True)
        st.download_button("CSV", full_df.to_csv(index=False).encode(),
                           f"modules_{cat.replace(' ','_').lower()}.csv", "text/csv")

# -------------------------------------------------
# 🤝 Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? "
    "<a href='mailto:support@prebid.org'>support@prebid.org</a></center>",
    unsafe_allow_html=True,
)
