import streamlit as st
import pandas as pd
import altair as alt
import json
import re
import requests
from collections import Counter
from itertools import chain

# -----------------------
# 🎨 Page & Global Config
# -----------------------
st.set_page_config(
    page_title="Prebid Integration Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    html, body, [class*='css'] {font-family: "Helvetica Neue", Arial, sans-serif;}
    .block-container {padding-top: 2rem; padding-bottom: 2rem;}
    [data-testid='stMetricValue'] {font-size: 1.75rem; font-weight: 600;}
    footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

import matplotlib as mpl
mpl.rcParams["text.usetex"] = False
mpl.rcParams["mathtext.default"] = "regular"

# -----------------------
# 📦 Data‑loading helpers
# -----------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-Monitor-App"})

@st.cache_data(show_spinner=False)
def load_json_from_url(url: str):
    """Download a JSON list from a raw URL."""
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

# GitHub constants
ORG = "prebid"
REPO = "prebid-integration-monitor"
BRANCH = "jlist"
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"
RAW_BASE = f"https://raw.githubusercontent.com/{ORG}/{REPO}/{BRANCH}/"

@st.cache_data(show_spinner=True)
def load_all_months():
    """Gather JSON from every month folder under /output.
    Uses GitHub API when available; falls back to a static month list
    if rate‑limited (HTTP 403) or any other API failure occurs.
    Optionally honours a personal token via st.secrets['github_token']
    to raise the unauthenticated rate limit.
    """
    MONTH_LIST = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]

    combined: list[dict] = []

    # --- helper to load any raw JSON url safely
    def _extend_from_raw(raw_url: str, label: str):
        try:
            combined.extend(load_json_from_url(raw_url))
        except Exception as e:
            st.warning(f"   ↳ skip {label}: {e}")

    # --- try GitHub API first (higher fidelity listing)
    try:
        top = SESSION.get(f"{API_BASE}?ref={BRANCH}", timeout=30)
        top.raise_for_status()
        months_meta = [m for m in top.json() if m["type"] == "dir"]
    except Exception as api_err:
        st.warning(f"GitHub API directory listing failed ({api_err}). "
                   "Falling back to static month list.")
        months_meta = [{"name": m, "url": f"{API_BASE}/{m}"} for m in MONTH_LIST]

    for month in months_meta:
        month_name = month["name"]
        dir_api = month["url"]
        try:
            resp = SESSION.get(f"{dir_api}?ref={BRANCH}", timeout=30)
            resp.raise_for_status()
            files_meta = resp.json()
        except Exception:
            # Fallback: try loading output/{month}/results.json directly
            fallback_raw = f"{RAW_BASE}output/{month_name}/results.json"
            _extend_from_raw(fallback_raw, f"{month_name}/results.json (fallback)")
            continue

        # Prefer results.json; else every *.json file
        results_meta = next((f for f in files_meta if f["type"] == "file" and f["name"] == "results.json"), None)
        targets = [results_meta] if results_meta else [f for f in files_meta if f["type"] == "file" and f["name"].endswith(".json")]

        for fmeta in targets:
            raw_url = fmeta.get("download_url") or RAW_BASE + fmeta["path"]
            _extend_from_raw(raw_url, fmeta["name"])

    return combined


def load_uploaded_json(file):
    try:
        return json.load(file)
    except json.JSONDecodeError:
        st.error("Uploaded file is not valid JSON.")
        return None

# -----------------------
# 🏷️  Helper classifiers
# -----------------------

def categorize_version(v: str) -> str:
    v = v.lstrip("v")
    parts = re.split(r"[.-]", v)
    try:
        major = int(parts[0])
    except ValueError:
        return "Other"
    if major <= 2:
        return "0.x-2.x"
    if 3 <= major <= 5:
        return "3.x-5.x"
    if 6 <= major <= 7:
        return "6.x-7.x"
    if major == 8:
        return "8.x"
    if major == 9:
        return "9.x"
    return "Other"


def classify_module(name: str) -> str:
    if "BidAdapter" in name:
        return "Bid Adapter"
    if any(x in name for x in ("RtdProvider", "rtdModule")):
        return "RTD Module"
    if any(x in name for x in ("IdSystem", "userId")):
        return "ID System"
    if any(x in name for x in ("Analytics", "analyticsAdapter")):
        return "Analytics Adapter"
    return "Other"

# -----------------------
# 🔍 Field extractors
# -----------------------

def extract_versions(item):
    vers = []
    if "version" in item:
        vers.append(item["version"])
    for inst in item.get("prebidInstances", []):
        if "version" in inst:
            vers.append(inst["version"])
    return vers


def extract_modules(item):
    mods = list(item.get("modules", []))
    for inst in item.get("prebidInstances", []):
        mods.extend(inst.get("modules", []))
    return mods


def count_prebid_instances(item):
    if "prebidInstances" in item:
        return len(item["prebidInstances"])
    return 1 if "version" in item else 0


def extract_libraries(item):
    return item.get("libraries", [])


def extract_global_var_names(data):
    names = []
    for item in data:
        if "prebidInstances" in item:
            for inst in item["prebidInstances"]:
                if "globalVarName" in inst:
                    names.append(inst["globalVarName"])
        elif "globalVarName" in item:
            names.append(item["globalVarName"])
    return names

# -----------------------
# 📊 Cached dataframe builders
# -----------------------
VERSION_ORDER   = ["0.x-2.x", "3.x-5.x", "6.x-7.x", "8.x", "9.x", "Other"]
INSTANCE_BUCKETS = ["0", "1", "2", "3", "4", "5", "6+"]

@st.cache_data(show_spinner=False)
def build_version_df(data):
    buckets = [categorize_version(v) for item in data for v in extract_versions(item)]
    return (
        pd.Series(buckets, name="bucket").value_counts().reindex(VERSION_ORDER, fill_value=0)
        .reset_index(name="count").rename(columns={"index": "bucket"})
    )

@st.cache_data(show_spinner=False)
def build_instance_df(data):
    counts = [count_prebid_instances(item) for item in data]
    bins = pd.cut(counts, [-0.1,0,1,2,3,4,5,float("inf")], labels=INSTANCE_BUCKETS, include_lowest=True)
    return (
        pd.Series(bins, name="instances").value_counts().reindex(INSTANCE_BUCKETS, fill_value=0)
        .reset_index(name="count").rename(columns={"index": "instances"})
    )

@st.cache_data(show_spinner=False)
def build_library_df(data):
    libs = [lib for item in data for lib in extract_libraries(item)]
    return (
        pd.Series(libs, name="library").value_counts()
        .reset_index(name="count").rename(columns={"index": "library"})
    )

@st.cache_data(show_spinner=False)
def build_global_df(data):
    names = extract_global_var_names(data)
    return (
        pd.Series(names, name="global").value_counts()
        .reset_index(name="count").rename(columns={"index": "global"})
    )

@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_counter = {k: Counter() for k in ("Bid Adapter", "RTD Module", "ID System", "Analytics Adapter", "Other")}
    inst_counter = {k: Counter() for k in site_counter}
    total_instances = 0
    for item in data:
        prebid_insts = item.get("prebidInstances", [item]) if "version" in item else item.get("prebidInstances", [])
        total_instances += len(prebid_insts)
        site_mods: set[str] = set()
        for inst in prebid_insts:
            mods_inst = set(inst.get("modules", []))
            site_mods.update(mods_inst)
            for m in mods_inst:
                inst_counter[classify_module(m)][m] += 1
        for m in site_mods:
            site_counter[classify_module(m)][m] += 1
    return site_counter, inst_counter, total_instances

# -----------------------
# 📥 Sidebar – data source
# -----------------------
st.sidebar.header("Data source")
mode = st.sidebar.radio("Choose dataset",
                        ("All historical months (default)", "Single feed URL"))
DEFAULT_SINGLE_URL = f"{RAW_BASE}output/results.json"
json_url = st.sidebar.text_input("Single JSON URL",
                                 value=DEFAULT_SINGLE_URL,
                                 disabled=mode.startswith("All"))
upload_file = st.sidebar.file_uploader("…or upload a JSON file", type="json")

with st.spinner("Loading data …"):
    if upload_file:
        raw_data = load_uploaded_json(upload_file)
        st.sidebar.success("Using uploaded file ✅")
    elif mode.startswith("All"):
        raw_data = load_all_months()
        st.sidebar.success("Loaded all months ✅")
    else:
        raw_data = load_json_from_url(json_url)
        st.sidebar.success("Loaded from URL ✅")

if not raw_data:
    st.stop()

MAX_MODULES = st.sidebar.slider("Ignore sites with more than N modules",
                                50, 500, 300, 25)

# -----------------------
# 🧹 Data cleanse
# -----------------------
data = [item for item in raw_data if len(extract_modules(item)) <= MAX_MODULES]
if not data:
    st.warning("No records after filtering – adjust slider?")
    st.stop()

# -----------------------
# 🏷️  Summary metrics
# -----------------------
site_count     = len(data)
sites_with_pb  = sum(1 for item in data if count_prebid_instances(item) > 0)
instance_total = sum(count_prebid_instances(item) for item in data)
avg_mods       = sum(len(extract_modules(item)) for item in data) / max(instance_total, 1)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total sites scanned", f"{site_count:,}")
c2.metric("Sites w/ Prebid.js", f"{sites_with_pb:,}")
c3.metric("Total Prebid instances", f"{instance_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

st.divider()

# -----------------------
# 📊 Build DataFrames
# -----------------------
version_df  = build_version_df(data)
instance_df = build_instance_df(data)
library_df  = build_library_df(data)
global_df   = build_global_df(data)
module_site_counter, module_inst_counter, _ = build_module_stats(data)

# -----------------------
# 🗂️  Tabs
# -----------------------
TAB_TITLES = ["Versions", "Instances/site",
              "Libraries", "Global names", "Modules"]
tabs = st.tabs(TAB_TITLES)

# — Versions tab
with tabs[0]:
    st.subheader("Prebid.js version buckets")
    chart = (
        alt.Chart(version_df)
           .mark_bar()
           .encode(
               x=alt.X("bucket:N", sort=VERSION_ORDER, title="Version bucket"),
               y=alt.Y("count:Q", title="Occurrences"),
               tooltip=["count"]
           )
           .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)

# — Instances/site tab
with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    chart = (
        alt.Chart(instance_df)
           .mark_bar()
           .encode(
               x=alt.X("instances:N",
                       sort=INSTANCE_BUCKETS,
                       title="Instances per site"),
               y=alt.Y("count:Q", title="Number of sites"),
               tooltip=["count"]
           )
           .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
    with st.expander("Raw table & download"):
        st.dataframe(instance_df, use_container_width=True)
        st.download_button(
            "Download CSV",
            instance_df.to_csv(index=False).encode("utf-8"),
            "instances_per_site.csv",
            "text/csv",
        )

# — Libraries tab
with tabs[2]:
    st.subheader("Popularity of external libraries")
    top_n = st.slider("Show top N", 10, 100, 30, 5)
    chart = (
        alt.Chart(library_df.head(top_n))
           .mark_bar()
           .encode(
               y=alt.Y("library:N", sort="-x", title="Library"),
               x=alt.X("count:Q", title="Sites"),
               tooltip=["count"]
           )
           .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)
    with st.expander("Raw library table & download"):
        st.dataframe(library_df, use_container_width=True)
        st.download_button(
            "Download CSV",
            library_df.to_csv(index=False).encode("utf-8"),
            "libraries.csv",
            "text/csv",
        )

# — Global names tab
with tabs[3]:
    st.subheader("Popularity of global Prebid object names")
    chart = (
        alt.Chart(global_df)
           .mark_bar()
           .encode(
               y=alt.Y("global:N", sort="-x", title="Global object name"),
               x=alt.X("count:Q", title="Sites"),
               tooltip=["count"]
           )
           .properties(height=500)
    )
    st.altair_chart(chart, use_container_width=True)
    with st.expander("Raw global-name table & download"):
        st.dataframe(global_df, use_container_width=True)
        st.download_button(
            "Download CSV",
            global_df.to_csv(index=False).encode("utf-8"),
            "global_names.csv",
            "text/csv",
        )

# — Modules tab
with tabs[4]:
    st.subheader("Module popularity")
    category = st.selectbox("Module category",
                            list(module_site_counter.keys()))
    top_n = st.slider("Bar-chart: top N", 5, 100, 20, 5, key="mod_topn")

    full_df = pd.DataFrame({
        "Module": list(module_site_counter[category].keys()),
        "Sites": list(module_site_counter[category].values()),
        "Instances": [module_inst_counter[category][m]
                      for m in module_site_counter[category].keys()],
    }).sort_values("Sites", ascending=False).reset_index(drop=True)

    bar_df = full_df.head(top_n)

    chart = (
        alt.Chart(bar_df)
           .mark_bar()
           .encode(
               y=alt.Y("Module:N", sort="-x"),
               x=alt.X("Sites:Q"),
               tooltip=["Sites", "Instances"]
           )
           .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

    with st.expander("Raw module table & download"):
        st.dataframe(full_df, use_container_width=True)
        st.download_button(
            "Download CSV",
            full_df.to_csv(index=False).encode("utf-8"),
            f"modules_{category.replace(' ', '_').lower()}.csv",
            "text/csv",
        )

# -----------------------
# 🤝 Footer
# -----------------------
st.markdown(
    "<br><center>Reach out with feedback 👉 "
    "<a href='mailto:chuie@prebid.org'>chuie@prebid.org</a>"
    "</center>",
    unsafe_allow_html=True,
)
