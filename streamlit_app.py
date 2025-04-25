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

# A touch of CSS polish 🧽
st.markdown(
    """
    <style>
    html, body, [class*='css'] {font-family: "Helvetica Neue", Arial, sans-serif;}
    .block-container {padding-top: 2rem; padding-bottom: 2rem;}
    [data-testid="stMetricValue"] {font-size: 1.75rem; font-weight: 600;}
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

@st.cache_data(show_spinner=False)
def load_json_from_url(url: str):
    """Download and return JSON list from a raw GitHub (or any) URL."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


GITHUB_API_DIR = "https://api.github.com/repos/prebid/prebid-integration-monitor/contents/output?ref=jlist"
RAW_BASE       = "https://raw.githubusercontent.com/prebid/prebid-integration-monitor/jlist/output"

@st.cache_data(show_spinner=True)
def load_all_months():
    """Combine *every* monthly results.json under /output/ into a single list."""
    months_resp = requests.get(GITHUB_API_DIR, timeout=30)
    months_resp.raise_for_status()
    months = [item["name"] for item in months_resp.json() if item["type"] == "dir"]

    data: list[dict] = []
    for month in months:
        url = f"{RAW_BASE}/{month}/results.json"
        try:
            data.extend(load_json_from_url(url))
        except Exception as err:
            st.warning(f"Skipping {month}: {err}")
    return data


def load_uploaded_json(file):
    try:
        return json.load(file)
    except json.JSONDecodeError:
        st.error("The uploaded file is not a valid JSON.")
        return None

# -----------------------
# 🏷️  Classification helpers
# -----------------------

def categorize_version(version: str) -> str:
    version = version.lstrip("v")
    parts = re.split(r"[.-]", version)
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
# 🔍 Extraction helpers
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
# 📊 Data‑frame builders (cached)
# -----------------------

VERSION_ORDER = ["0.x-2.x", "3.x-5.x", "6.x-7.x", "8.x", "9.x", "Other"]
INSTANCE_LABELS = ["0", "1", "2", "3", "4", "5", "6+"]

@st.cache_data(show_spinner=False)
def build_version_df(data):
    buckets = list(chain.from_iterable(categorize_version(v) for v in chain.from_iterable(extract_versions(i) for i in data)))
    df = pd.DataFrame({"bucket": buckets})
    counts = df["bucket"].value_counts().reindex(VERSION_ORDER, fill_value=0).reset_index()
    counts.columns = ["bucket", "count"]
    return counts


@st.cache_data(show_spinner=False)
def build_instance_df(data):
    counts = [count_prebid_instances(item) for item in data]
    bins = pd.cut(counts, [-0.1, 0, 1, 2, 3, 4, 5, float("inf")], labels=INSTANCE_LABELS, include_lowest=True)
    df = pd.DataFrame({"instances": bins})
    counts_df = df["instances"].value_counts().reindex(INSTANCE_LABELS, fill_value=0).reset_index()
    counts_df.columns = ["instances", "count"]
    return counts_df


@st.cache_data(show_spinner=False)
def build_library_df(data):
    libs = list(chain.from_iterable(extract_libraries(i) for i in data))
    df = pd.DataFrame({"library": libs})
    return df["library"].value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_globalname_df(data):
    names = extract_global_var_names(data)
    df = pd.DataFrame({"global": names})
    return df["global"].value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_counter = {k: Counter() for k in ("Bid Adapter", "RTD Module", "ID System", "Analytics Adapter", "Other")}
    inst_counter = {k: Counter() for k in site_counter}
    total_instances = 0

    for item in data:
        prebid_insts = item.get("prebidInstances", [item]) if "version" in item else item.get("prebidInstances", [])
        total_instances += len(prebid_insts)
        mods_site_lvl: set[str] = set()
        for inst in prebid_insts:
            mods_inst = set(inst.get("modules", []))
            mods_site_lvl.update(mods_inst)
            for m in mods_inst:
                inst_counter[classify_module(m)][m] += 1
        for m in mods_site_lvl:
            site_counter[classify_module(m)][m] += 1
    return site_counter, inst_counter, total_instances

# -----------------------
# 📥 Sidebar – choose dataset
# -----------------------

st.sidebar.header("Data source")
load_mode = st.sidebar.radio("Choose dataset", ("All historical months (default)", "Single feed URL"))

DEFAULT_SINGLE_URL = "https://raw.githubusercontent.com/prebid/prebid-integration-monitor/main/output/results.json"
json_url = st.sidebar.text_input("Single JSON URL", value=DEFAULT_SINGLE_URL, disabled=(load_mode.startswith("All")))

uploaded_file = st.sidebar.file_uploader("…or upload a JSON file", type="json")

with st.spinner("Loading data …"):
    if uploaded_file:
        raw_data = load_uploaded_json(uploaded_file)
        st.sidebar.success("Using uploaded file ✅")
    elif load_mode.startswith("All"):
        raw_data = load_all_months()
        st.sidebar.success("Loaded all months ✅")
    else:
        raw_data = load_json_from_url(json_url)
        st.sidebar.success("Loaded from URL ✅")

if not raw_data:
    st.stop()

MAX_MODULES = st.sidebar.slider("Ignore sites with more than N modules", 50, 500, 300, 25)

# -----------------------
# 🧹 Data sanitisation
# -----------------------

data = [item for item in raw_data if len(extract_modules(item)) <= MAX_MODULES]
if not data:
    st.warning("No records after filtering – try adjusting the slider.")
    st.stop()

# -----------------------
# 🏷️  Summary Metrics
# -----------------------

site_count = len(data)
sites_with_prebid = sum(1 for i in data if count_prebid_instances(i) > 0)
total_instances = sum(count_prebid_instances(i) for i in data)
avg_modules = sum(len(extract_modules(i)) for i in data) / max(total_instances, 1)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total sites scanned", f"{site_count:,}")
col2.metric("Sites w/ Prebid.js", f"{sites_with_prebid:,}")
col3.metric("Total Prebid instances", f"{total_instances:,}")
col4.metric("Avg modules / instance", f"{avg_modules:.1f}")

st.divider()

# -----------------------
# 📊 Build DataFrames
# -----------------------

version_df  = build_version_df(data)
instance_df = build_instance_df(data)
library_df  = build_library_df(data)
global_df   = build_globalname_df(data)
module_site_counter, module_inst_counter, _ = build_module_stats(data)

# -----------------------
# 🗂️  Tabs for exploration
# -----------------------

tabs = st.tabs(["Versions", "Instances/site", "Libraries", "Global names", "Modules"])

# --- Versions
with tabs[0]:
    st.subheader("Prebid.js version buckets")
    chart = (
        alt.Chart(version_df)
        .mark_bar()
        .encode(
            x=alt.X("bucket:N", sort=VERSION_ORDER, title="Version bucket"),
            y=alt.Y("count:Q", title="Number of occurrences"),
            tooltip=["count"]
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)

# --- Instances per site
with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    chart = (
        alt.Chart(instance_df)
        .mark_bar()
        .encode(
            x=alt.X("instances:N", sort=INSTANCE_LABELS, title="Instances"),
            y=alt.Y("count:Q", title="Number of sites"),
            tooltip=["count"]
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)

# --- Libraries
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
        st.download_button("Download CSV", library_df.to_csv(index=False).encode("utf-8"), "libraries.csv", "text/csv")

# --- Global names
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
    with st.expander("Raw global‑name table & download"):
        st.dataframe(global_df, use_container_width=True)
        st.download_button("Download CSV", global_df.to_csv(index=False).encode("utf-8"), "global_names.csv", "text/csv")

# --- Modules
with tabs[4]:
    st.subheader("Module popularity – select category")
    category = st.selectbox("Module category", list(module_site_counter.keys()))
    top_n = st.slider("Bar‑chart: top N", 5, 100, 20, 5, key="mod_topn")

    # full dataframe (not truncated) for table/download
    full_df = pd.DataFrame({
        "Module": list(module_site_counter[category].keys()),
        "Sites": list(module_site_counter[category].values()),
        "Instances": [module_inst_counter[category][m] for m in module_site_counter[category].keys()],
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
    "<br><center>Reach out with feedback 👉 <a href='mailto:support@prebid.org'>support@prebid.org</a></center>",
    unsafe_allow_html=True,
)
