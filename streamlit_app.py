import streamlit as st
import pandas as pd
import altair as alt
import json
import re
from collections import Counter
import requests

# -----------------------
# 🎨 Page & Global Config
# -----------------------
st.set_page_config(
    page_title="Prebid Integration Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject a touch of custom CSS
st.markdown(
    """
    <style>
    /* Improve base typography */
    html, body, [class*="css"]  {font-family: "Helvetica Neue", Arial, sans-serif;}

    /* Tighten overall padding */
    .block-container {padding-top:2rem; padding-bottom:2rem;}

    /* Prettify metric values */
    [data-testid="stMetricValue"] {font-size: 1.75rem; font-weight: 600;}

    /* Hide Streamlit footer */
    footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

# Disable math text parsing in case matplotlib is ever used downstream
import matplotlib as mpl
mpl.rcParams["text.usetex"] = False
mpl.rcParams["mathtext.default"] = "regular"

# -----------------------
# 📦 Utility Functions
# -----------------------

def load_json_from_url(url: str):
    """Fetch JSON from URL and cache result."""
    @st.cache_data(show_spinner=False)
    def _load(_url):  # inner so st.cache_data works
        response = requests.get(_url, timeout=20)
        response.raise_for_status()
        return response.json()

    try:
        return _load(url)
    except Exception as e:
        st.error(f"Error fetching data from URL: {e}")
        return None


def load_uploaded_json(file):
    try:
        return json.load(file)
    except json.JSONDecodeError:
        st.error("The uploaded file is not a valid JSON.")
        return None


# -----------------------
# 🧮 Data Helpers
# -----------------------

def categorize_version(version: str) -> str:
    version = version.lstrip("v")
    parts = re.split(r"[.-]", version)
    try:
        major = int(parts[0])
    except ValueError:
        return "Other"

    if major <= 2:
        return "0.x‑2.x"
    elif 3 <= major <= 5:
        return "3.x‑5.x"
    elif 6 <= major <= 7:
        return "6.x‑7.x"
    elif major == 8:
        return "8.x"
    elif major == 9:
        return "9.x"
    return "Other"


def classify_module(name: str) -> str:
    if "BidAdapter" in name:
        return "Bid Adapter"
    if any(x in name for x in ["RtdProvider", "rtdModule"]):
        return "RTD Module"
    if any(x in name for x in ["IdSystem", "userId"]):
        return "ID System"
    if any(x in name for x in ["Analytics", "analyticsAdapter"]):
        return "Analytics Adapter"
    return "Other"


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
# 📊 Chart Builders (Altair)
# -----------------------

@st.cache_data(show_spinner=False)
def build_version_df(data):
    buckets = []
    for item in data:
        for v in extract_versions(item):
            buckets.append(categorize_version(v))
    return pd.DataFrame(buckets, columns=["bucket"]).value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_instance_df(data):
    counts = [count_prebid_instances(item) for item in data]
    bins = pd.cut(counts, [-0.1, 0, 1, 2, 3, 4, 5, float("inf")], labels=["0", "1", "2", "3", "4", "5", "6+"])
    return pd.DataFrame(bins).value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_library_df(data):
    libs = []
    for item in data:
        libs.extend(extract_libraries(item))
    return pd.DataFrame(libs, columns=["library"]).value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_globalname_df(data):
    names = extract_global_var_names(data)
    return pd.DataFrame(names, columns=["global"]).value_counts().reset_index(name="count")


@st.cache_data(show_spinner=False)
def build_module_stats(data):
    site_counter = {k: Counter() for k in ["Bid Adapter", "RTD Module", "ID System", "Analytics Adapter", "Other"]}
    inst_counter = {k: Counter() for k in site_counter}
    total_instances = 0

    for item in data:
        prebid_insts = item.get("prebidInstances", [item]) if "version" in item else item.get("prebidInstances", [])
        total_instances += len(prebid_insts)
        mods_site_lvl = set()
        for inst in prebid_insts:
            mods_inst = set(inst.get("modules", []))
            mods_site_lvl.update(mods_inst)
            for m in mods_inst:
                inst_counter[classify_module(m)][m] += 1
        for m in mods_site_lvl:
            site_counter[classify_module(m)][m] += 1
    return site_counter, inst_counter, total_instances


# -----------------------
# 📥 Sidebar – Data Source
# -----------------------

st.sidebar.header("Data Source")
DEFAULT_URL = "https://raw.githubusercontent.com/prebid/prebid-integration-monitor/main/output/results.json"

json_url = st.sidebar.text_input("JSON feed URL", value=DEFAULT_URL, help="Raw Prebid Integration Monitor JSON feed.")

uploaded_file = st.sidebar.file_uploader("...or upload a JSON file", type="json")

if uploaded_file:
    raw_data = load_uploaded_json(uploaded_file)
    st.sidebar.success("Loaded data from file ✅")
else:
    raw_data = load_json_from_url(json_url)

if not raw_data:
    st.stop()

# Optional filtering for pathological outliers
MAX_MODULES = st.sidebar.slider("Ignore sites with more than N modules", min_value=50, max_value=500, value=300, step=25)

# -----------------------
# 🧹 Data Prep
# -----------------------

data = [item for item in raw_data if len(extract_modules(item)) <= MAX_MODULES]
if not data:
    st.warning("No data after filtering – try increasing the module limit.")
    st.stop()

sites_with_prebid = sum(1 for item in data if count_prebid_instances(item) > 0)
site_count        = len(data)

version_df  = build_version_df(data)
instance_df = build_instance_df(data)
library_df  = build_library_df(data)
global_df   = build_globalname_df(data)
module_site_counter, module_inst_counter, total_prebid_instances = build_module_stats(data)

# -----------------------
# 🔢 Top‑line Metrics
# -----------------------

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total sites scanned", f"{site_count:,}")
col2.metric("Sites w/ Prebid.js", f"{sites_with_prebid:,}")
col3.metric("Total Prebid instances", f"{total_prebid_instances:,}")
col4.metric("Avg modules / Prebid instance", f"{(sum(len(extract_modules(i)) for i in data) / max(total_prebid_instances,1)):.1f}")

st.divider()

# -----------------------
# 🗂️ Tabs for Exploration
# -----------------------

tabs = st.tabs(["Versions", "Instances/site", "Libraries", "Global names", "Modules"])

# --- Versions Tab
with tabs[0]:
    st.subheader("Prebid.js version buckets")
    chart = (
        alt.Chart(version_df)
        .mark_bar()
        .encode(
            x=alt.X("bucket:N", title="Version bucket", sort=None),
            y=alt.Y("count:Q", title="Number of occurrences"),
            tooltip=["count"]
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)

# --- Instances/site Tab
with tabs[1]:
    st.subheader("Distribution of Prebid instances per site")
    chart = (
        alt.Chart(instance_df)
        .mark_bar()
        .encode(
            x=alt.X("cut_bin_0:N", title="Instances", sort=None),
            y=alt.Y("count:Q", title="Number of sites"),
            tooltip=["count"]
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)

# --- Libraries Tab
with tabs[2]:
    st.subheader("Popularity of external libraries")
    chart = (
        alt.Chart(library_df.head(30))  # Top 30 for readability
        .mark_bar()
        .encode(
            y=alt.Y("library:N", sort="-x", title="Library"),
            x=alt.X("count:Q", title="Sites"),
            tooltip=["count"]
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

# --- Global names Tab
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

# --- Modules Tab
with tabs[4]:
    st.subheader("Module popularity")

    category = st.selectbox("Select module category", list(module_site_counter.keys()))
    top_n = st.slider("Show top N modules", 5, 50, 20)

    df = pd.DataFrame({
        "Module": [m for m, _ in module_site_counter[category].most_common(top_n)],
        "Sites": [c for _, c in module_site_counter[category].most_common(top_n)],
        "Instances": [module_inst_counter[category][m] for m, _ in module_site_counter[category].most_common(top_n)],
    })

    bar = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y=alt.Y("Module:N", sort="-x"),
            x=alt.X("Sites:Q", title="Number of sites"),
            tooltip=["Sites", "Instances"]
        )
        .properties(height=500)
    )
    st.altair_chart(bar, use_container_width=True)

    with st.expander("Raw data table"):
        st.dataframe(df, use_container_width=True)

# -----------------------
# 🤝 Footer
# -----------------------

st.markdown(
    """
    <br>
    <center>
    Reach out with feedback 👉 <a href="mailto:support@prebid.org">support@prebid.org</a>
    </center>
    """,
    unsafe_allow_html=True,
)
