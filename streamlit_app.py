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
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-Monitor-App"})

@st.cache_data(show_spinner=False)
def load_json_from_url(url: str):
    """Download and return JSON list from a raw GitHub (or any) URL."""
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

# GitHub API constants
ORG  = "prebid"
REPO = "prebid-integration-monitor"
BRANCH = "jlist"
API_BASE = f"https://api.github.com/repos/{ORG}/{REPO}/contents/output"

@st.cache_data(show_spinner=True)
def load_all_months():
    """Walk every month directory under /output and merge all JSON files."""
    top_resp = SESSION.get(f"{API_BASE}?ref={BRANCH}", timeout=30)
    top_resp.raise_for_status()
    months_meta = [item for item in top_resp.json() if item["type"] == "dir"]

    combined: list[dict] = []
    for month in months_meta:
        month_name = month["name"]
        month_api_url = month["url"]  # API path for directory already includes branch ref
        try:
            dir_resp = SESSION.get(f"{month_api_url}?ref={BRANCH}", timeout=30)
            dir_resp.raise_for_status()
        except Exception as e:
            st.warning(f"Skipping {month_name}: cannot list directory ({e})")
            continue

        files_meta = dir_resp.json()
        # Prefer a single results.json if present
        results_meta = next((f for f in files_meta if f["type"] == "file" and f["name"] == "results.json"), None)
        target_files = [results_meta] if results_meta else [f for f in files_meta if f["type"] == "file" and f["name"].endswith(".json")]

        for fmeta in target_files:
            raw_url = fmeta["download_url"]
            if not raw_url:
                # fallback to raw path construction
                path = fmeta["path"]  # e.g., output/Mar/2025-03-01.json
                raw_url = f"https://raw.githubusercontent.com/{ORG}/{REPO}/{BRANCH}/{path}"
            try:
                combined.extend(load_json_from_url(raw_url))
            except Exception as err:
                st.warning(f"Skipping {fmeta['name']} in {month_name}: {err}")
    return combined


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
    buckets = list(chain.from_iterable(
        categorize_version(v) for v in chain.from_iterable(extract_versions(i) for i in data)
    ))
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

DEFAULT_SINGLE_URL = f"https://raw.githubusercontent.com/{ORG}/{REPO}/{BRANCH}/output/results.json"
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
sites_with_prebid = sum(1 for i in data if count_prebid_instances
