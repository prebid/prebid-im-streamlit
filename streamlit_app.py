# streamlit_app.py  – Prebid Integration Monitor  (slim + dedupe + compact I/O)

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, json, csv, tempfile, os, pathlib
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

# ---------- optional Parquet ----------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PARQUET = True
except ModuleNotFoundError:
    _PARQUET = False

# -------------------------------------------------
# 🎨 Page config
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
# HTTP session
# -------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-Monitor-App"})

token = st.secrets.get("github_token")  # type: ignore[attr-defined]
if token:
    SESSION.headers["Authorization"] = f"token {token}"

ORG, REPO = "prebid", "prebid-integration-monitor"
COMBINED_URL = f"https://raw.githubusercontent.com/{ORG}/{REPO}/main/output/prebid_combined.json.gz"

# -------------------------------------------------
# Slim-object + dedupe helpers
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "siteKey": it.get("site") or it.get("domain") or it.get("url") or it.get("pageUrl"),
        "version": it.get("version"),
        "modules": it.get("modules", []),
        "libraries": it.get("libraries", []),
        "globals": [i.get("globalVarName") for i in it.get("prebidInstances", [])
                    if i.get("globalVarName")],
        "prebidInstances": [  # keep minimal for counts
            {
                "version": i.get("version"),
                "modules": i.get("modules", []),
            } for i in it.get("prebidInstances", [])
        ],
    }

def dedupe_sites(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for r in rows:
        key = r["siteKey"] or ""
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out

# -------------------------------------------------
# Compact ↔︎ slim conversions
# -------------------------------------------------
def slim_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert slim list → flat table (pipe-separated strings)."""
    return pd.DataFrame({
        "siteKey":   [r["siteKey"] for r in rows],
        "version":   [r["version"] for r in rows],
        "modules":   ["|".join(r["modules"])        for r in rows],
        "libraries": ["|".join(r["libraries"])      for r in rows],
        "globals":   ["|".join(r["globals"])        for r in rows],
        "pb_inst":   [len(r["prebidInstances"])     for r in rows],   # simple count
    })

def dataframe_to_slim(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert compact table back → slim list (for charting)."""
    out: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        out.append({
            "siteKey": row["siteKey"],
            "version": row["version"] if pd.notna(row["version"]) else None,
            "modules": row["modules"].split("|") if row["modules"] else [],
            "libraries": row["libraries"].split("|") if row["libraries"] else [],
            "globals": row["globals"].split("|") if row["globals"] else [],
            # prebidInstances preserved only as count for charts using count
            "prebidInstances": [{}]*int(row["pb_inst"]),
        })
    return out

def write_compact(rows: List[Dict[str, Any]]) -> bytes:
    """Return bytes of a compact file (Parquet or gz-CSV)."""
    df = slim_to_dataframe(rows)
    if _PARQUET:
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
        return buf.getvalue()
    else:
        buf = io.BytesIO()
        with gzip.open(buf, mode="wt", newline="") as gz:
            df.to_csv(gz, index=False, quoting=csv.QUOTE_MINIMAL)
        return buf.getvalue()

def read_compact(b: bytes, filename: str) -> List[Dict[str, Any]]:
    """Read Parquet or gz/CSV back to slim list."""
    ext = pathlib.Path(filename).suffix.lower()
    if ext == ".parquet" and _PARQUET:
        df = pd.read_parquet(io.BytesIO(b))
    else:
        # assume CSV or gz-CSV
        try:
            df = pd.read_csv(io.BytesIO(b))
        except Exception:  # maybe gzipped
            df = pd.read_csv(gzip.open(io.BytesIO(b)))
    return dataframe_to_slim(df)

# -------------------------------------------------
# Load slim data (combined feed only, for brevity)
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_combined_and_dedupe() -> List[Dict[str, Any]]:
    r = SESSION.get(COMBINED_URL, timeout=60); r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = jloads(gz.read())
    slimmed = [slim_item(x) for x in raw]
    return dedupe_sites(slimmed)

# -------------------------------------------------
# Sidebar UI
# -------------------------------------------------
st.sidebar.header("Data source")
upload = st.sidebar.file_uploader("Upload slim JSON / compact file", type=["json","parquet","csv","gz"])

MAX_MODS = st.sidebar.slider("Ignore sites with > N modules", 50, 500, 300, 25)

# -------------------------------------------------
# Load data
# -------------------------------------------------
with st.spinner("Loading data …"):
    if upload:
        data_bytes = upload.read()
        if upload.name.endswith((".json",".json.gz",".gz")):
            rows = jloads(gzip.decompress(data_bytes) if upload.name.endswith(".gz") else data_bytes)
            slim_rows = [slim_item(x) for x in rows]  # normalise
        else:
            slim_rows = read_compact(data_bytes, upload.name)
    else:
        slim_rows = load_combined_and_dedupe()

# apply module-count filter
data = [d for d in slim_rows if len(d["modules"]) <= MAX_MODS]
if not data:
    st.warning("No records after filtering."); st.stop()

# -------------------------------------------------
# Metrics & downloads
# -------------------------------------------------
sites = len(data)
sites_pb = sum(1 for d in data if d["version"] or d["prebidInstances"])
inst_total = sum(len(d["prebidInstances"]) or 1 for d in data)
avg_mods = sum(len(d["modules"]) for d in data) / max(inst_total, 1)

c1,c2,c3,c4 = st.columns(4)
c1.metric("Total sites scanned", f"{sites:,}")
c2.metric("Sites w/ Prebid.js", f"{sites_pb:,}")
c3.metric("Total Prebid instances", f"{inst_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

# slim JSON download (as before)
st.download_button(
    "💾 Slim JSON",
    jdumps(data),
    "prebid_slim.json",
    "application/json",
)

# compact Parquet/CSV download
compact_bytes = write_compact(data)
compact_name = "prebid_compact.parquet" if _PARQUET else "prebid_compact.csv.gz"
mime = "application/octet-stream" if _PARQUET else "application/gzip"
st.download_button(
    "💾 Compact table",
    compact_bytes,
    compact_name,
    mime,
    help="Parquet if pyarrow available, else gzipped CSV",
)

st.divider()

# -------------------------------------------------
# Charts (unchanged – build DataFrames from `data`)
# -------------------------------------------------
def categorize_version(v: str | None) -> str:
    if not v: return "Other"
    v = v.lstrip("v")
    try: major=int(re.split(r\"[.-]\",v)[0])
    except ValueError: return "Other"
    if major<=2: return "0.x-2.x"
    if 3<=major<=5: return "3.x-5.x"
    if 6<=major<=7: return "6.x-7.x"
    if major==8: return "8.x"
    if major==9: return "9.x"
    return "Other"

def classify_module(n:str)->str:
    if "BidAdapter" in n: return "Bid Adapter"
    if any(x in n for x in ("RtdProvider","rtdModule")): return "RTD Module"
    if any(x in n for x in ("IdSystem","userId")): return "ID System"
    if any(x in n for x in ("Analytics","analyticsAdapter")): return "Analytics Adapter"
    return "Other"

def extract_versions(d): return [d["version"]] if d["version"] else []
def count_instances(d):  return len(d["prebidInstances"]) or (1 if d["version"] else 0)

VER_ORDER = ["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","Other"]
INST_BINS = ["0","1","2","3","4","5","6+"]

vers_df  = pd.Series([categorize_version(v) for d in data for v in extract_versions(d)])\
            .value_counts().reindex(VER_ORDER,fill_value=0).reset_index(name="count")\
            .rename(columns={"index":"bucket"})

inst_df  = pd.Series(pd.cut([count_instances(d) for d in data],
            [-.1,0,1,2,3,4,5,float("inf")], labels=INST_BINS, include_lowest=True))\
            .value_counts().reindex(INST_BINS, fill_value=0).reset_index(name="count")\
            .rename(columns={"index":"instances"})

lib_df   = pd.Series([l for d in data for l in d["libraries"]])\
            .value_counts().reset_index(name="count").rename(columns={"index":"library"})

glob_df  = pd.Series([g for d in data for g in d["globals"]])\
            .value_counts().reset_index(name="count").rename(columns={"index":"global"})

def module_stats(rows):
    site, inst = {k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}, {k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    for d in rows:
        mods_site=set(d["modules"])
        for m in mods_site: site[classify_module(m)][m]+=1
        for m in d["modules"]: inst[classify_module(m)][m]+=1
    return site, inst

mod_site, mod_inst = module_stats(data)

tabs=st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

with tabs[0]:
    st.altair_chart(
        alt.Chart(vers_df).mark_bar().encode(
            x=alt.X("bucket:N",sort=VER_ORDER), y="count:Q", tooltip=["count"]
        ).properties(height=400), use_container_width=True)

with tabs[1]:
    st.altair_chart(
        alt.Chart(inst_df).mark_bar().encode(
            x=alt.X("instances:N",sort=INST_BINS), y="count:Q", tooltip=["count"]
        ).properties(height=400), use_container_width=True)

with tabs[2]:
    topN=st.slider("Show top N libraries",10,100,30,5)
    st.altair_chart(
        alt.Chart(lib_df.head(topN)).mark_bar().encode(
            y=alt.Y("library:N",sort="-x"), x="count:Q", tooltip=["count"]
        ).properties(height=600), use_container_width=True)

with tabs[3]:
    st.altair_chart(
        alt.Chart(glob_df).mark_bar().encode(
            y=alt.Y("global:N",sort="-x"), x="count:Q", tooltip=["count"]
        ).properties(height=500), use_container_width=True)

with tabs[4]:
    cat=st.selectbox("Module category", list(mod_site.keys()))
    top=st.slider("Bar chart – top N",5,100,20,5)
    df=pd.DataFrame({
        "Module": list(mod_site[cat]),
        "Sites": [mod_site[cat][m] for m in mod_site[cat]],
        "Instances": [mod_inst[cat][m] for m in mod_inst[cat]],
    }).sort_values("Sites",ascending=False)
    st.altair_chart(
        alt.Chart(df.head(top)).mark_bar().encode(
            y=alt.Y("Module:N",sort="-x"), x="Sites:Q", tooltip=["Sites","Instances"]
        ).properties(height=600), use_container_width=True)

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? <a href='mailto:support@prebid.org'>support@prebid.org</a></center>",
    unsafe_allow_html=True)
