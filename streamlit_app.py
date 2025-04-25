# streamlit_app.py – Prebid Integration Monitor
# Default feed: prebid_combined.json  |  Groups Voltax globals

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, csv, pathlib, json
from collections import Counter
from typing import List, Dict, Any

# ---------- optional fast JSON ----------
try:
    import orjson
    jloads = lambda b: orjson.loads(b if isinstance(b, bytes) else b.encode())
    jdumps = lambda o: orjson.dumps(o, option=orjson.OPT_INDENT_2)
except ModuleNotFoundError:
    jloads = lambda b: json.loads(b if isinstance(b, str) else b.decode())
    jdumps = lambda o: json.dumps(o, indent=2).encode()

# ---------- optional Parquet ----------
try:
    import pyarrow as pa, pyarrow.parquet as pq
    _PARQUET = True
except ModuleNotFoundError:
    _PARQUET = False

# -------------------------------------------------
# UI / CSS
# -------------------------------------------------
st.set_page_config("Prebid Integration Monitor", "📊",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <style>
    html,body,[class*='css']{font-family:"Helvetica Neue",Arial,sans-serif;}
    .block-container{padding-top:3rem;padding-bottom:2rem;}
    [data-testid='stMetricValue']{font-size:1.75rem;font-weight:600;}
    footer{visibility:hidden;}
    </style>
    """,
    unsafe_allow_html=True
)

# -------------------------------------------------
# Remote feed URLs  (raw JSON default)
# -------------------------------------------------
BASE = "https://raw.githubusercontent.com/prebid/prebid-integration-monitor/main/output/"
RAW_JSON = BASE + "prebid_combined.json"
RAW_JSON_GZ = RAW_JSON + ".gz"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-App"})

# -------------------------------------------------
# slim_item(): handles raw JSON or compact rows
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    site_key = (
        it.get("siteKey") or it.get("site") or it.get("domain")
        or it.get("url") or it.get("pageUrl") or "unknown"
    )

    modules   = list(it.get("modules", []))
    libraries = list(it.get("libraries", []))
    globals_  = list(it.get("globals" , []))

    pb_instances = it.get("prebidInstances")
    if isinstance(pb_instances, list):
        for inst in pb_instances:
            modules   += inst.get("modules" , [])
            libraries += inst.get("libraries", [])
            gv = inst.get("globalVarName")
            if gv: globals_.append(gv)
    else:
        pb_instances = [{}] * int(it.get("pb_inst", 0))

    version = it.get("version")
    if not version and isinstance(pb_instances, list):
        for inst in pb_instances:
            if inst.get("version"):
                version = inst["version"]; break

    return {
        "siteKey": site_key,
        "version": version,
        "modules": modules,
        "libraries": libraries,
        "globals": globals_,
        "prebidInstances": pb_instances,
    }

# -------------------------------------------------
# Deduplication
# -------------------------------------------------
def dedupe(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for r in rows:
        if r["siteKey"] not in seen:
            seen.add(r["siteKey"]); out.append(r)
    return out

# -------------------------------------------------
# Compact ↔ slim helpers
# -------------------------------------------------
def _d(x):
    if pd.isna(x): return ""
    return x.decode() if isinstance(x, bytes) else str(x)

def compact_df_to_slim(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows=[]
    for _,r in df.iterrows():
        rows.append({
            "siteKey": _d(r["siteKey"]),
            "version": _d(r["version"]) or None,
            "modules": _d(r["modules"]).split("|")   if r["modules"] is not None else [],
            "libraries": _d(r["libraries"]).split("|") if r["libraries"] is not None else [],
            "globals": _d(r["globals"]).split("|")   if r["globals"] is not None else [],
            "prebidInstances": [{}]*int(r["pb_inst"] or 0),
        })
    return rows

def slim_to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame({
        "siteKey":[r["siteKey"] for r in rows],
        "version":[r["version"] for r in rows],
        "modules":["|".join(r["modules"]) for r in rows],
        "libraries":["|".join(r["libraries"]) for r in rows],
        "globals":["|".join(r["globals"]) for r in rows],
        "pb_inst":[len(r["prebidInstances"]) for r in rows],
    })

def read_compact(byts: bytes, name: str) -> List[Dict[str, Any]]:
    ext = pathlib.Path(name).suffix.lower()
    if ext == ".parquet" and _PARQUET:
        df = pd.read_parquet(io.BytesIO(byts))
    else:
        try:
            df = pd.read_csv(io.BytesIO(byts))
        except Exception:
            df = pd.read_csv(gzip.open(io.BytesIO(byts)))
    return compact_df_to_slim(df)

def write_compact(rows):
    df = slim_to_df(rows)
    if _PARQUET:
        buf = io.BytesIO(); pq.write_table(pa.Table.from_pandas(df), buf, compression="zstd")
        return buf.getvalue(), "prebid_compact.parquet", "application/octet-stream"
    buf = io.BytesIO()
    with gzip.open(buf, "wt", newline="") as gz:
        df.to_csv(gz, index=False, quoting=csv.QUOTE_MINIMAL)
    return buf.getvalue(), "prebid_compact.csv.gz", "application/gzip"

# -------------------------------------------------
# Default loader – raw JSON
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_default():
    try:
        r = SESSION.get(RAW_JSON_GZ, timeout=60); r.raise_for_status()
        data = jloads(gzip.decompress(r.content))
    except Exception:
        r = SESSION.get(RAW_JSON, timeout=60); r.raise_for_status()
        data = jloads(r.content)
    return dedupe([slim_item(x) for x in data])

# -------------------------------------------------
# Sidebar & upload
# -------------------------------------------------
st.sidebar.header("Data source")
upload = st.sidebar.file_uploader(
    "Upload full JSON / slim JSON / compact file (Parquet/CSV)",
    type=["json", "parquet", "csv", "gz"]
)
max_mods = st.sidebar.slider("Ignore sites with > N modules", 50, 500, 300, 25)

# -------------------------------------------------
# Load + dedupe + filter
# -------------------------------------------------
with st.spinner("Loading data …"):
    if upload:
        byts = upload.read()
        if upload.name.endswith((".parquet", ".csv", ".gz", ".csv.gz")):
            rows = read_compact(byts, upload.name)
        else:
            rows = [slim_item(x) for x in jloads(byts)]
    else:
        rows = load_default()

rows = dedupe(rows)
rows = [r for r in rows if len(r["modules"]) <= max_mods]
if not rows:
    st.stop()

# -------------------------------------------------
# Voltax global grouping
# -------------------------------------------------
_voltax_re = re.compile(r"^voltaxPlayerPrebid-[A-Za-z0-9-]{5,}$")
def group_global(name: str) -> str:
    return "voltaxPlayerPrebid-*" if _voltax_re.match(name) else name

# -------------------------------------------------
# Metrics
# -------------------------------------------------
rows_pb = [d for d in rows if d["version"] or d["prebidInstances"]]
inst_total = sum(len(d["prebidInstances"]) for d in rows_pb)
mod_total  = sum(len(d["modules"]) * (len(d["prebidInstances"]) or 1) for d in rows_pb)
avg_mods = mod_total / inst_total if inst_total else 0

c1,c2,c3,c4 = st.columns(4)
c1.metric("Total sites scanned", f"{len(rows):,}")
c2.metric("Sites w/ Prebid.js", f"{len(rows_pb):,}")
c3.metric("Total Prebid instances", f"{inst_total:,}")
c4.metric("Avg modules / instance", f"{avg_mods:.1f}")

st.download_button("💾 Slim JSON", jdumps(rows), "prebid_slim.json","application/json")
buf,name,mime = write_compact(rows)
st.download_button("💾 Compact table", buf, name, mime)
st.divider()

# -------------------------------------------------
# Categorisation helpers
# -------------------------------------------------
def cat_ver(v):
    v = str(v or "").lstrip("v")
    try:
        m = int(re.split(r"[.-]", v)[0])
    except: return "Other"
    return (
        "0.x-2.x" if m <= 2 else
        "3.x-5.x" if m <= 5 else
        "6.x-7.x" if m <= 7 else
        "8.x"     if m == 8 else
        "9.x"     if m == 9 else
        "10.x"    if m == 10 else
        "Other"
    )

def class_mod(m):
    m = m.lower()
    if "bidadapter" in m:               return "Bid Adapter"
    if "rtdprovider" in m or "rtdmodule" in m: return "RTD Module"
    if "idsystem" in m or "userid" in m: return "ID System"
    if "analytics" in m:                return "Analytics Adapter"
    return "Other"

VER_ORDER = ["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","10.x","Other"]
INST_BINS = ["0","1","2","3","4","5","6+"]

vers_df = pd.Series([cat_ver(d["version"]) for d in rows_pb])\
          .value_counts().reindex(VER_ORDER, fill_value=0)\
          .reset_index(name="count").rename(columns={"index":"bucket"})
inst_df = pd.Series(pd.cut([len(d["prebidInstances"]) for d in rows],
          [-.1,0,1,2,3,4,5,1e9], labels=INST_BINS))\
          .value_counts().reindex(INST_BINS, fill_value=0)\
          .reset_index(name="count").rename(columns={"index":"instances"})
lib_df  = pd.Series([l for d in rows for l in d["libraries"]])\
          .value_counts().reset_index(name="count").rename(columns={"index":"library"})
glob_df = pd.Series([group_global(g) for d in rows for g in d["globals"]])\
          .value_counts().reset_index(name="count").rename(columns={"index":"global"})

def module_stats(data):
    site_ctr = {k: Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
    inst_ctr = {k: Counter() for k in site_ctr}
    for d in data:
        inst_count = len(d["prebidInstances"]) or 1
        for m in set(d["modules"]):
            cat = class_mod(m)
            site_ctr[cat][m] += 1
            inst_ctr[cat][m] += inst_count
    return site_ctr, inst_ctr

mod_site, mod_inst = module_stats(rows)

# -------------------------------------------------
# Tabs & Charts
# -------------------------------------------------
tabs = st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

with tabs[0]:
    st.altair_chart(
        alt.Chart(vers_df).mark_bar().encode(
            x=alt.X("bucket:N", sort=VER_ORDER),
            y="count:Q", tooltip=["count"]
        ).properties(height=400), use_container_width=True)

with tabs[1]:
    st.altair_chart(
        alt.Chart(inst_df).mark_bar().encode(
            x=alt.X("instances:N", sort=INST_BINS),
            y="count:Q", tooltip=["count"]
        ).properties(height=400), use_container_width=True)

with tabs[2]:
    st.altair_chart(
        alt.Chart(lib_df).mark_bar().encode(
            y=alt.Y("library:N", sort="-x"),
            x="count:Q", tooltip=["count"]
        ).properties(height=600), use_container_width=True)
    st.dataframe(lib_df, use_container_width=True)
    st.download_button("CSV", lib_df.to_csv(index=False).encode(), "libraries.csv","text/csv")

with tabs[3]:
    st.altair_chart(
        alt.Chart(glob_df).mark_bar().encode(
            y=alt.Y("global:N", sort="-x"),
            x="count:Q", tooltip=["count"]
        ).properties(height=500), use_container_width=True)
    st.dataframe(glob_df, use_container_width=True)
    st.download_button("CSV", glob_df.to_csv(index=False).encode(), "global_names.csv","text/csv")

with tabs[4]:
    cat = st.selectbox("Module category", list(mod_site.keys()))
    topN = st.slider("Bar chart – top N modules", 5, 100, 20, 5)
    mdf = pd.DataFrame({
        "Module": list(mod_site[cat]),
        "Sites": [mod_site[cat][m] for m in mod_site[cat]],
        "Instances": [mod_inst[cat][m] for m in mod_inst[cat]],
    }).sort_values("Sites", ascending=False)
    st.altair_chart(
        alt.Chart(mdf.head(topN)).mark_bar().encode(
            y=alt.Y("Module:N", sort="-x"),
            x="Sites:Q", tooltip=["Sites","Instances"]
        ).properties(height=600), use_container_width=True)
    st.dataframe(mdf, use_container_width=True)
    st.download_button("CSV", mdf.to_csv(index=False).encode(),
                       f"modules_{cat.replace(' ','_').lower()}.csv","text/csv")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? <a href='mailto:chuie@prebid.org'>chuie@prebid.org</a></center>",
    unsafe_allow_html=True)
