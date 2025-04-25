# streamlit_app.py  – Prebid Integration Monitor  (compact feed default, raw-JSON compatible)

import streamlit as st
import pandas as pd
import altair as alt
import requests, gzip, io, re, json, csv, pathlib
from collections import Counter
from typing import List, Dict, Any

# ---------- optional fast JSON ----------
try:
    import orjson
    jloads = lambda b: orjson.loads(b if isinstance(b, bytes) else b.encode())
    jdumps = lambda o: orjson.dumps(o, option=orjson.OPT_INDENT_2)
except ModuleNotFoundError:
    import json as _js
    jloads = lambda b: _js.loads(b if isinstance(b, str) else b.decode())
    jdumps = lambda o: _js.dumps(o, indent=2).encode()

# ---------- optional Parquet ----------
try:
    import pyarrow as pa, pyarrow.parquet as pq
    _PARQUET = True
except ModuleNotFoundError:
    _PARQUET = False

# -------------------------------------------------
# 🎨  UI / CSS
# -------------------------------------------------
st.set_page_config("Prebid Integration Monitor", "📊", "wide", "expanded")
st.markdown(
    """
    <style>
    html,body,[class*='css']{font-family:"Helvetica Neue",Arial,sans-serif;}
    .block-container{padding-top:3rem;padding-bottom:2rem;}
    [data-testid='stMetricValue']{font-size:1.75rem;font-weight:600;}
    footer{visibility:hidden;}
    </style>""",
    unsafe_allow_html=True,
)

import matplotlib as mpl
mpl.rcParams["text.usetex"] = False
mpl.rcParams["mathtext.default"] = "regular"

# -------------------------------------------------
# Remote compact feed URLs
# -------------------------------------------------
BASE = "https://raw.githubusercontent.com/prebid/prebid-integration-monitor/main/output/"
PARQUET_URL = BASE + "prebid_compact.parquet"
CSVGZ_URL   = BASE + "prebid_compact.csv.gz"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Prebid-Integration-App"})

# -------------------------------------------------
# 🔑 NEW slim_item() accepting *raw JSON* or *compact* rows
# -------------------------------------------------
def slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
    """
    • Accepts raw full-schema objects (from prebid_combined.json) *or*
      already-slim / compact rows (siteKey/version/…).
    • Returns a unified slim dict used by the rest of the app.
    """
    # --- site identifier
    site_key = (
        it.get("siteKey") or it.get("site") or it.get("domain")
        or it.get("url") or it.get("pageUrl") or "unknown"
    )

    # --- top-level lists (may be empty in raw schema)
    modules   = list(it.get("modules", []))
    libraries = list(it.get("libraries", []))
    globals_  = list(it.get("globals" , []))

    # --- handle raw prebidInstances (if present)
    pb_instances = it.get("prebidInstances")
    if isinstance(pb_instances, list):
        if not modules or not libraries or not globals_:
            for inst in pb_instances:
                modules   += inst.get("modules", [])
                libraries += inst.get("libraries", [])
                if inst.get("globalVarName"):
                    globals_.append(inst["globalVarName"])
    else:
        # compact schema: only instance count stored as pb_inst
        pb_instances = [{}] * int(it.get("pb_inst", 0))

    return {
        "siteKey": site_key,
        "version": it.get("version"),
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
        k = r["siteKey"]
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out

# -------------------------------------------------
# Compact ↔ slim conversion helpers
# -------------------------------------------------
def _d(cell):                          # safe decode
    if pd.isna(cell): return ""
    if isinstance(cell, bytes): return cell.decode()
    return str(cell)

def compact_df_to_slim(df: pd.DataFrame) -> List[Dict[str, Any]]:
    out=[]
    for _,r in df.iterrows():
        out.append({
            "siteKey": _d(r["siteKey"]),
            "version": _d(r["version"]) or None,
            "modules": _d(r["modules"]).split("|")   if r["modules"] is not None else [],
            "libraries": _d(r["libraries"]).split("|") if r["libraries"] is not None else [],
            "globals": _d(r["globals"]).split("|")   if r["globals"] is not None else [],
            "prebidInstances": [{}]*int(r["pb_inst"] or 0),
        })
    return out

def slim_to_compact_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame({
        "siteKey":   [r["siteKey"] for r in rows],
        "version":   [r["version"] for r in rows],
        "modules":   ["|".join(r["modules"]) for r in rows],
        "libraries": ["|".join(r["libraries"]) for r in rows],
        "globals":   ["|".join(r["globals"]) for r in rows],
        "pb_inst":   [len(r["prebidInstances"]) for r in rows],
    })

def read_compact(b: bytes, name:str)->List[Dict[str,Any]]:
    ext=pathlib.Path(name).suffix.lower()
    if ext==".parquet" and _PARQUET:
        df=pd.read_parquet(io.BytesIO(b))
    else:
        try: df=pd.read_csv(io.BytesIO(b))
        except: df=pd.read_csv(gzip.open(io.BytesIO(b)))
    return compact_df_to_slim(df)

def write_compact(rows):
    df=slim_to_compact_df(rows)
    if _PARQUET:
        buf=io.BytesIO(); pq.write_table(pa.Table.from_pandas(df),buf,compression="zstd")
        return buf.getvalue(),"prebid_compact.parquet","application/octet-stream"
    buf=io.BytesIO()
    with gzip.open(buf,"wt",newline="") as gz: df.to_csv(gz,index=False,quoting=csv.QUOTE_MINIMAL)
    return buf.getvalue(),"prebid_compact.csv.gz","application/gzip"

# -------------------------------------------------
# Default (remote) compact loader
# -------------------------------------------------
@st.cache_data(show_spinner=True)
def load_default():
    try:
        r=SESSION.get(PARQUET_URL,timeout=60); r.raise_for_status()
        df=pd.read_parquet(io.BytesIO(r.content))
    except Exception:
        r=SESSION.get(CSVGZ_URL,timeout=60); r.raise_for_status()
        df=pd.read_csv(gzip.open(io.BytesIO(r.content)))
    return dedupe(compact_df_to_slim(df))

# -------------------------------------------------
# Sidebar
# -------------------------------------------------
st.sidebar.header("Data source")
upload=st.sidebar.file_uploader(
    "Upload slim JSON / full JSON / compact file",
    type=["json","parquet","csv","gz"])
max_mods=st.sidebar.slider("Ignore sites with > N modules",50,500,300,25)

# -------------------------------------------------
# Load data
# -------------------------------------------------
with st.spinner("Loading data …"):
    if upload:
        data_bytes = upload.read()
        if upload.name.endswith((".parquet",".csv",".gz",".csv.gz")):
            rows = read_compact(data_bytes, upload.name)
        else:  # assume JSON (slim or full)
            rows = [slim_item(x) for x in jloads(data_bytes)]
    else:
        rows = load_default()

rows=[r for r in rows if len(r["modules"])<=max_mods]
if not rows: st.stop()

# -------------------------------------------------
# Metrics & downloads
# -------------------------------------------------
sites=len(rows)
sites_pb=sum(1 for d in rows if d["version"] or d["prebidInstances"])
inst_total=sum(len(d["prebidInstances"]) or 1 for d in rows)
avg_mods=sum(len(d["modules"]) for d in rows)/max(inst_total,1)

c1,c2,c3,c4=st.columns(4)
c1.metric("Total sites scanned",f"{sites:,}")
c2.metric("Sites w/ Prebid.js",f"{sites_pb:,}")
c3.metric("Total Prebid instances",f"{inst_total:,}")
c4.metric("Avg modules / instance",f"{avg_mods:.1f}")

st.download_button("💾 Slim JSON", jdumps(rows),
                   "prebid_slim.json","application/json")

cbytes,cname,ctype=write_compact(rows)
st.download_button("💾 Compact table",cbytes,cname,ctype)

st.divider()

# -------------------------------------------------
# Chart helpers
# -------------------------------------------------
def cat_ver(v):
    v=str(v or "").lstrip("v")
    try: m=int(re.split(r"[.-]",v)[0])
    except: return "Other"
    if m<=2: return "0.x-2.x"
    if m<=5: return "3.x-5.x"
    if m<=7: return "6.x-7.x"
    if m==8: return "8.x"
    if m==9: return "9.x"
    if m==10:return "10.x"
    return "Other"

def class_mod(m):
    m=m.lower()
    if "bidadapter" in m: return "Bid Adapter"
    if "rtdprovider" in m or "rtdmodule" in m: return "RTD Module"
    if "idsystem" in m or "userid" in m: return "ID System"
    if "analytics" in m: return "Analytics Adapter"
    return "Other"

VER_ORDER=["0.x-2.x","3.x-5.x","6.x-7.x","8.x","9.x","10.x","Other"]
INST_BINS=["0","1","2","3","4","5","6+"]

vers_df=pd.Series([cat_ver(d["version"]) for d in rows])\
        .value_counts().reindex(VER_ORDER,fill_value=0)\
        .reset_index(name="count").rename(columns={"index":"bucket"})

inst_df=pd.Series(pd.cut([len(d["prebidInstances"]) or 1 for d in rows],
                         [-.1,0,1,2,3,4,5,1e9],labels=INST_BINS))\
        .value_counts().reindex(INST_BINS,fill_value=0)\
        .reset_index(name="count").rename(columns={"index":"instances"})

lib_df=pd.Series([l for d in rows for l in d["libraries"]])\
       .value_counts().reset_index(name="count")\
       .rename(columns={"index":"library"})

glob_df=pd.Series([g for d in rows for g in d["globals"]])\
        .value_counts().reset_index(name="count")\
        .rename(columns={"index":"global"})

def module_stats(data):
     site,inst={k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")},{k:Counter() for k in ("Bid Adapter","RTD Module","ID System","Analytics Adapter","Other")}
     for d in data:
         for m in set(d["modules"]): site[class_mod(m)][m]+=1
         for m in d["modules"]:      inst[class_mod(m)][m]+=1
     return site,inst


mod_site,mod_inst=module_stats(rows)

# -------------------------------------------------
# Tabs & Charts
# -------------------------------------------------
tabs=st.tabs(["Versions","Instances/site","Libraries","Global names","Modules"])

with tabs[0]:
    st.altair_chart(alt.Chart(vers_df).mark_bar().encode(
        x=alt.X("bucket:N",sort=VER_ORDER),
        y="count:Q", tooltip=["count"]).properties(height=400),
        use_container_width=True)

with tabs[1]:
    st.altair_chart(alt.Chart(inst_df).mark_bar().encode(
        x=alt.X("instances:N",sort=INST_BINS),
        y="count:Q", tooltip=["count"]).properties(height=400),
        use_container_width=True)

with tabs[2]:
    st.altair_chart(alt.Chart(lib_df).mark_bar().encode(
        y=alt.Y("library:N",sort="-x"),
        x="count:Q", tooltip=["count"]).properties(height=600),
        use_container_width=True)
    st.dataframe(lib_df,use_container_width=True)
    st.download_button("CSV",lib_df.to_csv(index=False).encode(),"libraries.csv","text/csv")

with tabs[3]:
    st.altair_chart(alt.Chart(glob_df).mark_bar().encode(
        y=alt.Y("global:N",sort="-x"),
        x="count:Q", tooltip=["count"]).properties(height=500),
        use_container_width=True)
    st.dataframe(glob_df,use_container_width=True)
    st.download_button("CSV",glob_df.to_csv(index=False).encode(),"global_names.csv","text/csv")

with tabs[4]:
    cat=st.selectbox("Module category",list(mod_site.keys()))
    maxN=st.slider("Bar chart – top N modules",5,100,20,5)
    mdf=pd.DataFrame({
        "Module": list(mod_site[cat]),
        "Sites":[mod_site[cat][m] for m in mod_site[cat]],
        "Instances":[mod_inst[cat][m] for m in mod_inst[cat]],
    }).sort_values("Sites",ascending=False)
    st.altair_chart(alt.Chart(mdf.head(maxN)).mark_bar().encode(
        y=alt.Y("Module:N",sort="-x"),
        x="Sites:Q", tooltip=["Sites","Instances"]).properties(height=600),
        use_container_width=True)
    st.dataframe(mdf,use_container_width=True)
    st.download_button("CSV",mdf.to_csv(index=False).encode(),
                       f"modules_{cat.replace(' ','_').lower()}.csv","text/csv")

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.markdown(
    "<br><center>Feedback? <a href='mailto:chuie@prebid.org'>chuie@prebid.org</a></center>",
    unsafe_allow_html=True)
