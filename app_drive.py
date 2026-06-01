"""
D2C Sales Dashboard v4 — memory-efficient
=========================================
Built for 1-5M+ sales rows on low-RAM hosting. DuckDB reads the sales Parquet/CSV
directly from disk (streaming, low memory) and joins to the small master SKU sheet
inside the database. No giant pandas frame is held in RAM.

Sources:
  SALES : Parquet/CSV in Google Drive (DRIVE_FILE_ID) or local sample.
  MASTER: live Google Sheet (MASTER_SHEET_ID + MASTER_SHEET_TAB), keyed by sku_code (small).
Join: sales.sku (text) <-> master.sku_code (text)
"""
import streamlit as st
import traceback

try:
    import os, io, tempfile
    import duckdb
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from pathlib import Path
except Exception:
    st.title("Startup error"); st.error("Import failed:"); st.code(traceback.format_exc()); st.stop()

HERE = Path(__file__).parent
SALES_LOCAL = HERE / "sales.parquet"
MASTER_LOCAL = HERE / "master.csv"

st.set_page_config(page_title="D2C Sales Dashboard", layout="wide", initial_sidebar_state="expanded")

# ---------- password ----------
def check_password():
    def entered():
        if st.session_state.get("pw","")==st.secrets.get("APP_PASSWORD",""):
            st.session_state["auth_ok"]=True; del st.session_state["pw"]
        else: st.session_state["auth_ok"]=False
    if st.session_state.get("auth_ok",False): return True
    st.text_input("Password", type="password", key="pw", on_change=entered)
    if st.session_state.get("auth_ok") is False: st.error("Incorrect password.")
    st.stop()
if st.secrets.get("APP_PASSWORD",""):
    check_password()

DRIVE_FILE_ID    = st.secrets.get("DRIVE_FILE_ID","")
MASTER_SHEET_ID  = st.secrets.get("MASTER_SHEET_ID","")
MASTER_SHEET_TAB = st.secrets.get("MASTER_SHEET_TAB","Master SKU")

# ---------- get sales file onto local disk (Drive download or local sample) ----------
@st.cache_data(ttl=3600, show_spinner="Loading sales data…")
def sales_file_path() -> str:
    if not DRIVE_FILE_ID:
        return SALES_LOCAL.as_posix()
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    info=dict(st.secrets["gcp_service_account"])
    creds=service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    svc=build("drive","v3",credentials=creds)
    req=svc.files().get_media(fileId=DRIVE_FILE_ID)
    out=Path(tempfile.gettempdir())/"sales_data_file"
    with io.FileIO(out,"wb") as fh:
        dl=MediaIoBaseDownload(fh,req); done=False
        while not done: _,done=dl.next_chunk()
    return out.as_posix()

# ---------- load master (small) ----------
@st.cache_data(ttl=3600, show_spinner="Loading product master…")
def load_master() -> pd.DataFrame:
    if MASTER_SHEET_ID:
        try:
            import gspread
            from google.oauth2 import service_account
            info=dict(st.secrets["gcp_service_account"])
            creds=service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
            gc=gspread.authorize(creds)
            ws=gc.open_by_key(MASTER_SHEET_ID).worksheet(MASTER_SHEET_TAB)
            df=pd.DataFrame(ws.get_all_records())
        except Exception as e:
            st.session_state["_master_error"]=str(e)[:300]
            return pd.DataFrame()
    else:
        if not MASTER_LOCAL.exists(): return pd.DataFrame()
        df=pd.read_csv(MASTER_LOCAL)
    if df.empty: return df
    df.columns=[str(c).strip() for c in df.columns]
    key=None
    for c in df.columns:
        if c.lower().replace(" ","")=="sku_code": key=c; break
    if key is None:
        for c in df.columns:
            if "sku" in c.lower(): key=c; break
    if key is None: return pd.DataFrame()
    df=df.rename(columns={key:"sku_code"})
    df["sku_code"]=df["sku_code"].astype(str).str.strip()
    return df

SALES_COLS={"marketplaces","date","mon","yr","sku","product_code_planning","color_code",
            "qty","subtotal","reference_code","product_code","Qty","Subtotal"}

@st.cache_data(ttl=3600)
def classify(master: pd.DataFrame):
    cat,num,label=[],[],[]
    if master.empty: return cat,num,label
    SKIP={"product code","color code","category code","size code","style code","style no",
          "product code planning","sku_code","key","accounting sku"}
    NUMHINT={"asp","mrp","cogs","price","cost"}
    for c in master.columns:
        if c=="sku_code" or c in SALES_COLS: continue
        lc=c.lower().strip()
        if lc in SKIP or lc.endswith("code") or lc.endswith("sku code") or "asin" in lc: continue
        s=master[c]; nun=s.nunique(dropna=True)
        isn=pd.to_numeric(s,errors="coerce").notna().mean()>0.8
        if isn and any(h in lc for h in NUMHINT): num.append(c); continue
        if isn and nun>40: continue
        if 1<nun<=60: cat.append(c)
        else: label.append(c)
    return cat,num,label

# ---------- build a DuckDB connection that reads sales from disk + registers small master ----------
@st.cache_resource(show_spinner="Preparing database…")
def get_db():
    """Returns (con, sales_cols). DuckDB reads the parquet/csv lazily from disk."""
    path=sales_file_path()
    con=duckdb.connect(":memory:")
    # Create a VIEW over the file (does NOT load into memory; scans on demand)
    if path.endswith(".csv"):
        con.execute(f"CREATE VIEW sales_raw AS SELECT * FROM read_csv_auto('{path}')")
    else:
        con.execute(f"CREATE VIEW sales_raw AS SELECT * FROM read_parquet('{path}')")
    cols=[r[0] for r in con.execute("DESCRIBE sales_raw").fetchall()]
    # detect canonical columns
    low={c.lower():c for c in cols}
    def pick(*cs):
        for x in cs:
            if x in low: return low[x]
        return None
    rev=pick("subtotal","revenue","sales","amount","gmv")
    qty=pick("qty","quantity","units")
    sku=pick("sku","sku_code")
    chan=pick("marketplaces","marketplace","channel","platform")
    dat=pick("date","order_date","txn_date")
    # build a normalized view with canonical names + sku as text
    sel=[]
    sel.append(f"CAST({sku} AS VARCHAR) AS sku" if sku else "'' AS sku")
    sel.append(f"{rev} AS subtotal" if rev else "0.0 AS subtotal")
    sel.append(f"{qty} AS qty" if qty else "0 AS qty")
    sel.append(f"{chan} AS marketplaces" if chan else "'Unknown' AS marketplaces")
    sel.append(f"CAST({dat} AS DATE) AS date" if dat else "CURRENT_DATE AS date")
    con.execute(f"CREATE VIEW sales AS SELECT {', '.join(sel)}, "
                f"EXTRACT(month FROM CAST({dat} AS DATE)) AS mon, "
                f"EXTRACT(year FROM CAST({dat} AS DATE)) AS yr FROM sales_raw")
    return con

@st.cache_data(ttl=3600)
def setup():
    """Register master into the db connection and create the joined view. Returns filter metadata."""
    con=get_db()
    master=load_master()
    cat,num,label=classify(master)
    if master.empty:
        con.execute("CREATE OR REPLACE VIEW joined AS SELECT *, FALSE AS _matched FROM sales")
        return [],[],[],0.0
    keep=["sku_code"]+cat+num+label
    keep=[c for c in keep if c in master.columns]
    m=master[keep].drop_duplicates("sku_code").copy()
    for c in num:
        if c in m.columns: m[c]=pd.to_numeric(m[c],errors="coerce")
    con.register("master_df", m)
    con.execute("CREATE OR REPLACE VIEW joined AS "
                "SELECT s.*, m.*, (m.sku_code IS NOT NULL) AS _matched "
                "FROM sales s LEFT JOIN master_df m ON s.sku = m.sku_code")
    # match rate (cheap aggregate, not a full load)
    mr=con.execute("SELECT AVG(CASE WHEN _matched THEN 1.0 ELSE 0.0 END)*100 FROM joined").fetchone()[0]
    return cat,num,label,(mr or 0.0)

try:
    CAT,NUM,LABEL,MATCH = setup()
    con=get_db()
except Exception:
    st.title("Data load error"); st.error("Failed while preparing data:")
    st.code(traceback.format_exc())
    st.info("Check: Drive file shared with service account & is Parquet/CSV; master sheet shared.")
    st.stop()

def Q(sql): return con.execute(sql).df()

# bounds for date picker (cheap min/max query)
try:
    b=Q("SELECT min(date) lo, max(date) hi FROM sales").iloc[0]
    DMIN=pd.to_datetime(b.lo).date(); DMAX=pd.to_datetime(b.hi).date()
except Exception:
    import datetime as _dt; DMIN=_dt.date(2024,1,1); DMAX=_dt.date.today()

# ---------- sidebar ----------
st.sidebar.title("Controls")
metric=st.sidebar.radio("Metric",["subtotal","qty"],horizontal=True,
                        format_func=lambda x:"Revenue" if x=="subtotal" else "Units")
mlab="Revenue" if metric=="subtotal" else "Units"; agg=f"SUM({metric})"
dr=st.sidebar.date_input("Date range", value=(DMIN,DMAX), min_value=DMIN, max_value=DMAX)
start,end=(dr if isinstance(dr,tuple) and len(dr)==2 else (DMIN,DMAX))

def distinct(col):
    try:
        return [str(r[0]) for r in con.execute(f'SELECT DISTINCT "{col}" FROM joined WHERE "{col}" IS NOT NULL ORDER BY 1').fetchall() if str(r[0])!=""]
    except Exception:
        return []

selected={}
ch=st.sidebar.multiselect("Channel", distinct("marketplaces"))
if ch: selected["marketplaces"]=ch
if CAT:
    st.sidebar.markdown("**Product filters**")
    for c in CAT[:6]:
        v=st.sidebar.multiselect(c, distinct(c))
        if v: selected[c]=v
    if CAT[6:] or NUM:
        with st.sidebar.expander("More filters"):
            for c in CAT[6:]:
                v=st.multiselect(c, distinct(c))
                if v: selected[c]=v
            for c in NUM:
                try:
                    r=con.execute(f'SELECT min("{c}") lo, max("{c}") hi FROM joined').fetchone()
                    if r and r[0] is not None and r[1] is not None and r[0]<r[1]:
                        rng=st.slider(c, float(r[0]), float(r[1]), (float(r[0]),float(r[1])))
                        selected["__num__"+c]=rng
                except Exception: pass

def sin(col,vals): return "\""+col+"\" IN ("+",".join("'"+v.replace("'","''")+"'" for v in vals)+")"
wheres=[f"date BETWEEN '{start}' AND '{end}'"]
for col,vals in selected.items():
    if col.startswith("__num__"):
        c=col[7:]; wheres.append(f'"{c}" BETWEEN {vals[0]} AND {vals[1]}')
    else:
        wheres.append(sin(col,vals))
WHERE=" AND ".join(wheres)

# ---------- header + KPIs ----------
st.title("D2C Sales Dashboard")
active=[f"{k}: {', '.join(v)}" for k,v in selected.items() if not k.startswith('__num__')]
st.caption(f"{mlab} · {start} → {end}"+(" · "+" · ".join(active) if active else ""))
if not CAT:
    merr=st.session_state.get("_master_error","")
    if merr: st.info(f"Master sheet not loaded ({'permission — share the sheet with the service account' if 'ermission' in merr or '403' in merr else merr}). Showing sales-only views.")
    else: st.info("No master attributes — connect the master sheet to unlock product filters.")
elif MATCH<99:
    st.warning(f"SKU match rate: {MATCH:.1f}%. Unmatched rows count in totals but carry no attributes.")

try:
    k=Q(f"SELECT SUM(subtotal) rev, SUM(qty) units, COUNT(*) txns, COUNT(DISTINCT sku) skus FROM joined WHERE {WHERE}").iloc[0]
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Revenue",f"{(k.rev or 0):,.0f}"); c2.metric("Units",f"{(k.units or 0):,.0f}")
    c3.metric("Transactions",f"{(k.txns or 0):,.0f}"); c4.metric("Active SKUs",f"{(k.skus or 0):,.0f}")
except Exception:
    st.error("Could not compute KPIs."); st.code(traceback.format_exc())
st.divider()

tabs=["📈 Trend","🛒 Channel"]+(["🧩 By Attribute"] if CAT else [])+["🔀 Compare","📦 SKUs","🧮 Pivot"]
T=dict(zip(tabs, st.tabs(tabs)))

with T["📈 Trend"]:
    g=st.radio("Granularity",["Daily","Weekly","Monthly"],horizontal=True,index=2,key="g")
    tr={"Daily":"day","Weekly":"week","Monthly":"month"}[g]
    df=Q(f"SELECT date_trunc('{tr}',date) period,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 1")
    st.plotly_chart(px.area(df,x="period",y="v",title=f"{g} {mlab}").update_layout(height=420,yaxis_title=mlab,xaxis_title=None),use_container_width=True)

with T["🛒 Channel"]:
    df=Q(f"SELECT date_trunc('month',date) period,marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1,2 ORDER BY 1")
    st.plotly_chart(px.line(df,x="period",y="v",color="marketplaces",markers=True,title=f"Monthly {mlab} by Channel").update_layout(height=420),use_container_width=True)
    sh=Q(f"SELECT marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 2 DESC")
    a,b=st.columns(2)
    a.plotly_chart(px.pie(sh,names="marketplaces",values="v",hole=0.45,title="Channel Share"),use_container_width=True)
    b.plotly_chart(px.bar(sh,x="marketplaces",y="v",text_auto=".2s",title="Channel Totals"),use_container_width=True)

if CAT:
    with T["🧩 By Attribute"]:
        dim=st.selectbox("Break down by",CAT)
        df=Q(f'SELECT "{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC')
        st.plotly_chart(px.bar(df,x="v",y="k",orientation="h",text_auto=".2s",title=f"{mlab} by {dim}").update_layout(height=max(400,len(df)*26),yaxis=dict(categoryorder="total ascending"),yaxis_title=None,xaxis_title=mlab),use_container_width=True)
        trd=Q(f'SELECT date_trunc(\'month\',date) period,"{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1,2 ORDER BY 1')
        st.plotly_chart(px.line(trd,x="period",y="v",color="k",markers=True,title=f"Monthly {mlab} by {dim}").update_layout(height=420),use_container_width=True)

with T["🔀 Compare"]:
    mode=st.selectbox("Comparison",["Year over Year","Month over Month"])
    if mode=="Year over Year":
        df=Q(f'SELECT mon "month",yr "year",{agg} v FROM joined WHERE {WHERE} GROUP BY 1,2 ORDER BY 1,2'); df["year"]=df["year"].astype(str)
        st.plotly_chart(px.line(df,x="month",y="v",color="year",markers=True,title=f"YoY {mlab}").update_layout(height=440),use_container_width=True)
    else:
        df=Q(f"SELECT date_trunc('month',date) period,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 1"); df["MoM %"]=(df["v"].pct_change()*100).round(1)
        fig=go.Figure(); fig.add_bar(x=df["period"],y=df["v"],name=mlab)
        fig.add_trace(go.Scatter(x=df["period"],y=df["MoM %"],name="MoM %",yaxis="y2",mode="lines+markers"))
        fig.update_layout(height=440,yaxis=dict(title=mlab),yaxis2=dict(title="MoM %",overlaying="y",side="right"))
        st.plotly_chart(fig,use_container_width=True)

with T["📦 SKUs"]:
    n=st.slider("Top N SKUs",5,50,15)
    namecol=next((c for c in LABEL if "name" in c.lower()),None)
    sel=f'sku, "{namecol}"' if namecol else "sku"
    df=Q(f'SELECT {sel}, SUM(subtotal) rev, SUM(qty) units FROM joined WHERE {WHERE} GROUP BY {sel} ORDER BY {"rev" if metric=="subtotal" else "units"} DESC LIMIT {n}')
    st.dataframe(df,use_container_width=True)

with T["🧮 Pivot"]:
    rowopts=(CAT if CAT else [])+["marketplaces"]
    rd=st.selectbox("Rows",rowopts)
    df=Q(f'SELECT "{rd}" r, yr||\'-\'||lpad(mon::VARCHAR,2,\'0\') ym,{agg} v FROM joined WHERE {WHERE} AND "{rd}" IS NOT NULL GROUP BY 1,2 ORDER BY 2')
    piv=df.pivot(index="r",columns="ym",values="v").fillna(0)
    st.dataframe(piv.style.format("{:,.0f}"),use_container_width=True)
    st.download_button("Download CSV",piv.to_csv().encode(),"pivot.csv","text/csv")
