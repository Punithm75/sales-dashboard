"""
D2C Sales Dashboard v3 — fully dynamic
======================================
Joins sales data (sku) to a master SKU Google Sheet (sku_code) and AUTO-BUILDS
filters from whatever columns the sheet contains. No code edits needed when the
sheet changes — the app inspects the data and adapts.

Sources:
  SALES  : Parquet/CSV in Google Drive (DRIVE_FILE_ID) or local sample.
           Columns: marketplaces, date, mon, yr, sku, product_code_planning, color_code, qty, subtotal
  MASTER : live Google Sheet (MASTER_SHEET_ID + MASTER_SHEET_TAB), keyed by sku_code.

Join: sales.sku (text) <-> master.sku_code (text)
"""
import os, io, tempfile
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

HERE = Path(__file__).parent
SALES_LOCAL = HERE / "sales.parquet"
MASTER_LOCAL = HERE / "master.csv"

st.set_page_config(page_title="D2C Sales Dashboard", layout="wide",
                   initial_sidebar_state="expanded")

# ---------- password gate ----------
def check_password():
    def entered():
        if st.session_state.get("pw","") == st.secrets.get("APP_PASSWORD",""):
            st.session_state["auth_ok"]=True; del st.session_state["pw"]
        else:
            st.session_state["auth_ok"]=False
    if st.session_state.get("auth_ok",False): return True
    st.text_input("Password", type="password", key="pw", on_change=entered)
    if st.session_state.get("auth_ok") is False: st.error("Incorrect password.")
    st.stop()
if st.secrets.get("APP_PASSWORD",""):
    check_password()

DRIVE_FILE_ID    = st.secrets.get("DRIVE_FILE_ID", os.environ.get("DRIVE_FILE_ID",""))
MASTER_SHEET_ID  = st.secrets.get("MASTER_SHEET_ID", os.environ.get("MASTER_SHEET_ID",""))
MASTER_SHEET_TAB = st.secrets.get("MASTER_SHEET_TAB", os.environ.get("MASTER_SHEET_TAB","Master SKU"))

# ---------- known sales columns (everything else in master = attribute) ----------
SALES_COLS = {"marketplaces","date","mon","yr","sku","product_code_planning",
              "color_code","qty","subtotal","Qty","Subtotal","product_code"}

# ---------- load sales ----------
@st.cache_resource(ttl=3600)
def fetch_sales_from_drive() -> str:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    svc = build("drive","v3",credentials=creds)
    req = svc.files().get_media(fileId=DRIVE_FILE_ID)
    out = Path(tempfile.gettempdir())/"sales_from_drive"
    with io.FileIO(out,"wb") as fh:
        dl=MediaIoBaseDownload(fh,req); done=False
        while not done: _,done=dl.next_chunk()
    return out.as_posix()

def sales_path() -> str:
    return fetch_sales_from_drive() if DRIVE_FILE_ID else SALES_LOCAL.as_posix()

@st.cache_data(ttl=3600)
def load_sales() -> pd.DataFrame:
    p = sales_path()
    try:
        df = pd.read_parquet(p)
    except Exception:
        df = pd.read_csv(p)
    df.columns=[str(c).strip() for c in df.columns]
    # normalise the metric column names so the rest of the app is stable
    ren={}
    if "Qty" in df.columns and "qty" not in df.columns: ren["Qty"]="qty"
    if "Subtotal" in df.columns and "subtotal" not in df.columns: ren["Subtotal"]="subtotal"
    df=df.rename(columns=ren)
    if "sku" in df.columns: df["sku"]=df["sku"].astype(str).str.strip()
    df["date"]=pd.to_datetime(df["date"])
    if "mon" not in df.columns: df["mon"]=df["date"].dt.month
    if "yr" not in df.columns: df["yr"]=df["date"].dt.year
    return df

# ---------- load master (live sheet or local sample) ----------
@st.cache_data(ttl=3600)
def load_master() -> pd.DataFrame:
    if MASTER_SHEET_ID:
        import gspread
        from google.oauth2 import service_account
        info=dict(st.secrets["gcp_service_account"])
        creds=service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        gc=gspread.authorize(creds)
        ws=gc.open_by_key(MASTER_SHEET_ID).worksheet(MASTER_SHEET_TAB)
        df=pd.DataFrame(ws.get_all_records())
    else:
        if not MASTER_LOCAL.exists(): return pd.DataFrame()
        df=pd.read_csv(MASTER_LOCAL)
    if df.empty: return df
    df.columns=[str(c).strip() for c in df.columns]
    # find the join key column (sku_code, or first column containing 'sku')
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

# ---------- auto-classify master columns into filter types ----------
@st.cache_data(ttl=3600)
def classify(master: pd.DataFrame):
    """Return (categorical_cols, numeric_cols, label_cols) auto-detected."""
    cat, num, label = [], [], []
    if master.empty: return cat, num, label
    # columns to never surface as filters (join/code/id-like)
    SKIP = {"product code","color code","category code","size code","style code",
            "style no","product code planning","sku_code","key","accounting sku"}
    # columns that should be numeric range sliders if numeric (price/cost-like)
    NUMHINT = {"asp","mrp","cogs","price","cost"}
    for c in master.columns:
        if c=="sku_code" or c in SALES_COLS: continue
        lc=c.lower().strip()
        if lc in SKIP or lc.endswith("code") or lc.endswith("sku code") or "asin" in lc:
            continue
        s=master[c]
        nun=s.nunique(dropna=True)
        asnum=pd.to_numeric(s, errors="coerce")
        is_numeric = asnum.notna().mean()>0.8
        # price/cost-like numeric -> slider
        if is_numeric and any(h in lc for h in NUMHINT):
            num.append(c); continue
        # other high-cardinality numeric (ids etc) -> skip as filter
        if is_numeric and nun>40:
            continue
        # low-cardinality -> categorical filter
        if 1 < nun <= 60:
            cat.append(c)
        else:
            label.append(c)
    return cat, num, label

# ---------- build joined frame ----------
@st.cache_data(ttl=3600)
def build_joined():
    sales=load_sales()
    master=load_master()
    if master.empty or "sku" not in sales.columns:
        sales["_matched"]=False
        return sales, [], [], []
    cat,num,label=classify(master)
    keep=["sku_code"]+cat+num+label
    keep=[c for c in keep if c in master.columns]
    m=master[keep].drop_duplicates("sku_code")
    for c in num:
        if c in m.columns: m[c]=pd.to_numeric(m[c], errors="coerce")
    j=sales.merge(m, how="left", left_on="sku", right_on="sku_code")
    j["_matched"]=j["sku_code"].notna()
    return j, cat, num, label

con=duckdb.connect(":memory:")
@st.cache_data(ttl=3600)
def register():
    j,cat,num,label=build_joined()
    return j,cat,num,label

jdf,CAT,NUM,LABEL=register()
con.register("joined", jdf)

def Q(sql): return con.execute(sql).df()

# ---------- sidebar ----------
st.sidebar.title("Controls")
metric=st.sidebar.radio("Metric",["subtotal","qty"],horizontal=True,
                        format_func=lambda x:"Revenue" if x=="subtotal" else "Units")
mlab="Revenue" if metric=="subtotal" else "Units"
agg=f"SUM({metric})"

dmin=pd.to_datetime(jdf["date"]).min().date()
dmax=pd.to_datetime(jdf["date"]).max().date()
dr=st.sidebar.date_input("Date range", value=(dmin,dmax), min_value=dmin, max_value=dmax)
start,end=(dr if isinstance(dr,tuple) and len(dr)==2 else (dmin,dmax))

def ms(label,col,container=st.sidebar):
    if col not in jdf.columns: return None
    opts=sorted([str(x) for x in jdf[col].dropna().unique() if str(x)!=""])
    if not opts: return None
    v=container.multiselect(label,opts)
    return v or None

selected={}
if "marketplaces" in jdf.columns:
    v=ms("Channel","marketplaces");  selected["marketplaces"]=v if v else None

# auto-built product filters (first ~6 categorical up front, rest in expander)
if CAT:
    st.sidebar.markdown("**Product filters**")
    primary=CAT[:6]; rest=CAT[6:]
    for c in primary:
        v=ms(c,c)
        if v: selected[c]=v
    if rest or NUM:
        with st.sidebar.expander("More filters"):
            for c in rest:
                v=ms(c,c,container=st)
                if v: selected[c]=v
            num_ranges={}
            for c in NUM:
                if jdf[c].notna().any():
                    lo,hi=float(jdf[c].min()),float(jdf[c].max())
                    if lo<hi:
                        num_ranges[c]=st.slider(c,lo,hi,(lo,hi))
else:
    num_ranges={}

# where clause
def sin(col,vals): return "\""+col+"\" IN ("+",".join("'"+v.replace("'","''")+"'" for v in vals)+")"
wheres=[f"date BETWEEN '{start}' AND '{end}'"]
for col,vals in selected.items():
    if vals: wheres.append(sin(col,vals))
for c,(lo,hi) in (num_ranges.items() if 'num_ranges' in dir() else []):
    wheres.append(f"\"{c}\" BETWEEN {lo} AND {hi}")
WHERE=" AND ".join(wheres)

# ---------- header + KPIs ----------
st.title("D2C Sales Dashboard")
active=[f"{k}: {', '.join(v)}" for k,v in selected.items() if v]
st.caption(f"{mlab} · {start} → {end}"+(" · "+" · ".join(active) if active else ""))

matched=jdf["_matched"].mean()*100 if "_matched" in jdf.columns else 0
if not CAT:
    st.info("No master attributes loaded yet — connect the master sheet (MASTER_SHEET_ID) to unlock product filters. Showing sales-only views.")
elif matched<99:
    st.warning(f"SKU match rate to master: {matched:.1f}%. Unmatched rows still count in totals but carry no attributes.")

k=Q(f"SELECT SUM(subtotal) rev, SUM(qty) units, COUNT(*) txns, COUNT(DISTINCT sku) skus FROM joined WHERE {WHERE}").iloc[0]
c1,c2,c3,c4=st.columns(4)
c1.metric("Revenue",f"{(k.rev or 0):,.0f}")
c2.metric("Units",f"{(k.units or 0):,.0f}")
c3.metric("Transactions",f"{(k.txns or 0):,.0f}")
c4.metric("Active SKUs",f"{(k.skus or 0):,.0f}")
st.divider()

tabs=["📈 Trend","🛒 Channel"]
if CAT: tabs.append("🧩 By Attribute")
tabs+=["🔀 Compare","📦 SKUs","🧮 Pivot"]
T=st.tabs(tabs); idx={name:t for name,t in zip(tabs,T)}

with idx["📈 Trend"]:
    g=st.radio("Granularity",["Daily","Weekly","Monthly"],horizontal=True,index=2,key="g1")
    tr={"Daily":"day","Weekly":"week","Monthly":"month"}[g]
    df=Q(f"SELECT date_trunc('{tr}',date) period,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 1")
    st.plotly_chart(px.area(df,x="period",y="v",title=f"{g} {mlab}").update_layout(height=420,yaxis_title=mlab,xaxis_title=None),use_container_width=True)

with idx["🛒 Channel"]:
    if "marketplaces" in jdf.columns:
        df=Q(f"SELECT date_trunc('month',date) period,marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1,2 ORDER BY 1")
        st.plotly_chart(px.line(df,x="period",y="v",color="marketplaces",markers=True,title=f"Monthly {mlab} by Channel").update_layout(height=420),use_container_width=True)
        sh=Q(f"SELECT marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 2 DESC")
        a,b=st.columns(2)
        a.plotly_chart(px.pie(sh,names="marketplaces",values="v",hole=0.45,title="Channel Share"),use_container_width=True)
        b.plotly_chart(px.bar(sh,x="marketplaces",y="v",text_auto=".2s",title="Channel Totals"),use_container_width=True)

if CAT:
    with idx["🧩 By Attribute"]:
        dim=st.selectbox("Break down by",CAT)
        df=Q(f'SELECT "{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC')
        st.plotly_chart(px.bar(df,x="v",y="k",orientation="h",text_auto=".2s",title=f"{mlab} by {dim}").update_layout(height=max(400,len(df)*26),yaxis=dict(categoryorder="total ascending"),yaxis_title=None,xaxis_title=mlab),use_container_width=True)
        trd=Q(f'SELECT date_trunc(\'month\',date) period,"{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1,2 ORDER BY 1')
        st.plotly_chart(px.line(trd,x="period",y="v",color="k",markers=True,title=f"Monthly {mlab} by {dim}").update_layout(height=420),use_container_width=True)

with idx["🔀 Compare"]:
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

with idx["📦 SKUs"]:
    n=st.slider("Top N SKUs",5,50,15)
    namecol=next((c for c in LABEL if "name" in c.lower()),None)
    sel=f'sku, "{namecol}"' if namecol else "sku"
    df=Q(f'SELECT {sel}, SUM(subtotal) rev, SUM(qty) units FROM joined WHERE {WHERE} GROUP BY {sel} ORDER BY {"rev" if metric=="subtotal" else "units"} DESC LIMIT {n}')
    st.dataframe(df,use_container_width=True)

with idx["🧮 Pivot"]:
    rowopts=([c for c in CAT] if CAT else [])+["marketplaces"]
    rd=st.selectbox("Rows",rowopts)
    df=Q(f'SELECT "{rd}" r, yr||\'-\'||lpad(mon::VARCHAR,2,\'0\') ym,{agg} v FROM joined WHERE {WHERE} AND "{rd}" IS NOT NULL GROUP BY 1,2 ORDER BY 2')
    piv=df.pivot(index="r",columns="ym",values="v").fillna(0)
    st.dataframe(piv.style.format("{:,.0f}"),use_container_width=True)
    st.download_button("Download CSV",piv.to_csv().encode(),"pivot.csv","text/csv")
