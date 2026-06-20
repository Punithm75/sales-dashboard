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

LOGO_B64 = "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"

st.set_page_config(page_title="D2C Sales Dashboard", layout="wide",
                   initial_sidebar_state="expanded", page_icon="📊")

# ---------- Theme / styling ----------
st.markdown('''
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600;9..144,700&family=Outfit:wght@300;400;500;600;700;800&display=swap');

:root{
  --bg:#0e0e10; --panel:#16161a; --panel2:#1d1d22; --line:#2a2a30;
  --ink:#f4f4f5; --muted:#9b9ba6;
  --lilac:#9b87f5; --sand:#e8c468; --rose:#e87a90; --teal:#5bbfb0;
}
html, body, [class*="css"], .stApp{ font-family:'Outfit',sans-serif; }

/* Refined single-accent atmosphere */
.stApp{
  background:
    radial-gradient(1000px 600px at 15% -10%, rgba(155,135,245,.12) 0%, transparent 55%),
    radial-gradient(700px 500px at 100% 0%, rgba(155,135,245,.05) 0%, transparent 50%),
    #0e0e10;
  color:var(--ink);
}

/* Title — gradient wordmark */
h1{ font-family:'Fraunces',serif !important; font-weight:700 !important;
    letter-spacing:-1px; font-size:2.7rem !important; margin-top:.1rem !important;
    background:linear-gradient(100deg,#fff 0%,#c9bdff 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
h2,h3{ font-family:'Outfit',sans-serif !important; font-weight:600 !important; color:var(--ink); }

/* Sidebar */
section[data-testid="stSidebar"]{
  background:linear-gradient(180deg,#11141f 0%,#0d1019 100%);
  border-right:1px solid var(--line); }
section[data-testid="stSidebar"] h1{ font-size:1.25rem !important;
  background:linear-gradient(90deg,#fff,#c9bdff); -webkit-background-clip:text;
  -webkit-text-fill-color:transparent; }
section[data-testid="stSidebar"] .stMarkdown strong{ color:var(--lilac); }

/* KPI cards — glass panels with a colored top accent; each of the 4 gets its own hue */
div[data-testid="stMetric"]{
  position:relative; overflow:hidden;
  background:linear-gradient(160deg,rgba(40,46,72,.6) 0%,rgba(22,26,40,.9) 100%);
  border:1px solid var(--line); border-radius:18px; padding:20px 22px 18px;
  box-shadow:0 8px 28px rgba(0,0,0,.35); backdrop-filter:blur(6px);
  transition:transform .18s ease, box-shadow .18s ease, border-color .18s; }
div[data-testid="stMetric"]:hover{ transform:translateY(-4px);
  box-shadow:0 14px 40px rgba(0,0,0,.5); }
div[data-testid="stMetric"]::before{ content:""; position:absolute; top:0; left:0; right:0; height:3px; background:linear-gradient(90deg,#9b87f5,#5bbfb0); }

div[data-testid="stMetricLabel"]{ color:var(--muted) !important; font-size:.78rem !important;
  text-transform:uppercase; letter-spacing:1.4px; font-weight:600; }
div[data-testid="stMetricValue"]{ font-family:'Fraunces',serif !important;
  font-weight:600 !important; font-size:2rem !important; color:#f4f4f5 !important; }

/* Tabs */
button[data-baseweb="tab"]{ font-weight:600; color:var(--muted); font-size:.95rem; }
button[data-baseweb="tab"][aria-selected="true"]{ color:var(--lilac) !important; }
div[data-baseweb="tab-highlight"]{ background:linear-gradient(90deg,#9b87f5,#5bbfb0) !important; height:3px; }

/* Inputs / tags */
.stMultiSelect div[data-baseweb="select"]>div, .stDateInput input{
  background:var(--panel2); border-color:var(--line); border-radius:11px; }
div[data-baseweb="tag"]{ background:#9b87f5 !important; border-radius:8px; border:none; }
.stRadio [aria-checked="true"]{ }

/* Caption + divider */
.stCaption, div[data-testid="stCaptionContainer"]{ color:var(--muted) !important; }
hr{ border-color:var(--line); }

/* Dataframe subtle polish */
div[data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; border:1px solid var(--line); }
</style>
''', unsafe_allow_html=True)

# Plotly theme applied per-chart via a helper
PLOT_BG="rgba(0,0,0,0)"
PALETTE=["#9b87f5","#e8c468","#e87a90","#5bbfb0"]
def style_fig(fig):
    fig.update_layout(paper_bgcolor=PLOT_BG, plot_bgcolor=PLOT_BG,
                      font=dict(color="#c9c9d2", family="Outfit", size=13),
                      colorway=PALETTE, margin=dict(t=54,l=10,r=10,b=10),
                      legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=12)),
                      title=dict(font=dict(family="Fraunces", size=18, color="#eef0f7")),
                      hoverlabel=dict(bgcolor="#1d1d22", bordercolor="#2a2a30",
                                      font=dict(family="Outfit", color="#eef0f7")))
    fig.update_xaxes(gridcolor="rgba(255,255,255,.05)", zerolinecolor="rgba(255,255,255,.08)",
                     linecolor="rgba(255,255,255,.12)")
    fig.update_yaxes(gridcolor="rgba(255,255,255,.05)", zerolinecolor="rgba(255,255,255,.08)",
                     linecolor="rgba(255,255,255,.12)")
    # make area/line fills richer
    fig.update_traces(selector=dict(type="scatter"), line=dict(width=2.5))
    return fig

def plot(fig, **kw):
    import streamlit as _st
    return _st.plotly_chart(style_fig(fig), **kw)

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
          "product code planning","sku_code","key","accounting sku","po product name"}
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
def build_everything():
    """ONE source of truth: open connection, build sales view, join master, create joined view.
    Returns (con, CAT, NUM, LABEL, MATCH, HAS_REF). Cached as a resource so the connection
    and all its views are created together and stay in sync."""
    path=sales_file_path()
    con=duckdb.connect(":memory:")
    if path.endswith(".csv"):
        con.execute(f"CREATE VIEW sales_raw AS SELECT * FROM read_csv_auto('{path}')")
    else:
        con.execute(f"CREATE VIEW sales_raw AS SELECT * FROM read_parquet('{path}')")
    cols=[r[0] for r in con.execute("DESCRIBE sales_raw").fetchall()]
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
    ref=pick("reference_code","invoice_id","billing_id","order_id","invoice","bill_id")
    sel=[]
    sel.append(f"CAST({sku} AS VARCHAR) AS sku" if sku else "'' AS sku")
    sel.append(f"{rev} AS subtotal" if rev else "0.0 AS subtotal")
    sel.append(f"{qty} AS qty" if qty else "0 AS qty")
    sel.append(f"{chan} AS marketplaces" if chan else "'Unknown' AS marketplaces")
    sel.append(f"CAST({dat} AS DATE) AS date" if dat else "CURRENT_DATE AS date")
    sel.append(f"CAST({ref} AS VARCHAR) AS reference_code" if ref else "CAST(NULL AS VARCHAR) AS reference_code")
    con.execute(f"CREATE VIEW sales AS SELECT {', '.join(sel)}, "
                f"EXTRACT(month FROM CAST({dat} AS DATE)) AS mon, "
                f"EXTRACT(year FROM CAST({dat} AS DATE)) AS yr FROM sales_raw")
    has_ref = ref is not None

    master=load_master()
    cat,num,label=classify(master)
    if master.empty:
        con.execute("CREATE VIEW joined AS SELECT *, FALSE AS _matched FROM sales")
        return con,[],[],[],0.0,has_ref
    keep=["sku_code"]+cat+num+label
    keep=[c for c in keep if c in master.columns]
    m=master[keep].drop_duplicates("sku_code").copy()
    for c in num:
        if c in m.columns: m[c]=pd.to_numeric(m[c],errors="coerce")
    con.register("master_df", m)
    con.execute("CREATE VIEW joined AS "
                "SELECT s.*, m.* EXCLUDE(sku_code), (m.sku_code IS NOT NULL) AS _matched "
                "FROM sales s LEFT JOIN master_df m ON s.sku = m.sku_code")
    mr=con.execute("SELECT AVG(CASE WHEN _matched THEN 1.0 ELSE 0.0 END)*100 FROM joined").fetchone()[0]
    return con,cat,num,label,(mr or 0.0),has_ref

try:
    con,CAT,NUM,LABEL,MATCH,_HAS_REF = build_everything()
    # Promote ONLY the exact columns we want as filters: product_name and Color.
    # Exact (case-insensitive) match only — avoids pulling in "PO Product Name",
    # "OLD COLOUR NAME", "Color Category", etc.
    def _promote_exact(*targets):
        for t in targets:
            for c in list(LABEL):              # only promote from label cols
                if c.lower().strip()==t and c not in CAT:
                    CAT.insert(0, c); return c
        return None
    _promote_exact("product_name","product name")
    _promote_exact("color","colour")
except Exception:
    st.title("Data load error"); st.error("Failed while preparing data:")
    st.code(traceback.format_exc())
    st.info("Check: Drive file shared with service account & is Parquet/CSV; master sheet shared.")
    st.stop()

def Q(sql): return con.execute(sql).df()

# ---------- AI assistant (Gemini text-to-SQL, optional) ----------
# Natural language -> DuckDB SQL over the `joined` view. We send the model only the
# SCHEMA (column names/types) + the question -- never the data rows -- run the SQL
# locally, then (best-effort) ask the model to phrase the answer. Stays dynamic: the
# schema is read live from `joined`, so new master columns are usable with zero code change.
import re
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY","")
GEMINI_MODEL   = st.secrets.get("GEMINI_MODEL","gemini-2.0-flash")
AI_ROW_CAP     = 5000   # hard cap on rows an AI query may materialise (memory safety)

_SQL_SYS = (
 "You are a senior analytics engineer. Translate the user's question into ONE DuckDB SQL "
 "query over a single view named \"joined\". Return ONLY the SQL -- no prose, no markdown.\n\n"
 "HARD RULES:\n"
 "- Read-only: a single SELECT (optionally a leading WITH ...). Never use "
 "INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/ATTACH/COPY/PRAGMA or any file-reading function.\n"
 "- Query ONLY the view \"joined\". Use ONLY the columns listed below -- never invent columns.\n"
 "- DuckDB dialect.\n"
 "- Use these exact metric definitions so results match the dashboard:\n"
 "    revenue / subtotal = SUM(subtotal)      -- amounts are Indian Rupees (INR)\n"
 "    units              = SUM(qty)\n"
 "    orders             = COUNT(DISTINCT reference_code)   -- distinct invoices\n"
 "- \"marketplaces\" is the sales channel. \"date\" is a DATE; \"mon\"/\"yr\" are its month/year.\n"
 "- Alias every aggregate with a clear, human-readable name.\n"
 "- For 'top N' use ORDER BY <measure> DESC LIMIT N. Keep result sets small.\n"
 "- If the question is ambiguous, choose a sensible interpretation.\n\n"
 "Columns of \"joined\" (name : type):\n{schema_lines}\n\nData spans {DMIN} to {DMAX}."
)
_ANS_SYS = (
 "You are a concise analyst for an Indian D2C activewear brand. Given the user's question and the "
 "query result as CSV, answer in 1-3 sentences of plain English. Show money with the rupee symbol "
 "(₹) and thousands separators; show counts with thousands separators. Do not output SQL or "
 "tables. If the result is empty, say that no data matched."
)

@st.cache_data(ttl=3600)
def ai_schema():
    try:
        return [(r[0],r[1]) for r in con.execute("DESCRIBE joined").fetchall()]
    except Exception:
        return []

def _gemini(prompt, system=None, temperature=0.1, timeout=45):
    import requests
    url=f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    body={"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"temperature":temperature}}
    if system: body["systemInstruction"]={"parts":[{"text":system}]}
    r=requests.post(url,headers={"x-goog-api-key":GEMINI_API_KEY,"Content-Type":"application/json"},
                    json=body,timeout=timeout)
    if r.status_code!=200:
        raise RuntimeError(f"Gemini API {r.status_code}: {r.text[:200]}")
    cands=r.json().get("candidates",[])
    if not cands: raise RuntimeError(str(r.json().get("promptFeedback") or "No response from model"))
    return "".join(p.get("text","") for p in cands[0].get("content",{}).get("parts",[])).strip()

def _extract_sql(text):
    m=re.search(r"```(?:sql)?\s*(.*?)```",text,re.S|re.I)
    return (m.group(1) if m else text).strip().rstrip(";").strip()

_AI_FORBIDDEN=re.compile(
 r"\b(attach|detach|copy|insert|update|delete|drop|create|alter|pragma|install|load|export|import|"
 r"call|truncate|grant|revoke|vacuum|checkpoint|read_csv|read_csv_auto|read_parquet|read_json|"
 r"read_json_auto|read_text|read_blob|parquet_scan|csv_scan|glob|sniff_csv)\b",re.I)
def _safe_select(sql):
    low=sql.lower().lstrip()
    if not (low.startswith("select") or low.startswith("with")): return False  # SELECT/WITH only
    if ";" in sql.strip().rstrip(";"): return False                            # no stacked statements
    if "joined" not in low: return False                                       # must query our view
    if _AI_FORBIDDEN.search(sql): return False                                 # no DDL/DML/file access
    return True

def _fmt(v):
    try:
        f=float(v); return f"{f:,.0f}" if f==int(f) else f"{f:,.2f}"
    except Exception:
        return v

def _auto_chart(df):
    if df is None or df.empty or df.shape[1]<2 or len(df)>200: return None
    num=[c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not num: return None
    y=num[-1]; dims=[c for c in df.columns if c!=y]
    if not dims: return None
    x=dims[0]
    if pd.api.types.is_datetime64_any_dtype(df[x]): return px.line(df,x=x,y=y,markers=True)
    if len(df)<=30: return px.bar(df.sort_values(y,ascending=False),x=x,y=y,text_auto=".2s")
    return None

def _render_result(df):
    if df is None: return
    if getattr(df,"empty",False): st.info("No rows matched that question."); return
    if df.shape==(1,1) and pd.api.types.is_numeric_dtype(df.iloc[:,0]):
        st.metric(str(df.columns[0]),_fmt(df.iloc[0,0])); return
    try:
        num=[c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        st.dataframe(df.style.format({c:_fmt for c in num}) if num else df,width='stretch')
    except Exception:
        st.dataframe(df,width='stretch')
    try:
        fig=_auto_chart(df)
        if fig is not None: plot(fig,width='stretch')
    except Exception:
        pass

# bounds for date picker (cheap min/max query)
try:
    b=Q("SELECT min(date) lo, max(date) hi FROM sales").iloc[0]
    DMIN=pd.to_datetime(b.lo).date(); DMAX=pd.to_datetime(b.hi).date()
except Exception:
    import datetime as _dt; DMIN=_dt.date(2024,1,1); DMAX=_dt.date.today()

# ---------- sidebar ----------
st.sidebar.title("Controls")
metric=st.sidebar.radio("Metric",["subtotal","qty","orders"],horizontal=True,
                        format_func=lambda x:{"subtotal":"Subtotal","qty":"Units","orders":"Orders"}[x])
_mlabels={"subtotal":"Subtotal","qty":"Units","orders":"Orders"}
mlab=_mlabels[metric]
# Orders = distinct invoices (reference_code); others are sums
if metric=="orders":
    agg=("COUNT(DISTINCT reference_code)" if _HAS_REF else "COUNT(*)")
else:
    agg=f"SUM({metric})"
dr=st.sidebar.date_input("Date range", value=(DMIN,DMAX), min_value=DMIN, max_value=DMAX)
start,end=(dr if isinstance(dr,tuple) and len(dr)==2 else (DMIN,DMAX))

def distinct(col):
    try:
        return [str(r[0]) for r in con.execute(f'SELECT DISTINCT "{col}" FROM joined WHERE "{col}" IS NOT NULL ORDER BY 1').fetchall() if str(r[0])!=""]
    except Exception:
        return []

selected={}
_all_ch=distinct("marketplaces")
_default_ch=[c for c in _all_ch if c.lower().strip() not in ("internal","retail")]
ch=st.sidebar.multiselect("Channel", _all_ch, default=_default_ch)
# If user clears all, treat as "all selected" so the dashboard isn't empty
if ch: selected["marketplaces"]=ch
elif _all_ch and not ch: selected["marketplaces"]=_default_ch
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
# ---- Logo header ----
st.markdown(
    f'<div style="margin:0 0 6px 0;">'
    f'<img src="data:image/png;base64,{LOGO_B64}" style="height:46px;"/>'
    f'</div>',
    unsafe_allow_html=True)
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
    txn_expr="COUNT(DISTINCT reference_code)" if _HAS_REF else "COUNT(*)"
    k=Q(f"SELECT SUM(subtotal) rev, SUM(qty) units, {txn_expr} txns, COUNT(DISTINCT sku) skus FROM joined WHERE {WHERE}").iloc[0]
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Subtotal",f"{(k.rev or 0):,.0f}"); c2.metric("Units",f"{(k.units or 0):,.0f}")
    c3.metric("Orders",f"{(k.txns or 0):,.0f}"); c4.metric("Active SKUs",f"{(k.skus or 0):,.0f}")
except Exception:
    st.error("Could not compute KPIs."); st.code(traceback.format_exc())
st.divider()

tabs=["📈 Trend","🛒 Channel"]+(["🧩 By Attribute"] if CAT else [])+["🔀 Compare","📦 SKUs","🧮 Pivot","🤖 Ask AI"]
T=dict(zip(tabs, st.tabs(tabs)))

with T["📈 Trend"]:
    g=st.radio("Granularity",["Daily","Weekly","Monthly"],horizontal=True,index=2,key="g")
    tr={"Daily":"day","Weekly":"week","Monthly":"month"}[g]
    df=Q(f"SELECT date_trunc('{tr}',date) period,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 1")
    plot(px.area(df,x="period",y="v",title=f"{g} {mlab}").update_traces(line_color="#9b87f5", fillcolor="rgba(155,135,245,0.22)").update_layout(height=420,yaxis_title=mlab,xaxis_title=None),width='stretch')

with T["🛒 Channel"]:
    gc=st.radio("Granularity",["Daily","Weekly","Monthly"],horizontal=True,index=2,key="gchan")
    trc={"Daily":"day","Weekly":"week","Monthly":"month"}[gc]
    df=Q(f"SELECT date_trunc('{trc}',date) period,marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1,2 ORDER BY 1")
    plot(px.line(df,x="period",y="v",color="marketplaces",markers=True,title=f"{gc} {mlab} by Channel").update_layout(height=420),width='stretch')
    sh=Q(f"SELECT marketplaces,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 2 DESC")
    a,b=st.columns(2)
    a.plotly_chart(px.pie(sh,names="marketplaces",values="v",hole=0.45,title="Channel Share"),width='stretch')
    b.plotly_chart(px.bar(sh,x="marketplaces",y="v",text_auto=".2s",title="Channel Totals"),width='stretch')

if CAT:
    with T["🧩 By Attribute"]:
        dim=st.selectbox("Break down by",CAT)
        df=Q(f'SELECT "{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC')
        plot(px.bar(df,x="v",y="k",orientation="h",text_auto=".2s",title=f"{mlab} by {dim}").update_layout(height=max(400,len(df)*26),yaxis=dict(categoryorder="total ascending"),yaxis_title=None,xaxis_title=mlab),width='stretch')
        trd=Q(f'SELECT date_trunc(\'month\',date) period,"{dim}" k,{agg} v FROM joined WHERE {WHERE} AND "{dim}" IS NOT NULL GROUP BY 1,2 ORDER BY 1')
        plot(px.line(trd,x="period",y="v",color="k",markers=True,title=f"Monthly {mlab} by {dim}").update_layout(height=420),width='stretch')

with T["🔀 Compare"]:
    mode=st.selectbox("Comparison",["Year over Year","Month over Month"])
    if mode=="Year over Year":
        df=Q(f'SELECT mon "month",yr "year",{agg} v FROM joined WHERE {WHERE} GROUP BY 1,2 ORDER BY 1,2'); df["year"]=df["year"].astype(str)
        plot(px.line(df,x="month",y="v",color="year",markers=True,title=f"YoY {mlab}").update_layout(height=440),width='stretch')
    else:
        df=Q(f"SELECT date_trunc('month',date) period,{agg} v FROM joined WHERE {WHERE} GROUP BY 1 ORDER BY 1"); df["MoM %"]=(df["v"].pct_change()*100).round(1)
        fig=go.Figure(); fig.add_bar(x=df["period"],y=df["v"],name=mlab)
        fig.add_trace(go.Scatter(x=df["period"],y=df["MoM %"],name="MoM %",yaxis="y2",mode="lines+markers"))
        fig.update_layout(height=440,yaxis=dict(title=mlab),yaxis2=dict(title="MoM %",overlaying="y",side="right"))
        plot(fig,width='stretch')

with T["📦 SKUs"]:
    n=st.slider("Top N SKUs",5,50,15)
    namecol=next((c for c in LABEL if "name" in c.lower()),None)
    sel=f'sku, "{namecol}"' if namecol else "sku"
    ord_expr=("COUNT(DISTINCT reference_code)" if _HAS_REF else "COUNT(*)")
    _orderby={"subtotal":"rev","qty":"units","orders":"orders"}[metric]
    df=Q(f'SELECT {sel}, SUM(subtotal) rev, SUM(qty) units, {ord_expr} orders FROM joined WHERE {WHERE} GROUP BY {sel} ORDER BY {_orderby} DESC LIMIT {n}')
    st.dataframe(df,width='stretch')

with T["🧮 Pivot"]:
    # Columns dropdown: time options + only LOW-cardinality dims (so we never try to
    # render thousands of columns, which breaks the table). High-cardinality fields
    # like product_name / Color are fine for ROWS but not for COLUMNS.
    def _card(col):
        try: return con.execute(f'SELECT COUNT(DISTINCT "{col}") FROM joined').fetchone()[0]
        except Exception: return 9999
    dim_opts=(CAT if CAT else [])+["marketplaces"]
    low_card_dims=[c for c in dim_opts if _card(c)<=30]
    col_opts=["Month (YYYY-MM)","Year","Quarter"]+low_card_dims
    cpa,cpb=st.columns(2)
    rd=cpa.selectbox("Rows",dim_opts,key="piv_rows")
    cd=cpb.selectbox("Columns",col_opts,key="piv_cols")
    if cd=="Month (YYYY-MM)":
        col_sql="yr||'-'||lpad(mon::VARCHAR,2,'0')"; order_sql="2"
    elif cd=="Year":
        col_sql="CAST(yr AS VARCHAR)"; order_sql="2"
    elif cd=="Quarter":
        col_sql="yr||'-Q'||CAST(CEIL(mon/3.0) AS INT)"; order_sql="2"
    else:
        col_sql=f'"{cd}"'; order_sql="1"
    extra_where=f' AND {col_sql} IS NOT NULL' if cd in low_card_dims else ''
    df=Q(f'SELECT "{rd}" r, {col_sql} c, {agg} v FROM joined WHERE {WHERE} AND "{rd}" IS NOT NULL{extra_where} GROUP BY 1,2 ORDER BY {order_sql}')
    if df.empty:
        st.info("No data for this selection.")
    else:
        piv=df.pivot(index="r",columns="c",values="v").fillna(0)
        # add a row total and sort rows by it (most relevant rows first)
        piv["Total"]=piv.sum(axis=1)
        piv=piv.sort_values("Total",ascending=False)
        st.caption(f"{len(piv):,} rows · {len(piv.columns)-1} columns")
        st.dataframe(piv.style.format("{:,.0f}"),width='stretch',height=480)
        st.download_button("Download CSV",piv.to_csv().encode(),"pivot.csv","text/csv")

with T["🤖 Ask AI"]:
    st.subheader("🤖 Ask AI")
    if not GEMINI_API_KEY:
        st.info(
            "**Assistant is off.** Add a Gemini API key to switch it on. In `.streamlit/secrets.toml`:\n\n"
            "```toml\nGEMINI_API_KEY = \"your-key-here\"\n"
            "# optional, defaults to gemini-2.0-flash\nGEMINI_MODEL = \"gemini-2.0-flash\"\n```\n\n"
            "The key is read from `st.secrets` (gitignored) — never paste it into the code. Rerun after saving.")
    else:
        st.caption("Ask in plain English — questions are translated to SQL and run on your live data. "
                   "Aggregates use the dashboard's definitions (Subtotal · Units · Orders). "
                   "Note: the sidebar filters are not applied here — mention the period/segment in your question.")
        if "ai_msgs" not in st.session_state: st.session_state["ai_msgs"]=[]

        for turn in st.session_state["ai_msgs"]:
            with st.chat_message("user"): st.markdown(turn["q"])
            with st.chat_message("assistant"):
                if turn.get("error"): st.error(turn["error"])
                else:
                    if turn.get("answer"): st.markdown(turn["answer"])
                    _render_result(turn.get("df"))
                if turn.get("sql"):
                    with st.expander("Show SQL"): st.code(turn["sql"],language="sql")

        pending=None
        examples=["Top 10 SKUs by revenue","Monthly revenue trend in 2025",
                  "Revenue share by channel","Total units sold"]
        ecols=st.columns(len(examples))
        for i,e in enumerate(examples):
            if ecols[i].button(e,key=f"ai_ex_{i}"): pending=e
        with st.form("ai_form",clear_on_submit=True):
            qin=st.text_input("Your question",placeholder="e.g. Which colour sold the most units last month?")
            if st.form_submit_button("Ask") and qin.strip(): pending=qin.strip()

        if pending:
            turn={"q":pending}
            with st.spinner("Thinking…"):
                try:
                    sl="\n".join(f"  {n} : {t}" for n,t in ai_schema())
                    sql=_extract_sql(_gemini(pending,system=_SQL_SYS.format(schema_lines=sl,DMIN=DMIN,DMAX=DMAX)))
                    turn["sql"]=sql
                    if not _safe_select(sql):
                        turn["error"]="That request didn't produce a safe read-only SELECT. Try rephrasing."
                    else:
                        turn["df"]=Q(f"SELECT * FROM ({sql}) AS _ai LIMIT {AI_ROW_CAP}")
                        try:
                            turn["answer"]=_gemini(
                                f"Question: {pending}\n\nResult (CSV):\n{turn['df'].head(50).to_csv(index=False)}",
                                system=_ANS_SYS)
                        except Exception:
                            turn["answer"]=None
                except Exception as e:
                    turn["error"]=f"AI request failed: {e}"
            st.session_state["ai_msgs"].append(turn)
            st.rerun()
