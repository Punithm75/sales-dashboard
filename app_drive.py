"""
D2C Sales Dashboard — Streamlit + DuckDB
=========================================
Reads a Parquet file exported from Redshift and serves an interactive
sales dashboard: daily / weekly / monthly sales by channel, with
YoY, MoM and product-to-product comparison. Metric toggle: Qty or Subtotal.

Run locally:
    pip install streamlit duckdb pandas pyarrow plotly
    streamlit run app.py

Data contract (columns expected in sales.parquet):
    marketplaces (str) | date (date) | yr (int) | mon (int)
    product_code (int) | color_code (int) | Qty (int) | Subtotal (float)
"""

import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
DATA_PATH = Path(__file__).parent / "sales.parquet"

# Google Drive file ID of the data file (set in Streamlit "Secrets" when hosted,
# or as an environment variable locally). If present, the app downloads the
# latest copy from Drive on startup; otherwise it falls back to a local file.
DRIVE_FILE_ID = st.secrets.get("DRIVE_FILE_ID", os.environ.get("DRIVE_FILE_ID", ""))

st.set_page_config(page_title="D2C Sales Dashboard", layout="wide",
                   initial_sidebar_state="expanded")


# --------------------------------------------------------------------------
# Fetch the data file from Google Drive using a service account.
# Credentials come from Streamlit Secrets (when hosted) under [gcp_service_account],
# never hard-coded. Runs once per app start; cached.
# --------------------------------------------------------------------------
@st.cache_resource(ttl=3600)  # re-download at most once per hour
def fetch_from_drive() -> str:
    """Download the Drive file to a local temp path and return that path."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io, tempfile

    info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=creds)
    request = service.files().get_media(fileId=DRIVE_FILE_ID)

    out_path = Path(tempfile.gettempdir()) / "sales_from_drive.parquet"
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out_path.as_posix()


def data_source_path() -> str:
    """Use Drive if a file ID is configured, else the local sample file."""
    if DRIVE_FILE_ID:
        return fetch_from_drive()
    return DATA_PATH.as_posix()

# --------------------------------------------------------------------------
# Data access. DuckDB queries the Parquet file in-process — no DB server,
# no Redshift connection, no credentials here. Cached so it loads once.
# --------------------------------------------------------------------------
@st.cache_resource
def get_con():
    con = duckdb.connect(database=":memory:")
    path = data_source_path()
    con.execute(f"""
        CREATE VIEW sales AS
        SELECT * FROM read_parquet('{path}')
    """)
    return con

@st.cache_data
def q(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).df()

@st.cache_data
def bounds():
    r = q("SELECT min(date) lo, max(date) hi FROM sales").iloc[0]
    return pd.to_datetime(r.lo).date(), pd.to_datetime(r.hi).date()

@st.cache_data
def channels():
    return q("SELECT DISTINCT marketplaces FROM sales ORDER BY 1")["marketplaces"].tolist()

@st.cache_data
def products():
    return q("SELECT DISTINCT product_code FROM sales ORDER BY 1")["product_code"].tolist()

# --------------------------------------------------------------------------
# Sidebar — global controls
# --------------------------------------------------------------------------
st.sidebar.title("Controls")

metric = st.sidebar.radio("Metric", ["Subtotal", "Qty"], horizontal=True)
metric_label = "Revenue (Subtotal)" if metric == "Subtotal" else "Units (Qty)"
agg = f"SUM({metric})"

lo, hi = bounds()
date_range = st.sidebar.date_input("Date range", value=(lo, hi), min_value=lo, max_value=hi)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
else:
    start, end = lo, hi

all_ch = channels()
sel_ch = st.sidebar.multiselect("Channels", all_ch, default=all_ch)
if not sel_ch:
    sel_ch = all_ch

ch_filter = "(" + ",".join(f"'{c}'" for c in sel_ch) + ")"
base_where = f"date BETWEEN '{start}' AND '{end}' AND marketplaces IN {ch_filter}"

def fmt(v, m=metric):
    if m == "Subtotal":
        return f"{v:,.0f}"
    return f"{v:,.0f}"

# --------------------------------------------------------------------------
# Header + KPIs
# --------------------------------------------------------------------------
st.title("D2C Sales Dashboard")
st.caption(f"{metric_label} · {start} → {end} · channels: {', '.join(sel_ch)}")

kpi = q(f"""
    SELECT {agg} AS total, SUM(Qty) AS units, SUM(Subtotal) AS rev,
           COUNT(*) AS txns, COUNT(DISTINCT product_code) AS skus
    FROM sales WHERE {base_where}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Revenue", f"{kpi.rev:,.0f}")
c2.metric("Units", f"{kpi.units:,.0f}")
c3.metric("Transactions", f"{kpi.txns:,.0f}")
c4.metric("Active SKUs", f"{kpi.skus:,.0f}")

st.divider()

# --------------------------------------------------------------------------
# Tabs
# --------------------------------------------------------------------------
tab_trend, tab_channel, tab_compare, tab_product, tab_table = st.tabs(
    ["📈 Trend", "🛒 By Channel", "🔀 Compare", "📦 Products", "🧮 Pivot Table"]
)

# ---- Trend: daily / weekly / monthly ----
with tab_trend:
    grain = st.radio("Granularity", ["Daily", "Weekly", "Monthly"],
                     horizontal=True, key="grain")
    trunc = {"Daily": "day", "Weekly": "week", "Monthly": "month"}[grain]
    df = q(f"""
        SELECT date_trunc('{trunc}', date) AS period, {agg} AS "value"
        FROM sales WHERE {base_where}
        GROUP BY 1 ORDER BY 1
    """)
    fig = px.area(df, x="period", y="value", title=f"{grain} {metric_label}")
    fig.update_traces(line_width=2)
    fig.update_layout(height=420, yaxis_title=metric_label, xaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

# ---- By Channel ----
with tab_channel:
    grain2 = st.radio("Granularity", ["Daily", "Weekly", "Monthly"],
                      horizontal=True, key="grain2", index=2)
    trunc2 = {"Daily": "day", "Weekly": "week", "Monthly": "month"}[grain2]
    df = q(f"""
        SELECT date_trunc('{trunc2}', date) AS period, marketplaces, {agg} AS "value"
        FROM sales WHERE {base_where}
        GROUP BY 1, 2 ORDER BY 1
    """)
    fig = px.line(df, x="period", y="value", color="marketplaces",
                  title=f"{grain2} {metric_label} by Channel", markers=True)
    fig.update_layout(height=420, yaxis_title=metric_label, xaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

    share = q(f"""
        SELECT marketplaces, {agg} AS "value"
        FROM sales WHERE {base_where}
        GROUP BY 1 ORDER BY 2 DESC
    """)
    cpie, cbar = st.columns(2)
    cpie.plotly_chart(px.pie(share, names="marketplaces", values="value",
                             title="Channel Share", hole=0.45), use_container_width=True)
    cbar.plotly_chart(px.bar(share, x="marketplaces", y="value",
                             title="Channel Totals", text_auto=".2s"),
                      use_container_width=True)

# ---- Compare: YoY / MoM / Product vs Product ----
with tab_compare:
    mode = st.selectbox("Comparison mode",
                        ["Year over Year (YoY)", "Month over Month (MoM)",
                         "Product vs Product"])

    if mode == "Year over Year (YoY)":
        df = q(f"""
            SELECT mon AS "month", yr AS "year", {agg} AS "value"
            FROM sales WHERE marketplaces IN {ch_filter}
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        df["year"] = df["year"].astype(str)
        fig = px.line(df, x="month", y="value", color="year", markers=True,
                      title=f"YoY {metric_label} by Month")
        fig.update_layout(height=440, xaxis=dict(tickmode="linear"),
                          yaxis_title=metric_label)
        st.plotly_chart(fig, use_container_width=True)

        piv = df.pivot(index="month", columns="year", values="value").fillna(0)
        yrs = sorted(piv.columns)
        if len(yrs) >= 2:
            a, b = yrs[-2], yrs[-1]
            piv["Δ %"] = ((piv[b] - piv[a]) / piv[a].replace(0, pd.NA) * 100).round(1)
        st.dataframe(piv.style.format("{:,.0f}", subset=yrs), use_container_width=True)

    elif mode == "Month over Month (MoM)":
        df = q(f"""
            SELECT date_trunc('month', date) AS period, {agg} AS "value"
            FROM sales WHERE {base_where}
            GROUP BY 1 ORDER BY 1
        """)
        df["MoM %"] = (df["value"].pct_change() * 100).round(1)
        fig = go.Figure()
        fig.add_bar(x=df["period"], y=df["value"], name=metric_label)
        fig.add_trace(go.Scatter(x=df["period"], y=df["MoM %"], name="MoM %",
                                 yaxis="y2", mode="lines+markers"))
        fig.update_layout(height=440, title=f"MoM {metric_label}",
                          yaxis=dict(title=metric_label),
                          yaxis2=dict(title="MoM %", overlaying="y", side="right"))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True)

    else:  # Product vs Product
        prods = products()
        picks = st.multiselect("Pick products to compare", prods,
                               default=prods[:3], max_selections=8)
        if picks:
            plist = "(" + ",".join(str(p) for p in picks) + ")"
            grainp = st.radio("Granularity", ["Weekly", "Monthly"],
                              horizontal=True, key="grainp", index=1)
            truncp = {"Weekly": "week", "Monthly": "month"}[grainp]
            df = q(f"""
                SELECT date_trunc('{truncp}', date) AS period,
                       product_code, {agg} AS "value"
                FROM sales
                WHERE {base_where} AND product_code IN {plist}
                GROUP BY 1, 2 ORDER BY 1
            """)
            df["product_code"] = df["product_code"].astype(str)
            fig = px.line(df, x="period", y="value", color="product_code",
                          markers=True, title=f"Product comparison · {metric_label}")
            fig.update_layout(height=440, yaxis_title=metric_label)
            st.plotly_chart(fig, use_container_width=True)

# ---- Products: top performers ----
with tab_product:
    topn = st.slider("Top N products", 5, 50, 15)
    df = q(f"""
        SELECT product_code, {agg} AS "value", SUM(Qty) AS units, SUM(Subtotal) AS rev
        FROM sales WHERE {base_where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT {topn}
    """)
    df["product_code"] = df["product_code"].astype(str)
    fig = px.bar(df, x="value", y="product_code", orientation="h",
                 title=f"Top {topn} Products by {metric_label}", text_auto=".2s")
    fig.update_layout(height=max(420, topn * 26),
                      yaxis=dict(categoryorder="total ascending"))
    st.plotly_chart(fig, use_container_width=True)

# ---- Pivot Table ----
with tab_table:
    st.caption("Channel × Month pivot — the Sheets replacement, but at scale.")
    df = q(f"""
        SELECT marketplaces,
               yr || '-' || lpad(mon::VARCHAR, 2, '0') AS ym,
               {agg} AS "value"
        FROM sales WHERE {base_where}
        GROUP BY 1, 2 ORDER BY 2
    """)
    piv = df.pivot(index="marketplaces", columns="ym", values="value").fillna(0)
    st.dataframe(piv.style.format("{:,.0f}"), use_container_width=True)
    st.download_button("Download CSV", piv.to_csv().encode(),
                       "pivot.csv", "text/csv")
