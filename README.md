# D2C Sales Dashboard (Streamlit + DuckDB)

A self-serve sales dashboard that handles millions of rows — the replacement
for Google Sheets when your sales data outgrows it.

## What it does
- **Trend**: daily / weekly / monthly sales
- **By Channel**: Shopify / MP / Internal / Cocoblue split, share, totals
- **Compare**: Year-over-Year, Month-over-Month, and Product-vs-Product
- **Products**: top performers
- **Pivot Table**: channel × month, CSV export
- **Metric toggle**: switch the whole dashboard between Revenue (Subtotal) and Units (Qty)
- Date range + channel filters apply everywhere

## Architecture (and why it's secure)
```
Redshift ──(export_from_redshift.py, scheduled)──> sales.parquet ──> DuckDB (in-process) ──> Streamlit app ──> company URL
```
- The **export script** is the only thing that connects to Redshift. Credentials
  live there (env vars / secrets manager), never in the app.
- **DuckDB** only reads the local Parquet file. It has no Redshift connection.
- **Streamlit** is the only user-facing surface — put it behind your SSO / internal
  network for company-wide access.
- The export query pulls only the columns the dashboard needs (data minimization).

## Quick start (with the included sample data)
```bash
pip install streamlit duckdb pandas pyarrow plotly
streamlit run app.py
```
`sales.parquet` included here is **synthetic sample data** (2 years, ~73K rows) in
your exact column format so you can see it working immediately.

## Going live with your data
1. Fill in Redshift credentials as environment variables (see export script).
2. Confirm the `inv_raw` column names match (adjust the SELECT if needed).
3. Run `python export_from_redshift.py` to overwrite `sales.parquet`.
4. Schedule that script daily/weekly (cron, GitHub Action, Airflow).
5. Restart / redeploy the app — it picks up the new Parquet automatically.

## Data contract (columns)
`marketplaces, date, yr, mon, product_code, color_code, Qty, Subtotal`

> `color_code` is kept in the data but the current views aggregate at the
> product level, as requested. A color-level drill-down can be added later.

## Files
- `app.py` — the dashboard
- `export_from_redshift.py` — scheduled Redshift → Parquet export
- `generate_sample.py` — generates the sample Parquet (for demo only)
- `sales.parquet` — sample data
