"""
export_from_redshift.py
========================
Scheduled job: pulls the sales data from Redshift and writes sales.parquet
for the dashboard to read. Run this on a cron / GitHub Action / Airflow
schedule (daily or weekly).

SECURITY NOTES
--------------
* This is the ONLY component that touches Redshift. The dashboard app never
  connects to the database — it only reads the Parquet file this script writes.
* Credentials come from environment variables / a secrets manager. NEVER hard-code
  them and never put them in the app layer.
* Pull ONLY the columns and grain the dashboard needs (data minimization). The
  query below already excludes anything not used by the dashboard.

Setup:
    pip install redshift-connector pandas pyarrow
    export REDSHIFT_HOST=...      REDSHIFT_PORT=5439
    export REDSHIFT_DB=...        REDSHIFT_USER=...
    export REDSHIFT_PASSWORD=...  (or use IAM / secrets manager)

Run:
    python export_from_redshift.py
"""

import os
import redshift_connector  # pip install redshift-connector

OUT_PATH = "sales.parquet"

# Only the columns the dashboard uses. Add a WHERE clause to limit the window
# (e.g. last 24 months) if the table is very large.
QUERY = """
    SELECT
        marketplaces,
        date,
        EXTRACT(year  FROM date) AS yr,
        EXTRACT(month FROM date) AS mon,
        product_code,
        color_code,
        Qty,
        Subtotal
    FROM inv_raw
    WHERE date >= DATEADD(month, -24, CURRENT_DATE)
"""

def main():
    conn = redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ.get("REDSHIFT_PORT", 5439)),
        database=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        ssl=True,  # encrypt data in transit
    )
    try:
        df = conn.cursor().execute(QUERY).fetch_dataframe()
    finally:
        conn.close()

    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df):,} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()
