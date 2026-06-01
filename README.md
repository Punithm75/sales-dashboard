# D2C Sales Dashboard v3 (fully dynamic)

**You never edit the app.** It reads your sales data + master SKU Google Sheet,
joins them on SKU, and AUTO-BUILDS the filters from whatever columns the sheet has.
Add/rename/remove a column in the sheet -> filters update on next refresh. No code change.

## Automatic behaviour
- Joins sales `sku` <-> master `sku_code` (as text).
- Detects filters (Collection, Category, UseCase, Gender, Price Bracket, tags...),
  range sliders (ASP, MRP, COGS), and labels (name, color). Code/ID columns ignored.
- Shows a SKU match-rate warning if some sales rows don't match the master.
- Tabs: Trend, Channel, By Attribute, Compare (YoY/MoM), SKUs, Pivot.

## Connect real sources (one-time)
1. Enable Google Sheets API in your Cloud project (separate from Drive API).
2. Share BOTH with the service account email (...iam.gserviceaccount.com, Viewer):
   the daily sales file in Drive, and the "MASTER SKU" Google Sheet.
3. In secrets (local AND cloud), above [gcp_service_account]:
   APP_PASSWORD     = "your-password"
   DRIVE_FILE_ID    = "sales_file_id"
   MASTER_SHEET_ID  = "1gVj6Ufd9JKQTMOYLvNpfqQVK_Dpznotya_je_F2vtO0"
   MASTER_SHEET_TAB = "Master SKU"
4. requirements.txt now includes gspread - push it so the cloud installs it.

## Deploy (same flow as before)
Replace your app file with app_v3.py (or set it as the main file path), push via
GitHub Desktop, add the 2 new MASTER_* secret lines in the cloud, it rebuilds itself.

## Offline dev
Without MASTER_SHEET_ID it uses local master.csv sample; without DRIVE_FILE_ID it
uses local sales.parquet. Both gitignored.
