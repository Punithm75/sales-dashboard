# Deploy to Google Cloud Run

The app stays exactly as it is — it still reads the Drive folder + master sheet via the
service account. Cloud Run just gives you a box where **you choose the RAM**, and it
**scales to zero** when idle (so it's cheap). Run these from the repo folder.

## Prerequisites (one time)
1. Install the gcloud CLI: https://cloud.google.com/sdk/docs/install
2. Log in and pick your project (use the same GCP project your service account lives in):
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```
3. Turn on the APIs we need:
   ```bash
   gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
     artifactregistry.googleapis.com secretmanager.googleapis.com
   ```

## Step 1 — Move your secrets into Secret Manager
Your secrets are already in TOML form on Streamlit Cloud.
1. Streamlit Cloud → your app → **Settings → Secrets** → copy **all** the text.
2. Save it locally as `secrets.toml` (in this folder).
3. Upload it, then delete the local copy:
   ```bash
   gcloud secrets create streamlit-secrets --data-file=secrets.toml
   rm secrets.toml
   ```
   *(To update secrets later: `gcloud secrets versions add streamlit-secrets --data-file=secrets.toml`.)*
4. Let Cloud Run read it (grants the default runtime service account access):
   ```bash
   PROJ=$(gcloud config get-value project)
   NUM=$(gcloud projects describe "$PROJ" --format='value(projectNumber)')
   gcloud secrets add-iam-policy-binding streamlit-secrets \
     --member="serviceAccount:${NUM}-compute@developer.gserviceaccount.com" \
     --role="roles/secretmanager.secretAccessor"
   ```

## Step 2 — Deploy
```bash
gcloud run deploy sales-dashboard \
  --source . \
  --region asia-south1 \
  --memory 2Gi --cpu 1 \
  --timeout 600 \
  --session-affinity \
  --allow-unauthenticated \
  --update-secrets=/root/.streamlit/secrets.toml=streamlit-secrets:latest
```
- It builds the container (from the `Dockerfile`), deploys it, and prints a **service URL**.
- `--session-affinity` keeps each user pinned to one instance (required for Streamlit's live connection).
- `--allow-unauthenticated` makes the URL reachable; your **app password gate** still protects it (same as today). Remove this flag later if you want to require Google sign-in instead.
- Secrets are mounted as `~/.streamlit/secrets.toml`, so `st.secrets` works unchanged.

## Step 3 — Verify
Open the printed URL → enter the app password → confirm the dashboard loads and shows the Retail channel.

## Notes & knobs
- **RAM:** start at `--memory 2Gi`. If it ever OOMs (the data downloads into in-memory `/tmp` on Cloud Run), bump to `--memory 4Gi` (keep `--cpu 1`) and redeploy. Beyond 4Gi, add `--cpu 2`.
- **Cold start:** with scale-to-zero, the first visit after idle waits ~15–40s while it downloads the Drive files. To keep it always warm, add `--min-instances 1` (small always-on cost).
- **New data / code:** uploading a new parquet to the Drive folder is picked up within ~1 hr (or on the next cold start). To ship code changes, re-run the Step 2 deploy command.
- **Region:** `asia-south1` (Mumbai) for low latency in India.
