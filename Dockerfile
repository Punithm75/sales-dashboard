# Container for the BlissClub D2C Sales Dashboard (Streamlit + DuckDB) on Google Cloud Run.
# The app itself is unchanged — this just packages it and binds Cloud Run's $PORT.
FROM python:3.12-slim

WORKDIR /app

# Install Python deps first so this layer is cached across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code. Secrets, data files and logos are excluded via .dockerignore
# (secrets are mounted at runtime; data comes from the Drive folder; the logo is
# already embedded as base64 inside app_drive.py).
COPY . .

# Cloud Run routes traffic to $PORT (default 8080); Streamlit must bind it on 0.0.0.0.
ENV PORT=8080
EXPOSE 8080
CMD streamlit run app_drive.py \
    --server.port=${PORT} \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false
