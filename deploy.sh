#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT:-hazel-hall-487120-v3}"
REGION="${REGION:-us-east1}"
SA="${SA:-495337434896-compute@developer.gserviceaccount.com}"
IMAGE="us-east1-docker.pkg.dev/${PROJECT}/elmeeda/elmeeda-watch:latest"
JOB="${JOB:-samsara-watch}"   # keep existing job name so scheduler keeps pointing here

echo "==> Building + pushing image..."
gcloud builds submit --config=cloudbuild.yaml \
  --region="$REGION" --project="$PROJECT" \
  --substitutions=SHORT_SHA="$(git rev-parse --short HEAD)" .

echo "==> Updating Cloud Run Job $JOB..."
gcloud run jobs update "$JOB" \
  --region="$REGION" --project="$PROJECT" \
  --image="$IMAGE" \
  --service-account="$SA" \
  --set-secrets="DATABASE_URL=ELMEEDA_DATABASE_URL_V1:latest,ENCRYPTION_SECRET=encryption-secret:latest,SECRET_KEY=secret-key:latest,TELEGRAM_BOT_TOKEN=TELEGRAM_SAMSARA_BOT_TOKEN:latest,TELEGRAM_CHAT_ID=TELEGRAM_SAMSARA_CHAT_ID:latest" \
  --set-env-vars="GCP_PROJECT=${PROJECT}" \
  --set-cloudsql-instances="${PROJECT}:${REGION}:elmeeda-db" \
  --args="watch-faults" \
  --max-retries=1 --memory=512Mi --cpu=1 --task-timeout=300 2>/dev/null || \
gcloud run jobs create "$JOB" \
  --region="$REGION" --project="$PROJECT" \
  --image="$IMAGE" \
  --service-account="$SA" \
  --set-secrets="DATABASE_URL=ELMEEDA_DATABASE_URL_V1:latest,ENCRYPTION_SECRET=encryption-secret:latest,SECRET_KEY=secret-key:latest,TELEGRAM_BOT_TOKEN=TELEGRAM_SAMSARA_BOT_TOKEN:latest,TELEGRAM_CHAT_ID=TELEGRAM_SAMSARA_CHAT_ID:latest" \
  --set-env-vars="GCP_PROJECT=${PROJECT}" \
  --set-cloudsql-instances="${PROJECT}:${REGION}:elmeeda-db" \
  --args="watch-faults" \
  --max-retries=1 --memory=512Mi --cpu=1 --task-timeout=300

echo "==> Ensuring scheduler is wired..."
gcloud scheduler jobs update http samsara-job-watch \
  --project="$PROJECT" --location="$REGION" \
  --schedule="*/5 * * * *" --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --http-method=POST \
  --oauth-service-account-email="$SA" \
  --description="Every 5min: elmeeda-watch scans faults, creates breakdown dispatches" 2>/dev/null || \
gcloud scheduler jobs create http samsara-job-watch \
  --project="$PROJECT" --location="$REGION" \
  --schedule="*/5 * * * *" --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --http-method=POST \
  --oauth-service-account-email="$SA" \
  --description="Every 5min: elmeeda-watch scans faults, creates breakdown dispatches"

echo "==> Done. Manual test:"
echo "    gcloud run jobs execute $JOB --region=$REGION --project=$PROJECT --wait"
