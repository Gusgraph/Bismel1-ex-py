#!/usr/bin/env bash
# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/configure_cloud_scheduler.sh
# ======================================================

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID before configuring Cloud Scheduler.}"
REGION="${REGION:-us-central1}"
JOB_NAME="${JOB_NAME:-prime-stocks-scheduled}"
SCHEDULE="${SCHEDULE:-5 * * * 1-5}"
TIME_ZONE="${TIME_ZONE:-Etc/UTC}"
SERVICE_URL="${SERVICE_URL:?Set SERVICE_URL to the Cloud Run base URL.}"
SCHEDULER_HEADER_NAME="${SCHEDULER_HEADER_NAME:-X-Prime-Stocks-Scheduler}"
SCHEDULER_HEADER_VALUE="${SCHEDULER_HEADER_VALUE:-prime-stocks-hourly}"
OIDC_SERVICE_ACCOUNT_EMAIL="${OIDC_SERVICE_ACCOUNT_EMAIL:?Set OIDC_SERVICE_ACCOUNT_EMAIL for authenticated invocation.}"

TARGET_URI="${SERVICE_URL%/}/runtime/prime-stocks/scheduled"
HEADERS="Content-Type=application/json,${SCHEDULER_HEADER_NAME}=${SCHEDULER_HEADER_VALUE}"

COMMON_ARGS=(
  --project "${PROJECT_ID}"
  --location "${REGION}"
  --schedule "${SCHEDULE}"
  --time-zone "${TIME_ZONE}"
  --http-method POST
  --uri "${TARGET_URI}"
  --headers "${HEADERS}"
  --oidc-service-account-email "${OIDC_SERVICE_ACCOUNT_EMAIL}"
  --oidc-token-audience "${SERVICE_URL}"
  --message-body '{}'
)

if gcloud scheduler jobs describe "${JOB_NAME}" --project "${PROJECT_ID}" --location "${REGION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${JOB_NAME}" "${COMMON_ARGS[@]}"
else
  gcloud scheduler jobs create http "${JOB_NAME}" "${COMMON_ARGS[@]}"
fi
