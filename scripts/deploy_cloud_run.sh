#!/usr/bin/env bash
# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/deploy_cloud_run.sh
# ======================================================

set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-bismel1-prime-stocks}"
REGION="${REGION:-us-central1}"
PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID before deploying.}"
IMAGE_URI="${IMAGE_URI:?Set IMAGE_URI to the built container image.}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"

DEPLOY_CMD=(
  gcloud run deploy "${SERVICE_NAME}"
  --project "${PROJECT_ID}"
  --region "${REGION}"
  --image "${IMAGE_URI}"
  --platform managed
  --no-allow-unauthenticated
)

if [[ -n "${SERVICE_ACCOUNT}" ]]; then
  DEPLOY_CMD+=(--service-account "${SERVICE_ACCOUNT}")
fi

printf 'Deploying Cloud Run service %s in %s\n' "${SERVICE_NAME}" "${REGION}"
"${DEPLOY_CMD[@]}"
