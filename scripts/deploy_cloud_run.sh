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

SERVICE_NAME="${SERVICE_NAME:?Set SERVICE_NAME before deploying.}"
REGION="${REGION:-us-east1}"
PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID before deploying.}"
IMAGE_URI="${IMAGE_URI:-}"
SOURCE_DIR="${SOURCE_DIR:-}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"
SECRET_ENV_VARS="${SECRET_ENV_VARS:-}"

DEPLOY_CMD=(
  gcloud run deploy "${SERVICE_NAME}"
  --project "${PROJECT_ID}"
  --region "${REGION}"
  --platform managed
  --no-allow-unauthenticated
)

if [[ -n "${IMAGE_URI}" && -n "${SOURCE_DIR}" ]]; then
  printf 'Set only one of IMAGE_URI or SOURCE_DIR.\n' >&2
  exit 1
fi

if [[ -n "${IMAGE_URI}" ]]; then
  DEPLOY_CMD+=(--image "${IMAGE_URI}")
elif [[ -n "${SOURCE_DIR}" ]]; then
  DEPLOY_CMD+=(--source "${SOURCE_DIR}")
else
  printf 'Set IMAGE_URI or SOURCE_DIR before deploying.\n' >&2
  exit 1
fi

if [[ -n "${SERVICE_ACCOUNT}" ]]; then
  DEPLOY_CMD+=(--service-account "${SERVICE_ACCOUNT}")
fi

if [[ -n "${SECRET_ENV_VARS}" ]]; then
  DEPLOY_CMD+=(--set-secrets "${SECRET_ENV_VARS}")
fi

if [[ $# -gt 0 ]]; then
  DEPLOY_CMD+=("$@")
fi

printf 'Deploying Cloud Run service %s in %s\n' "${SERVICE_NAME}" "${REGION}"
"${DEPLOY_CMD[@]}"
