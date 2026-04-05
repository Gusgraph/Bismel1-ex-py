<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: docs/cloud-scheduler-prime-stocks.md
======================================================
-->

# Prime Stocks Cloud Scheduler Wiring

This phase prepares deployment wiring for scheduled server-side execution. It does not prove that Cloud Run or Cloud Scheduler has already been deployed.

## Runtime target

- Method: `POST`
- Path: `/runtime/prime-stocks/scheduled`
- Runtime continuity: Cloud Scheduler invokes Cloud Run, not the browser
- Runtime cadence rule: the service only processes a newly closed `1H` bar and returns a clean no-op when the latest closed bar was already processed

## Invocation contract

- Cloud Scheduler should target the deployed Cloud Run base URL plus `/runtime/prime-stocks/scheduled`
- Cloud Run should remain authenticated with `--no-allow-unauthenticated`
- Cloud Scheduler should use an OIDC service account when invoking Cloud Run
- Cloud Scheduler should send `Content-Type: application/json`
- Cloud Scheduler may send the configured header pair from `PRIME_STOCKS_SCHEDULER_HEADER_NAME` and `PRIME_STOCKS_SCHEDULER_HEADER_VALUE`
- If `PRIME_STOCKS_SCHEDULER_HEADER_VALUE` is unset, the app does not require the custom header
- If `PRIME_STOCKS_SCHEDULER_HEADER_VALUE` is set, the scheduled endpoint rejects requests whose configured header value does not match

## Deployment sequence

1. Build and publish the container image.
2. Deploy or update Cloud Run with `scripts/deploy_cloud_run.sh`.
3. Capture the Cloud Run service URL.
4. Configure the Scheduler job with `scripts/configure_cloud_scheduler.sh`.
5. Trigger the job manually from Google Cloud or with `gcloud scheduler jobs run` after deployment.

## Notes

- Prime Stocks remains stocks-only.
- Paper execution remains guarded and server-side.
- Live trading mode is still not part of this phase.
- The browser is not part of runtime continuity.
