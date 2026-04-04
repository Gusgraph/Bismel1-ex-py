<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: project-notes/PHASE-1-BOOTSTRAP-NOTES.md
======================================================
-->

# Phase 1 Bootstrap Notes

## Implemented In This Phase

- Normalized the existing package skeleton under `app/`, `reference/`, `tests/`, and `project-notes/` to match the requested phase-1 executor foundation.
- Kept the FastAPI-ready bootstrap app limited to `GET /` and `GET /_diag`.
- Kept `app/products/stocks/bismel1/` as thin placeholder modules only, without claiming Pine parity.
- Kept shared config and logging minimal for later service bootstrapping.
- Kept the container bootstrap aligned to Cloud Run intent instead of VM-owned runtime assumptions.
- Reviewed nearby VM paths that look related to older Python executor or strategy work and recorded cleanup candidates.

## Intentionally Not Implemented

- Pine indicator parity.
- Strategy execution logic beyond placeholders.
- Alpaca order execution.
- Firestore data/runtime layer.
- Webhook handling.
- Long-running VM service assumptions.

## Next Phase

- Add the original Pine file into `reference/pine/Stocks-pine.pine` and begin exact Pine-to-Python parity work in `app/products/stocks/bismel1/` starting with indicator behavior and input mapping.
