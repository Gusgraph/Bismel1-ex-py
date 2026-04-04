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

## Created

- Minimal package skeleton under `app/`, `reference/`, `tests/`, and `project-notes/`.
- FastAPI-ready bootstrap app with `GET /` and `GET /_diag`.
- Thin Bismel1 strategy placeholder modules under `app/products/stocks/bismel1/`.
- Shared config and logging placeholders.
- Minimal `requirements.txt`, `.env.example`, `.gitignore`, `Dockerfile`, and `README.md`.
- VM duplicate-review note for nearby Python executor-related files.

## Intentionally Not Implemented

- Pine indicator parity.
- Strategy execution logic beyond placeholders.
- Alpaca order execution.
- Firestore data/runtime layer.
- Webhook handling.
- Long-running VM service assumptions.

## Next Phase

- Add the original Pine file into `reference/pine/` and start exact Pine-to-Python indicator parity work for `stocks/bismel1`.
