<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: README.md
======================================================
-->

# Bismel1-ex-py

`Bismel1-ex-py` is the Python executor-side repository for Bismel1. This repo is intended to hold Pine-to-Python strategy conversion work, a later FastAPI executor service, later Alpaca integration, later Firestore-backed runtime state, and Cloud Run deployment packaging.

## Phase 1 Scope

Phase 1 creates only the minimum clean foundation:

- Python package skeleton for future multi-product executor work.
- A thin FastAPI-ready app stub with `GET /` and `GET /_diag`.
- Strategy placeholder modules for `stocks/bismel1`.
- Reference and project-note files that preserve the Pine source-of-truth rule.
- Minimal Cloud Run oriented container bootstrap.

Live trading, webhook execution, Alpaca order routing, Firestore persistence, and Pine parity implementation are intentionally not implemented in this phase.

## Pine Source-of-Truth Rule

The original Pine strategy is the exact source of truth for behavior. The expected reference file for the first strategy conversion is:

- `reference/pine/Stocks-pine.pine`

If that file is not yet present in this repo, place the Pine source there in a later step without rewriting the logic during import.

## Folder Structure

```text
app/
app/brokers/
app/products/
app/products/stocks/
app/products/stocks/bismel1/
app/runtime/
app/services/
app/shared/
project-notes/
reference/
reference/pine/
tests/
```

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
```

## Cloud Run Target

This repo targets Cloud Run deployment. The included `Dockerfile` is a minimal container entrypoint for `uvicorn` and does not assume VM-owned background services.
