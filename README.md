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

`Bismel1-ex-py` is the Python executor-side repository for Bismel1. This repo holds the canonical Prime Stocks Python strategy module, later Cloud Run service wiring, and the surrounding deployment packaging.

## Phase 2 Scope

This bounded production phase delivers:

- a canonical Python-first Prime Stocks strategy module
- stock-only enforcement at the strategy and runtime boundaries
- deterministic evaluation from bars + config + basket state
- Alpaca market-data fetches for 1H execution and 1D trend bars
- Firestore dry-run config, state, signal, snapshot, and runtime-log writes
- one Cloud Run friendly dry-run trigger surface
- focused tests for current intended behavior
- Cloud Run oriented repo/runtime notes

This phase is dry-run only. No live Alpaca orders are placed yet, no webhook flow is added, and the browser is not part of runtime ownership.

## Canonical Strategy Rule

- Python under `app/products/stocks/bismel1/` is the canonical implementation target.
- User intent is the final strategy authority.
- Pine remains reference material only.

Current Pine reference file:

- `reference/pine/Stocks-pine.pine`
- `reference/pine/Bismel1-Pine-Final.pine`

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

Prime Stocks is being shaped for server-side evaluation:

- strategy evaluation must continue without the user keeping a browser page open
- the browser is a control surface, not the runtime
- Cloud Run runs the bot server-side
- this phase exposes a dry-run runtime loop only
- later paper execution wiring can build on the same Firestore state/log path after the dry-run loop is stable

## Dry-Run Runtime Surface

The dry-run service surface is exposed through FastAPI:

- `GET /healthz`
- `GET /_diag`
- `POST /runtime/prime-stocks/dry-run`

The dry-run endpoint:

- loads one Prime Stocks runtime config
- fetches Alpaca bars for `1H` execution and `1D` trend
- runs the canonical strategy
- writes runtime state, latest snapshot, latest signal, and logs to Firestore
- places no live orders

Current Firestore path shape:

- `runtime_products/prime_stocks/config/current`
- `runtime_products/prime_stocks/state/current`
- `runtime_products/prime_stocks/snapshots/latest`
- `runtime_products/prime_stocks/signals/latest`
- `runtime_products/prime_stocks/logs/{run_id}`

## Next Phase

The next implementation step after this dry-run phase is paper execution wiring on top of the same Cloud Run and Firestore runtime loop.
