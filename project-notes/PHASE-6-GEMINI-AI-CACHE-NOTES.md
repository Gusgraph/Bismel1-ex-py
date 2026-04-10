<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: project-notes/PHASE-6-GEMINI-AI-CACHE-NOTES.md
======================================================
-->

# Phase 6 Gemini AI Cache Notes

## Implemented In This Phase

- Added a reusable Gemini scoring service under `app/services/gemini_ai_scoring.py`.
- Kept Gemini provider calls outside the Prime Stocks hot execution path.
- Added shared Firestore AI cache records under the existing runtime root:
  - `ai_market/current`
  - `ai_symbols/{SYMBOL}`
- Added normalized AI cache fields:
  - `Ai_regime_label`
  - `Ai_sentiment_label`
  - `Ai_safety_label`
  - `Ai_confidence`
  - `Ai_reason`
  - `Ai_updated_at`
  - `Ai_source`
  - `Ai_execution_allowed`
  - `Ai_block_new_entries`
  - `Ai_block_adds`
  - `Ai_blocked_reason`
- Added a non-hot-path scorer script:
  - `scripts/score_prime_stocks_ai.py`
- Refactored the existing Gemini test script to use the shared service:
  - `scripts/test_gemini_sentiment.py`
- Wired Prime Stocks runtime and strategy to consume cached AI records only.
- Added built-in Prime Stocks AI safety/regime blocking:
  - `unsafe` blocks new entries and adds
  - `risk_off` blocks new entries and adds
  - `bearish` blocks new entries only
- Extended runtime output payloads to include cached AI state.

## Intentionally Not Implemented

- No user/account toggle for AI filtering.
- No new product/module path.
- No direct Gemini request in `PrimeStocksRuntimeService.run_once`.
- No long-article ingestion flow.
- No Laravel-side AI integration in this phase.

## Validation

- `python3 -m py_compile` on touched runtime/services/scripts/tests
- `pytest tests/test_gemini_ai_scoring.py tests/test_prime_stocks_dry_run.py tests/test_scheduler_invocation.py tests/test_firestore_runtime_store.py tests/test_firestore_runtime_bootstrap.py tests/test_alpaca_market_data.py tests/test_alpaca_account_resolver.py tests/test_bismel1_strategy_runner.py tests/test_pine_parity_map_smoke.py`
- Result:
  - `59 passed`

## Remaining Blocker

- Live Gemini scoring still depends on restored Google AI Studio billing / credits.
- Current live error remains:
  - `429 RESOURCE_EXHAUSTED`

## Next Phase

- Restore Gemini billing/credits.
- Run one real market-wide AI cache write.
- Run one real symbol-level AI cache write.
- Deploy the updated Python runtime revision and confirm real cached `Ai_*` values appear in Firestore runtime execution/snapshot/action documents.

## Post-Phase Runtime Updates - 2026-04-10

- Prime Stocks runtime was moved from product-global Firestore paths to account-scoped paths:
  - `users/{uid}/accounts/{accountId}/prime_stocks/current/*`
- Live Cloud Run validation confirmed isolated writes for:
  - `state/current`
  - `execution/current`
  - `logs/{run_id}`
  - `notifications/{run_id}`
- Hourly scheduler fan-out was fixed so the scheduled path now dispatches valid:
  - `uid`
  - `account_id`
  - `alpaca_account_id`
  per entitled account instead of using broken global context.
- Runtime hardening added:
  - fail-safe persistence markers
  - scoped audit collections for signals/actions/orders
  - notification write path with duplicate suppression
  - secret-backed auth and dedicated runtime/scheduler service accounts
- Exposure rules were finalized in runtime execution guards:
  - per symbol entry: `3%` of account equity
  - all-symbol portfolio cap for new entries: `20%`
  - all-symbol portfolio cap for adds: `70%`
- New runtime blocked reasons now include:
  - `per_symbol_entry_cap_exceeded`
  - `total_entry_exposure_cap_exceeded`
  - `total_add_exposure_cap_exceeded`
- Runtime state/execution payloads now persist:
  - `per_symbol_entry_pct`
  - `total_entry_exposure_cap_pct`
  - `total_add_exposure_cap_pct`
  - `current_total_exposure_pct`
- Paper/live behavior remains unified:
  - same strategy logic
  - same AI gating
  - same guard path
  - same persistence path
  - only broker environment differs
