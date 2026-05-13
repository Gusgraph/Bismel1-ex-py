# V-1.0.0-Prime

Production Prime Stocks runtime release.

## Included
- Prime slot/account targeting hardening
- preview cannot overwrite submitted execution
- Prime-only allocation budgets:
  - per-symbol entry about `3%`
  - total first-lot budget about `20%`
  - add-side budget about `70%`
- removal of generic Execution guardrail leakage from Prime
- slot-config continuity / migration protections
- focused Prime scheduler/runtime regression coverage
- deployed guard blocking legacy/non-TP Prime close paths before broker submission
- close-order metadata validation for Prime close requests
- fresh market confirmation policy for take-profit close handling
- ATR/regime/trailing observations reclassified as diagnostic review signals only
- Python Cloud Run authority as the automated Prime order submitter while Laravel automated submit remains disabled/report-only

## Excluded
- unrelated experimental runtime files
- `__pycache__` / `*.pyc`
- Prime stop loss, trailing-stop close, ATR close, regime close, or Laravel automated Prime order submission
