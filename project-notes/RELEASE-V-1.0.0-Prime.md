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

## Excluded
- unrelated experimental runtime files
- `__pycache__` / `*.pyc`
