# V-1.0.0-Execute

Production Execution runtime release.

## Included
- live scheduled Execution processing
- symbol-first Execution runtime state writes
- stock + ETF support through the existing US-equity path
- same-symbol rebuy protection
- Execution strategy/runtime test coverage
- production invariants for shared runtime/account behavior
- optional runtime-evaluated long-position stop-loss exits when `risk_settings.stop_loss_enabled` is true and `stop_loss_percent` is positive
- review-state behavior for legacy/system auto-disable conditions so system review does not permanently remove watched symbols from future cycles
- structured close-order metadata guards before broker submit
- fresh market-data confirmation policy for strategy-driven closes
- Python Cloud Run authority as the automated Execution order submitter while Laravel automated submit remains disabled/report-only

## Excluded
- unrelated experimental files
- `__pycache__` / `*.pyc`
- hidden broker-side stop orders; stop loss is evaluated by the runtime and routed through the normal close path
- Laravel automated strategy order submission
