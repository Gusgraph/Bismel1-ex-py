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

## Excluded
- unrelated experimental files
- `__pycache__` / `*.pyc`
- hidden broker-side stop orders; stop loss is evaluated by the runtime and routed through the normal close path
