# Production Invariants

Last updated: 2026-04-28

These rules protect the recovered/stabilized Bismel1 runtime state. Read this before changing account discovery, market-data routing, history fetching, order/position monitoring, or Cloud Run release flow.

## 1. Admin Runtime Monitor
- Admin-only crypto monitor exists.
- Prime monitor symbol: `UNI/USD`.
- Execution monitor symbol: `LINK/USD`.
- Shared Admin Monitor Broker is used by both monitors.
- Admin monitor crypto access is restricted to:
  - `admin-runtime-monitor-prime`
  - `admin-runtime-monitor-execution`
- Admin monitor accounts must never be treated as normal customer accounts in customer UI or billing.
- Admin monitor orders, positions, and activity must be scoped to the monitor user/account/product/symbol.

## 2. Customer Product Boundaries
- Prime Stocks customer product remains stock/equity scoped.
- Execution customer product remains existing supported customer asset scope.
- Admin crypto monitor does not unlock crypto for customers.
- No entitlement, billing, or customer access behavior changes come from monitor support.
- Customer runtime fanout must not include admin monitor users except through the explicit admin-monitor branch.

## 3. Strategy Runtime Rules
- Prime uses 15M closed-bar execution.
- Prime uses 1D trend as directional bias.
- Historical bars must be fetched and passed into strategy:
  - Prime execution history: 227+ where applicable.
  - Prime trend/bias history: 1D history.
  - Execution strategy history enough for strategy warmup.
- Latest closed bar must not blanket-skip:
  - position monitoring
  - exit evaluation
  - runtime status refresh
- Duplicate entry protection must remain.
- No new bar can block exits or position monitoring.
- AI cache staleness may produce an advisory or pending signal state, but must not be treated as a runtime crash when scan and strategy evaluation still complete.

## 4. Exposure Rules
- Per-symbol entry target: 3% equity.
- Total account exposure cap: 20%.
- Adds up to 70%.
- These must not become per-product inconsistent unless explicitly changed and documented.

## 5. Runtime Fanout Safety
- Fanout must exclude invalid broker credentials.
- Alpaca accounts with `sync_status` of `auth_failed` or `invalid` must not enter customer runtime fanout.
- Fanout must include only valid, success, or partial-success broker connections.
- Admin monitor fanout must be scoped to internal monitor UIDs and must not overwrite customer runtime docs.
- Admin monitor crypto symbols must not appear in customer symbol sets.

## 6. Market Data Safety
- Stock/equity paths must continue using the stock/equity market-data path.
- Crypto market-data routing is limited to explicitly allowed admin monitor users/symbols unless customer crypto support is intentionally implemented later.
- Slash-safe Firestore IDs must be used for symbols such as `UNI/USD` and `LINK/USD`.

## 7. Runtime Status/Gauge Truth
- SCAN is operational only when bars/history were fetched and processed.
- SIGNAL is pending/neutral for normal no-setup states.
- ORDER/FILL/POSITION/EXIT are not failures unless an actual attempted stage fails.
- Normal no-trade cycles such as `no_signal`, `no_op`, and `skipped_no_open_position` are not runtime failures.

## 8. Git / Release Rule
- No recovery from zip/tag may overwrite live changes without:
  - git status check
  - branch check
  - remote push check
  - backup branch/tag
  - project-notes update
- Do not deploy from a broadly dirty worktree.
- Commit and push the protected release branch before destructive recovery work.

## Current Locked Runtime References
- Prime Cloud Run revision recorded for this lock: `bismel1-prime-stocks-00063-w9w`.
- Execution Cloud Run revision recorded for this lock: `bismel1-execution-trader-00015-cv6`.
- Runtime lock branch: `release/runtime-monitor-lock`.
