# Production Invariants

Last updated: 2026-05-20

These rules protect the recovered/stabilized Bismel1 runtime state. Read this before changing account discovery, market-data routing, history fetching, order/position monitoring, or Cloud Run release flow.

## Current Production Lock - 2026-05-20
- Alpaca SDK is the primary transport for Alpaca across Prime, Execution, Admin Runtime Control, paper, and live contexts.
- REST remains an emergency fallback only. Do not remove it.
- BrokerAdapter remains the strategy/runtime boundary. Strategy code must not call Alpaca SDK internals directly.
- Shared backend-owned broker stream architecture is active. Browser/customer pages must not own broker websocket streams.
- Prime production revision validated in market-hours proof: `bismel1-prime-stocks-00074-kr4`.
- Execution production revision validated after observability deployment: `bismel1-execution-trader-00023-ld9`.
- AI scanner production revision after bounded refresh fix: `bismel1-ai-scanning-00006-48s`.
- Laravel automated broker submit remains disabled.
- `prime-stocks-ping` remains paused.
- `prime-stocks-scheduled`, `execution-scheduled`, and `ai-market-refresh` are the active scheduled paths.
- Latest Python runtime commit: `09f2121 runtime: add bounded ai refresh and execution observability`.
- Latest Laravel UI commit recorded for matching app visibility: `5a264b2 customer: update automation controls and activity visibility`.

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
- Prime first-entry target: 5.3% equity.
- Prime total first-entry/open-entry cap: 37.1% equity.
- Prime adds/recovery cap: 95% equity.
- This is intended to allow up to seven first-lot Prime positions when broker buying power, duplicate protection, pending-order protection, and risk checks allow.
- Execution sizing and guardrails remain product-specific and must not inherit Prime exposure caps.
- These must not become per-account inconsistent unless explicitly changed and documented.

## 5. Runtime Fanout Safety
- Fanout must exclude invalid broker credentials.
- Alpaca accounts with `sync_status` of `auth_failed` or `invalid` must not enter customer runtime fanout.
- Fanout must include only valid, success, or partial-success broker connections.
- Admin monitor fanout must be scoped to internal monitor UIDs and must not overwrite customer runtime docs.
- Admin monitor crypto symbols must not appear in customer symbol sets.
- Python Cloud Run remains the automated strategy/runtime order authority.
- Laravel is the account/config bridge and reporting app; Laravel automated broker submit must remain disabled unless explicitly approved as emergency rollback.

## 6. Market Data Safety
- Stock/equity paths must continue using the stock/equity market-data path.
- Crypto market-data routing is limited to explicitly allowed admin monitor users/symbols unless customer crypto support is intentionally implemented later.
- Slash-safe Firestore IDs must be used for symbols such as `UNI/USD` and `LINK/USD`.

## 7. Runtime Status/Gauge Truth
- SCAN is operational only when bars/history were fetched and processed.
- SIGNAL is pending/neutral for normal no-setup states.
- ORDER/FILL/POSITION/EXIT are not failures unless an actual attempted stage fails.
- Normal no-trade cycles such as `no_signal`, `no_op`, and `skipped_no_open_position` are not runtime failures.
- Execution legacy auto-disabled assignments are normalized into review state and must not be permanently excluded unless user/admin manual disable is explicit.
- Prime ATR/regime/trailing observations are diagnostic review signals only and must not become executable close requests.

## 8. Close-Order Safety
- Prime executable close reason is take profit only.
- Prime non-TP close candidates must be blocked before broker submission.
- Prime TP threshold is `max(ATR TP, configured percent floor)`. Current floor uses `tp_percent=3.1`.
- Prime dynamic profit extension may hold only after TP floor is reached and may close only as `take_profit_extended`; it must never create a non-TP Prime exit.
- Execution stop-loss close requires `stop_loss_enabled=true` and a valid configured percent.
- Execution automated profit closes must respect configured symbol TP percent as the minimum floor.
- Execution strategy profit exits below configured TP must be blocked with `execution_strategy_profit_below_tp_floor`.
- Execution take-profit exits below configured TP must be blocked with `execution_take_profit_below_configured_floor`.
- Execution dynamic profit extension may hold only after configured TP floor is reached.
- Manual close and residual cleanup remain separate from automated TP floor enforcement.
- Strategy-driven closes require fresh market data or fresh take-profit confirmation.
- Bismel1-submitted close orders must carry structured metadata and controlled client order IDs.
- Broker-reconciled sells without local metadata must be treated as broker reconcile, not strategy close.
- 2026-05-20 proof: XLE Execution close submitted at 14:45 UTC used `execution-close-*`, close reason `take_profit`, TP 4%, entry 57.54, confirmation 60.685, fill 60.70, realized +5.492%, and respected TP floor 59.8416.

## 9. Broker Stream Monitoring Boundary
- Alpaca websocket `trade_updates` support is monitoring-only.
- Alpaca SDK is the primary active order/read transport.
- REST remains emergency fallback only and must not become the default again without explicit rollback.
- Websocket events must never submit or cancel broker orders.
- Reconciliation remains the broker truth checker after fills, rejects, cancels, and partial fills.
- SDK transport rollout is complete for Alpaca contexts; full fallback code must remain.
- SDK read/order/reconciliation/streaming paths have been validated.
- Stream writeback must store sanitized event summaries only:
  - no API keys or secrets
  - no raw broker payloads
  - no customer-visible broker order IDs
  - no customer-visible client order IDs
  - no raw broker account numbers
- Stream failures must degrade stream health and must not stop Prime or Execution runtime cycles.

## 10. Laravel UI / Notification Boundary
- Laravel homepage and admin email notification changes must not alter Python runtime strategy logic.
- Affiliate request and product purchase admin notifications are web-app operational emails only.
- Python Cloud Run runtime remains the automated strategy/runtime authority.
- Runtime code must not emit or depend on Laravel admin notification email side effects.

## 11. Git / Release Rule
- No recovery from zip/tag may overwrite live changes without:
  - git status check
  - branch check
  - remote push check
  - backup branch/tag
  - project-notes update
- Do not deploy from a broadly dirty worktree.
- Commit and push the protected release branch before destructive recovery work.

## Current Locked Runtime References
- Prime Cloud Run revision recorded for this lock: `bismel1-prime-stocks-00074-kr4`.
- Execution Cloud Run revision recorded for this lock: `bismel1-execution-trader-00023-ld9`.
- AI scanning Cloud Run revision recorded for this lock: `bismel1-ai-scanning-00006-48s`.
- Runtime branch: `main`.
