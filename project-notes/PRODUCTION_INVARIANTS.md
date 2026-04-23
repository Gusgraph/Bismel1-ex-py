# Production invariants

These rules must not regress.

## Prime Stocks
- Runtime state is slot-scoped and account-scoped:
  - `users/{uid}/accounts/{account_id}/prime_stocks/current/slots/slot_{n}/...`
- Slot config is the source of truth for Prime symbols.
- Slot-less or account-scoped fallback reads may help continuity, but they must not mutate active slot config.
- Preview must never downgrade real execution state.
- Prime execution rules are product-local:
  - per-symbol entry budget about `3%` of equity
  - total `FirstLot` budget about `20%` of equity
  - add-side budget about `70%` of equity
- Prime must not reuse generic Execution guardrails such as:
  - `max_notional_per_order`
  - generic execution dry-run stamping
  - Execution global guardrails

## Execution
- Runtime state is slot-scoped and symbol-first.
- Stocks and ETFs both run through the same `us_equity` execution path.
- Symbol-level runtime state must be written on processed cycles, including `no_signal` and skipped states.
- Removing a symbol must clear both:
  - `selected_symbols`
  - `symbol_assignments`
- Same-symbol duplicate rebuy protection must remain active.

## Shared app behavior
- The selected account/slot must drive runtime reads, broker state, and UI state consistently.
- Broker reconnect must resume the same Alpaca identity and preserve historical state.
- Positions and orders pages must prefer Alpaca-synced truth over stale local snapshot values.
- UI/runtime fallback reads must not resurrect removed symbols or mutate config.

## Release hygiene
- Do not deploy from a broadly dirty worktree.
- Release only an intentionally staged scope.
- After runtime changes, verify both:
  - active Cloud Run revision
  - live Firestore/UI truth

## Focused smoke checks
- Prime runtime tests:
  - `cd /home/gusgraphy/Bismel1-ex-py && venv/bin/pytest tests/test_prime_stocks_dry_run.py tests/test_scheduler_invocation.py tests/test_firestore_runtime_store.py -q`
- Execution runtime tests:
  - `cd /home/gusgraphy/Bismel1-ex-py && venv/bin/pytest tests/test_execution_runtime_base.py tests/test_scheduler_invocation.py -q`
- Laravel automation/runtime tests:
  - `cd /var/www/html/bismel1.com && php artisan test tests/Feature/Customer/CustomerAutomationRuntimeTest.php tests/Feature/Customer/CustomerTradingPagesTest.php tests/Feature/Customer/Bismel1EntitlementEnforcementTest.php`
- Broker/account sync tests:
  - `cd /var/www/html/bismel1.com && php artisan test tests/Feature/Broker/AlpacaAccountSyncServiceTest.php tests/Feature/Customer/CustomerSecretFlowsTest.php`

## Release checklist
- Confirm no user/account-specific logic was introduced.
- Run focused tests for the changed product path.
- Deploy only the scoped runtime/app changes.
- Confirm new revision has `100%` traffic only after live proof.
- Verify one Prime slot and one Execution slot end to end after deploy.
