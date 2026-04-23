# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_scheduler_invocation.py
# ======================================================

from __future__ import annotations

from dataclasses import replace

from fastapi import HTTPException
from starlette.requests import Request

from app import main
from app.runtime.execution import ExecutionSchedulerDiscovery, ExecutionSchedulerTarget
from app.runtime.execution.execution_runtime_base import ExecutionRuntimeResult
from app.runtime.prime_stocks_dry_run import PrimeStocksRuntimeResult
from app.services.alpaca_account_resolver import RuntimeAccountTarget


def _request_with_headers(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/runtime/prime-stocks/scheduled",
        "headers": [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers.items()],
    }
    return Request(scope)


def _execution_request_with_headers(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/runtime/execution/scheduled",
        "headers": [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers.items()],
    }
    return Request(scope)


def test_scheduler_request_allows_matching_header() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        main._validate_scheduler_request(
            request=_request_with_headers({"X-Prime-Stocks-Scheduler": "prime-stocks-hourly"})
        )
    finally:
        main.settings = original_settings


def test_scheduler_request_rejects_mismatched_header() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        try:
            main._validate_scheduler_request(request=_request_with_headers({"X-Prime-Stocks-Scheduler": "wrong-value"}))
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected scheduler request validation to reject mismatched header values.")
    finally:
        main.settings = original_settings


def test_scheduler_request_allows_missing_header_when_not_configured() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value=None,
    )
    try:
        main._validate_scheduler_request(request=_request_with_headers({}))
    finally:
        main.settings = original_settings


def test_scheduler_request_uses_configured_header_name() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Cloud-Scheduler-Prime",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        main._validate_scheduler_request(
            request=_request_with_headers({"X-Cloud-Scheduler-Prime": "prime-stocks-hourly"})
        )
    finally:
        main.settings = original_settings


def test_execution_scheduled_endpoint_processes_active_slots_and_returns_summary() -> None:
    class ExecutionService:
        def discover_scheduler_targets(self):
            return ExecutionSchedulerDiscovery(
                total_slots_seen=3,
                runnable_slots=2,
                skipped_disabled=1,
                skipped_no_symbols=0,
                skipped_invalid_config=0,
                targets=(
                    ExecutionSchedulerTarget(
                        uid="user-a",
                        account_id=101,
                        slot_number=1,
                        product_id="execution",
                        runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
                    ),
                    ExecutionSchedulerTarget(
                        uid="user-b",
                        account_id=202,
                        slot_number=2,
                        product_id="execution",
                        runtime_path="users/user-b/accounts/202/execution/current/slots/slot_2",
                    ),
                ),
            )

        def run_once(self, payload):
            slot = payload["slot"]
            return ExecutionRuntimeResult(
                ok=True,
                run_id=f"run-{slot}",
                product_id="execution",
                uid=payload["user_id"],
                account_id=payload["account_id"],
                slot=slot,
                symbol="SLOT",
                action="evaluate",
                execution_status="buy_submitted" if slot == 1 else "no_signal",
                message="processed",
                firestore_paths={},
            )

    original_settings = main.settings
    original_builder = main.build_execution_runtime_service
    main.settings = replace(original_settings, prime_stocks_scheduler_header_value=None)
    main.build_execution_runtime_service = lambda settings: ExecutionService()
    try:
        payload = main.trigger_execution_scheduled(request=_execution_request_with_headers({}))
        assert payload["status"] == "ok"
        assert payload["product_id"] == "execution"
        assert payload["total_slots_seen"] == 3
        assert payload["runnable_slots"] == 2
        assert payload["slots_processed"] == 2
        assert payload["slots_skipped"] == 1
        assert payload["slots_failed"] == 0
        assert [item["status"] for item in payload["results"]] == ["processed", "processed"]
    finally:
        main.settings = original_settings
        main.build_execution_runtime_service = original_builder


def test_execution_scheduled_endpoint_continues_when_one_slot_fails() -> None:
    class ExecutionService:
        def discover_scheduler_targets(self):
            return ExecutionSchedulerDiscovery(
                total_slots_seen=2,
                runnable_slots=2,
                skipped_disabled=0,
                skipped_no_symbols=0,
                skipped_invalid_config=0,
                targets=(
                    ExecutionSchedulerTarget(
                        uid="user-a",
                        account_id=101,
                        slot_number=1,
                        product_id="execution",
                        runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
                    ),
                    ExecutionSchedulerTarget(
                        uid="user-b",
                        account_id=202,
                        slot_number=2,
                        product_id="execution",
                        runtime_path="users/user-b/accounts/202/execution/current/slots/slot_2",
                    ),
                ),
            )

        def run_once(self, payload):
            if payload["slot"] == 1:
                raise ValueError("slot_1 failed")
            return ExecutionRuntimeResult(
                ok=True,
                run_id="run-2",
                product_id="execution",
                uid=payload["user_id"],
                account_id=payload["account_id"],
                slot=payload["slot"],
                symbol="SLOT",
                action="evaluate",
                execution_status="no_signal",
                message="processed",
                firestore_paths={},
            )

    original_settings = main.settings
    original_builder = main.build_execution_runtime_service
    main.settings = replace(original_settings, prime_stocks_scheduler_header_value=None)
    main.build_execution_runtime_service = lambda settings: ExecutionService()
    try:
        payload = main.trigger_execution_scheduled(request=_execution_request_with_headers({}))
        assert payload["slots_processed"] == 1
        assert payload["slots_failed"] == 1
        assert payload["results"][0]["status"] == "failed"
        assert payload["results"][1]["status"] == "processed"
    finally:
        main.settings = original_settings
        main.build_execution_runtime_service = original_builder


def test_runtime_request_rejects_mismatched_service_token() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_runtime_api_token="runtime-secret",
    )
    try:
        try:
            main._validate_runtime_request(request=_request_with_headers({"X-Prime-Stocks-Service-Token": "wrong"}))
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected runtime request validation to reject mismatched service token.")
    finally:
        main.settings = original_settings


def test_runtime_request_allows_matching_service_token() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_runtime_api_token="runtime-secret",
    )
    try:
        main._validate_runtime_request(request=_request_with_headers({"X-Prime-Stocks-Service-Token": "runtime-secret"}))
    finally:
        main.settings = original_settings


def test_scheduled_endpoint_returns_conflict_for_runtime_misconfiguration() -> None:
    class FailingService:
        def run_once(self, **kwargs):
            del kwargs
            raise ValueError("Prime Stocks runtime only supports asset_type='stock'. Received 'crypto'.")

    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: FailingService()
    try:
        try:
            main.trigger_prime_stocks_scheduled(request=_request_with_headers({}))
        except HTTPException as exc:
            assert exc.status_code == 409
            assert exc.detail == "Prime Stocks runtime only supports asset_type='stock'. Received 'crypto'."
        else:
            raise AssertionError("Expected scheduled endpoint to convert runtime misconfiguration into HTTP 409.")
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduled_endpoint_returns_blocked_runtime_result_when_market_data_fetch_is_unavailable() -> None:
    class BlockedService:
        def run_once(self, **kwargs):
            del kwargs
            return PrimeStocksRuntimeResult(
                run_id="run-test-market-data",
                mode="paper",
                runtime_target="cloud_run",
                product_key="stocks.bismel1",
                strategy_key="prime_stocks",
                strategy_title="Prime Stocks Bot Trader",
                symbol="AAPL",
                asset_type="stock",
                enabled=True,
                trigger_type="scheduled",
                trigger_source="cloud_scheduler",
                candidate_action="BLOCKED",
                execution_decision="market_data_unavailable",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="market_data_unavailable",
                latest_signal_time=None,
                ai=None,
                status="blocked",
                message="Prime Stocks runtime blocked because Alpaca market data could not be fetched after runtime config load.",
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths={"config_document": "runtime_products/prime_stocks/config/current"},
            )

    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: BlockedService()
    try:
        payload = main.trigger_prime_stocks_scheduled(request=_request_with_headers({}))
        assert payload["status"] == "blocked"
        assert payload["execution_decision"] == "market_data_unavailable"
        assert payload["skipped_reason"] == "market_data_unavailable"
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduled_endpoint_passes_account_selector_overrides_to_runtime() -> None:
    class RecordingService:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def run_once(self, **kwargs):
            self.calls.append(kwargs)
            return PrimeStocksRuntimeResult(
                run_id="run-test-selector",
                mode="paper",
                runtime_target="cloud_run",
                product_key="stocks.bismel1",
                strategy_key="prime_stocks",
                strategy_title="Prime Stocks Bot Trader",
                symbol="AAPL",
                asset_type="stock",
                enabled=True,
                trigger_type="scheduled",
                trigger_source="cloud_scheduler",
                candidate_action="HOLD",
                execution_decision="no_op",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="no_action_candidate",
                latest_signal_time=None,
                ai=None,
                status="no_op",
                message="ok",
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths={"config_document": "runtime_products/prime_stocks/config/current"},
            )

    service = RecordingService()
    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: service
    try:
        main.trigger_prime_stocks_scheduled(
            request=_request_with_headers({}),
            symbol="msft",
            account_id=17,
            alpaca_account_id=29,
        )
        assert service.calls == [
            {
                "symbol": "msft",
                "uid": None,
                "account_id": 17,
                "alpaca_account_id": 29,
                "slot_number": None,
                "allow_execution": True,
                "trigger_type": "scheduled",
                "trigger_source": "cloud_scheduler",
            }
        ]
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduler_fanout_dispatches_each_active_symbol_for_target() -> None:
    class SymbolFanoutService:
        def list_scheduler_targets(self):
            return [
                RuntimeAccountTarget(
                    uid="u_1",
                    account_id=8,
                    alpaca_account_id=7,
                    slot_number=1,
                    environment="paper",
                    entitlement={"runtime_allowed": True},
                )
            ]

        def list_target_symbols(self, **kwargs):
            del kwargs
            return ["AAPL", "NVDA"]

        def run_once(self, **kwargs):
            return PrimeStocksRuntimeResult(
                run_id=f"run-{kwargs['symbol'].lower()}",
                mode="paper",
                runtime_target="cloud_run",
                product_key="stocks.bismel1",
                strategy_key="prime_stocks",
                strategy_title="Prime Stocks Bot Trader",
                symbol=kwargs["symbol"],
                asset_type="stock",
                enabled=True,
                trigger_type="scheduled",
                trigger_source="cloud_scheduler",
                candidate_action="HOLD",
                execution_decision="no_op",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="no_action_candidate",
                latest_signal_time=None,
                ai=None,
                status="no_signal",
                message="no signal",
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths={},
            )

    payload = main._run_scheduled_fanout(
        service=SymbolFanoutService(),
        symbol=None,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert payload["target_count"] == 1
    assert payload["completed_count"] == 2
    assert payload["results"][0]["symbol"] == "AAPL"
    assert payload["results"][1]["symbol"] == "NVDA"


def test_scheduled_endpoint_fans_out_across_runtime_targets() -> None:
    class FanoutService:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list_scheduler_targets(self):
            return [
                RuntimeAccountTarget(uid="user-a", account_id=11, alpaca_account_id=21, slot_number=1),
                RuntimeAccountTarget(uid="user-b", account_id=12, alpaca_account_id=22, slot_number=2),
            ]

        def run_once(self, **kwargs):
            self.calls.append(kwargs)
            return PrimeStocksRuntimeResult(
                run_id=f"run-{kwargs['account_id']}",
                mode="paper",
                runtime_target="cloud_run",
                product_key="stocks.bismel1",
                strategy_key="prime_stocks",
                strategy_title="Prime Stocks Bot Trader",
                symbol="AAPL",
                asset_type="stock",
                enabled=True,
                trigger_type="scheduled",
                trigger_source="cloud_scheduler",
                candidate_action="HOLD",
                execution_decision="skipped_no_new_bar",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="no_new_closed_bar",
                latest_signal_time=None,
                ai=None,
                status="no_op",
                message="ok",
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths={"config_document": "users/test/accounts/current"},
            )

    service = FanoutService()
    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: service
    try:
        payload = main.trigger_prime_stocks_scheduled(request=_request_with_headers({}))
        assert payload["fanout"] is True
        assert payload["target_count"] == 2
        assert [item["account_id"] for item in payload["results"]] == [11, 12]
        assert service.calls == [
                {
                    "symbol": None,
                    "uid": "user-a",
                    "account_id": 11,
                    "alpaca_account_id": 21,
                    "slot_number": 1,
                    "allow_execution": False,
                    "trigger_type": "scheduled",
                    "trigger_source": "cloud_scheduler",
                },
                {
                    "symbol": None,
                    "uid": "user-b",
                    "account_id": 12,
                    "alpaca_account_id": 22,
                    "slot_number": 2,
                    "allow_execution": False,
                    "trigger_type": "scheduled",
                    "trigger_source": "cloud_scheduler",
                },
            ]
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduler_fanout_ranks_firstlot_candidates_by_signal_score() -> None:
    class RankedFanoutService:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list_scheduler_targets(self):
            return [
                RuntimeAccountTarget(
                    uid="u_1",
                    account_id=8,
                    alpaca_account_id=7,
                    slot_number=1,
                    environment="paper",
                    entitlement={"runtime_allowed": True},
                )
            ]

        def list_target_symbols(self, **kwargs):
            del kwargs
            return ["AAPL", "NVDA", "TSLA"]

        def run_once(self, **kwargs):
            self.calls.append(kwargs)
            score_map = {"AAPL": 71.0, "NVDA": 96.5, "TSLA": 44.0}
            symbol = kwargs["symbol"]
            allow_execution = kwargs["allow_execution"]
            preview_only = kwargs.get("preview_only", False)
            if allow_execution:
                if preview_only:
                    return PrimeStocksRuntimeResult(
                        run_id=f"run-{symbol.lower()}-preview",
                        mode="paper",
                        runtime_target="cloud_run",
                        product_key="stocks.bismel1",
                        strategy_key="prime_stocks",
                        strategy_title="Prime Stocks Bot Trader",
                        symbol=symbol,
                        asset_type="stock",
                        enabled=True,
                        trigger_type="scheduled",
                        trigger_source="cloud_scheduler",
                        candidate_action="FirstLot",
                        execution_decision="preview_only",
                        order_status="not_submitted",
                        order_submitted=False,
                        order_id=None,
                        client_order_id=None,
                        add_tier=None,
                        execution_allowed=False,
                        skipped_reason="preview_only",
                        latest_signal_time="2026-04-16T20:15:00+00:00",
                        ai=None,
                        status="signal",
                        message="preview",
                        bars_processed_execution=24,
                        bars_processed_trend=4,
                        firestore_paths={},
                        signal_score=score_map[symbol],
                    )
                return PrimeStocksRuntimeResult(
                    run_id=f"run-{symbol.lower()}-exec",
                    mode="paper",
                    runtime_target="cloud_run",
                    product_key="stocks.bismel1",
                    strategy_key="prime_stocks",
                    strategy_title="Prime Stocks Bot Trader",
                    symbol=symbol,
                    asset_type="stock",
                    enabled=True,
                    trigger_type="scheduled",
                    trigger_source="cloud_scheduler",
                    candidate_action="FirstLot",
                    execution_decision="submitted_buy",
                    order_status="accepted",
                    order_submitted=True,
                    order_id=f"order-{symbol.lower()}",
                    client_order_id=f"client-{symbol.lower()}",
                    add_tier=None,
                    execution_allowed=True,
                    skipped_reason=None,
                    latest_signal_time="2026-04-16T20:15:00+00:00",
                    ai=None,
                    status="signal",
                    message="executed",
                    bars_processed_execution=24,
                    bars_processed_trend=4,
                    firestore_paths={},
                    signal_score=score_map[symbol],
                )
            raise AssertionError("Preview pass should use allow_execution=True with preview_only=True.")

    service = RankedFanoutService()
    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: service
    try:
        payload = main._run_scheduled_fanout(
            service=service,
            symbol=None,
            trigger_type="scheduled",
            trigger_source="cloud_scheduler",
        )
        assert payload["target_count"] == 1
        assert [item["symbol"] for item in payload["results"]] == ["AAPL", "NVDA", "TSLA"]
        assert service.calls[:3] == [
                {
                    "symbol": "AAPL",
                    "uid": "u_1",
                    "account_id": 8,
                    "alpaca_account_id": 7,
                    "slot_number": 1,
                    "allow_execution": True,
                    "preview_only": True,
                    "trigger_type": "scheduled",
                    "trigger_source": "cloud_scheduler",
                },
                {
                    "symbol": "NVDA",
                    "uid": "u_1",
                    "account_id": 8,
                    "alpaca_account_id": 7,
                    "slot_number": 1,
                    "allow_execution": True,
                    "preview_only": True,
                    "trigger_type": "scheduled",
                    "trigger_source": "cloud_scheduler",
                },
                {
                    "symbol": "TSLA",
                    "uid": "u_1",
                    "account_id": 8,
                    "alpaca_account_id": 7,
                    "slot_number": 1,
                    "allow_execution": True,
                    "preview_only": True,
                    "trigger_type": "scheduled",
                    "trigger_source": "cloud_scheduler",
                },
            ]
        assert service.calls[3:6] == [
            {
                "symbol": "NVDA",
                "uid": "u_1",
                "account_id": 8,
                "alpaca_account_id": 7,
                "slot_number": 1,
                "allow_execution": True,
                "trigger_type": "scheduled",
                "trigger_source": "cloud_scheduler",
            },
            {
                "symbol": "AAPL",
                "uid": "u_1",
                "account_id": 8,
                "alpaca_account_id": 7,
                "slot_number": 1,
                "allow_execution": True,
                "trigger_type": "scheduled",
                "trigger_source": "cloud_scheduler",
            },
            {
                "symbol": "TSLA",
                "uid": "u_1",
                "account_id": 8,
                "alpaca_account_id": 7,
                "slot_number": 1,
                "allow_execution": True,
                "trigger_type": "scheduled",
                "trigger_source": "cloud_scheduler",
            },
        ]
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduler_fanout_holds_lower_ranked_candidates_after_prime_entry_budget_is_filled() -> None:
    class RankedFanoutService:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list_scheduler_targets(self):
            return [
                RuntimeAccountTarget(
                    uid="u_1",
                    account_id=8,
                    alpaca_account_id=7,
                    slot_number=1,
                    environment="paper",
                    entitlement={"runtime_allowed": True},
                )
            ]

        def list_target_symbols(self, **kwargs):
            del kwargs
            return ["AAPL", "NVDA", "TSLA"]

        def run_once(self, **kwargs):
            self.calls.append(kwargs)
            symbol = kwargs["symbol"]
            allow_execution = kwargs["allow_execution"]
            preview_only = kwargs.get("preview_only", False)
            score_map = {"AAPL": 91.0, "NVDA": 61.0, "TSLA": 31.0}
            if allow_execution:
                if preview_only:
                    return PrimeStocksRuntimeResult(
                        run_id=f"run-{symbol.lower()}-preview",
                        mode="paper",
                        runtime_target="cloud_run",
                        product_key="stocks.bismel1",
                        strategy_key="prime_stocks",
                        strategy_title="Prime Stocks Bot Trader",
                        symbol=symbol,
                        asset_type="stock",
                        enabled=True,
                        trigger_type="scheduled",
                        trigger_source="cloud_scheduler",
                        candidate_action="FirstLot",
                        execution_decision="preview_only",
                        order_status="not_submitted",
                        order_submitted=False,
                        order_id=None,
                        client_order_id=None,
                        add_tier=None,
                        execution_allowed=False,
                        skipped_reason="preview_only",
                        latest_signal_time="2026-04-16T20:15:00+00:00",
                        ai=None,
                        status="signal",
                        message="preview",
                        bars_processed_execution=24,
                        bars_processed_trend=4,
                        firestore_paths={},
                        signal_score=score_map[symbol],
                    )
                if symbol == "AAPL":
                    return PrimeStocksRuntimeResult(
                        run_id="run-aapl-exec",
                        mode="paper",
                        runtime_target="cloud_run",
                        product_key="stocks.bismel1",
                        strategy_key="prime_stocks",
                        strategy_title="Prime Stocks Bot Trader",
                        symbol=symbol,
                        asset_type="stock",
                        enabled=True,
                        trigger_type="scheduled",
                        trigger_source="cloud_scheduler",
                        candidate_action="FirstLot",
                        execution_decision="prime_total_entry_budget_reached",
                        order_status="not_submitted",
                        order_submitted=False,
                        order_id=None,
                        client_order_id=None,
                        add_tier=None,
                        execution_allowed=False,
                        skipped_reason="prime_total_entry_budget_reached",
                        latest_signal_time="2026-04-16T20:15:00+00:00",
                        ai=None,
                        status="signal",
                        message="budget full",
                        bars_processed_execution=24,
                        bars_processed_trend=4,
                        firestore_paths={},
                        signal_score=score_map[symbol],
                    )
                raise AssertionError("Lower-ranked candidates should not execute after Prime entry budget is reached.")
            raise AssertionError("Preview pass should use allow_execution=True with preview_only=True.")

    service = RankedFanoutService()
    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(original_settings, prime_stocks_scheduler_header_value=None)
    main.build_prime_stocks_runtime_service = lambda settings: service
    try:
        payload = main._run_scheduled_fanout(
            service=service,
            symbol=None,
            trigger_type="scheduled",
            trigger_source="cloud_scheduler",
        )
        by_symbol = {item["symbol"]: item for item in payload["results"]}
        assert by_symbol["AAPL"]["execution_decision"] == "prime_total_entry_budget_reached"
        assert by_symbol["NVDA"]["execution_decision"] == "held_for_higher_ranked_candidates"
        assert by_symbol["TSLA"]["execution_decision"] == "held_for_higher_ranked_candidates"
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder


def test_scheduled_ping_endpoint_uses_ping_header_value_and_test_trigger() -> None:
    class RecordingService:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def run_once(self, **kwargs):
            self.calls.append(kwargs)
            return PrimeStocksRuntimeResult(
                run_id="run-test-ping",
                mode="dry-run",
                runtime_target="cloud_run",
                product_key="stocks.bismel1",
                strategy_key="prime_stocks",
                strategy_title="Prime Stocks Bot Trader",
                symbol="SHIBUSD",
                asset_type="crypto",
                enabled=True,
                trigger_type="ping",
                trigger_source="cloud_scheduler",
                candidate_action="PING",
                execution_decision="validation_ping_ok",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason=None,
                latest_signal_time=None,
                ai=None,
                status="validation",
                message="ok",
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths={"config_document": "runtime_products/prime_stocks/config/current"},
            )

    service = RecordingService()
    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_ping_scheduler_header_value="prime-stocks-ping",
    )
    main.build_prime_stocks_runtime_service = lambda settings: service
    try:
        payload = main.trigger_prime_stocks_scheduled_ping(
            request=_request_with_headers({"X-Prime-Stocks-Scheduler": "prime-stocks-ping"}),
            symbol="ignored",
        )
        assert payload["execution_decision"] == "validation_ping_ok"
        assert service.calls == [
            {
                "symbol": "ignored",
                "uid": None,
                "account_id": None,
                "alpaca_account_id": None,
                "slot_number": None,
                "allow_execution": False,
                "trigger_type": "ping",
                "trigger_source": "cloud_scheduler",
                "test_trigger": "ping",
            }
        ]
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder
