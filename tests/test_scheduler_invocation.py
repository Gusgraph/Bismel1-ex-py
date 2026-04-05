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
from app.runtime.prime_stocks_dry_run import PrimeStocksRuntimeResult


def _request_with_headers(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/runtime/prime-stocks/scheduled",
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
                skipped_reason="market_data_unavailable",
                latest_signal_time=None,
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
