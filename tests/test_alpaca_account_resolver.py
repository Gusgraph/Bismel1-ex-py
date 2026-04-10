from __future__ import annotations

import json
from urllib.error import HTTPError

import pytest

from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
    RuntimeAccountTarget,
)
from app.services.firestore_runtime_store import PrimeStocksRuntimeConfigRecord
from app.shared.config import AppConfig


def test_resolver_returns_paper_account_context() -> None:
    resolver = LaravelAlpacaAccountResolver(
        settings=_settings(),
        http_client=FakeHttpClient(
            payload={
                "account_id": 101,
                "uid": "user-a",
                "alpaca_account_id": 501,
                "broker_connection_id": 301,
                "broker_credential_id": 401,
                "slot_number": 1,
                "environment": "paper",
                "data_feed": "iex",
                "access_mode": "trade",
                "trade_enabled": True,
                "entitlement": {
                    "product_key": "stocks.bismel1",
                    "runtime_allowed": True,
                },
                "key_id": "paper-key",
                "secret": "paper-secret",
            }
        ),
    )

    context = resolver.resolve_runtime_account(_runtime_config())

    assert context == ResolvedAlpacaAccountContext(
        account_id=101,
        uid="user-a",
        alpaca_account_id=501,
        broker_connection_id=301,
        broker_credential_id=401,
        environment="paper",
        data_feed="iex",
        access_mode="trade",
        trade_enabled=True,
        slot_number=1,
        entitlement={
            "product_key": "stocks.bismel1",
            "runtime_allowed": True,
        },
        key_id="paper-key",
        secret="paper-secret",
    )


def test_resolver_returns_live_account_context() -> None:
    resolver = LaravelAlpacaAccountResolver(
        settings=_settings(),
        http_client=FakeHttpClient(
            payload={
                "account_id": 101,
                "uid": "user-a",
                "alpaca_account_id": 502,
                "broker_connection_id": 301,
                "broker_credential_id": 402,
                "slot_number": 2,
                "environment": "live",
                "data_feed": "iex",
                "access_mode": "trade",
                "trade_enabled": True,
                "entitlement": {
                    "product_key": "stocks.bismel1",
                    "runtime_allowed": True,
                    "live_available": False,
                },
                "key_id": "live-key",
                "secret": "live-secret",
            }
        ),
    )

    context = resolver.resolve_runtime_account(_runtime_config(alpaca_account_id=502))

    assert context.environment == "live"
    assert context.slot_number == 2
    assert context.key_id == "live-key"
    assert context.entitlement["runtime_allowed"] is True


def test_resolver_rejects_missing_selector_fields() -> None:
    resolver = LaravelAlpacaAccountResolver(settings=_settings(), http_client=FakeHttpClient(payload={}))

    with pytest.raises(AlpacaAccountResolutionError, match="missing account_id or alpaca_account_id"):
        resolver.resolve_runtime_account(_runtime_config(account_id=None))


def test_resolver_maps_not_found_into_controlled_error() -> None:
    resolver = LaravelAlpacaAccountResolver(
        settings=_settings(),
        http_client=FakeHttpClient(
            error=HTTPError(
                url="https://bismel1.test/runtime/prime-stocks/account-context",
                code=404,
                msg="Not Found",
                hdrs=None,
                fp=None,
            )
        ),
    )

    with pytest.raises(AlpacaAccountResolutionError, match="was not found"):
        resolver.resolve_runtime_account(_runtime_config())


def test_resolver_returns_scheduler_runtime_targets() -> None:
    resolver = LaravelAlpacaAccountResolver(
        settings=_settings(),
        http_client=FanoutHttpClient(),
    )

    targets = resolver.list_runtime_targets()

    assert targets == [
        RuntimeAccountTarget(
            uid="user-a",
            account_id=101,
            alpaca_account_id=501,
            slot_number=1,
            environment="paper",
            account_label="Account 1",
            entitlement={"runtime_allowed": True},
        ),
        RuntimeAccountTarget(
            uid="user-b",
            account_id=202,
            alpaca_account_id=502,
            slot_number=2,
            environment="paper",
            account_label="Account 2",
            entitlement={"runtime_allowed": True},
        ),
    ]


class FakeHttpClient:
    def __init__(self, payload: dict[str, object] | None = None, error: Exception | None = None) -> None:
        self.payload = payload or {}
        self.error = error

    def request_json(self, *, url: str, headers: dict[str, str]) -> dict[str, object]:
        assert "account_id=101" in url
        assert "alpaca_account_id=" in url
        assert url.startswith("https://bismel1.test/runtime/prime-stocks/account-context?")
        assert headers["Authorization"].startswith("Bearer ")
        if self.error is not None:
            raise self.error
        return json.loads(json.dumps(self.payload))


class FanoutHttpClient:
    def request_json(self, *, url: str, headers: dict[str, str]) -> dict[str, object]:
        assert url == "https://bismel1.test/runtime/prime-stocks/account-context?fanout=1"
        assert headers["Authorization"] == "Bearer bridge-token"
        return {
            "count": 2,
            "targets": [
                {
                    "uid": "user-a",
                    "account_id": 101,
                    "alpaca_account_id": 501,
                    "slot_number": 1,
                    "environment": "paper",
                    "account_label": "Account 1",
                    "entitlement": {"runtime_allowed": True},
                },
                {
                    "uid": "user-b",
                    "account_id": 202,
                    "alpaca_account_id": 502,
                    "slot_number": 2,
                    "environment": "paper",
                    "account_label": "Account 2",
                    "entitlement": {"runtime_allowed": True},
                },
            ],
        }


def _runtime_config(*, account_id: int | None = 101, alpaca_account_id: int | None = 501) -> PrimeStocksRuntimeConfigRecord:
    return PrimeStocksRuntimeConfigRecord(
        product_key="stocks.bismel1",
        strategy_key="prime_stocks",
        strategy_title="Prime Stocks Bot Trader",
        symbol="AAPL",
        asset_type="stock",
        enabled=True,
        dry_run=False,
        paper_execution_enabled=True,
        live_execution_enabled=False,
        ping_enabled=False,
        ping_mode="off",
        ping_daily_heartbeat_enabled=False,
        test_mode=False,
        test_trigger=None,
        test_symbol_override=None,
        force_candidate_action=None,
        ai_validation_bypass_enabled=False,
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=351,
        trend_bar_limit=221,
        first_lot_notional=101.0,
        multi_notional=73.0,
        max_notional_per_order=303.0,
        max_total_notional_per_symbol=707.0,
        max_add_count=2,
        daily_order_cap=None,
        max_open_positions=None,
        broker_retry_max_attempts=1,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        runtime_target="cloud_run",
        entitlement={},
    )


def _settings() -> AppConfig:
    return AppConfig(
        app_name="Bismel1-ex-py",
        environment="test",
        host="0.0.0.0",
        port=8080,
        cloud_run_target=True,
        pine_source_filename="Stocks-pine.pine",
        firestore_project_id=None,
        firestore_database_id="(default)",
        firestore_runtime_collection="runtime_products",
        firestore_product_document="prime_stocks",
        laravel_runtime_bridge_url="https://bismel1.test/runtime/prime-stocks/account-context",
        laravel_runtime_bridge_token="bridge-token",
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_live_trading_base_url="https://api.alpaca.markets",
        alpaca_api_key_id=None,
        alpaca_api_secret=None,
        alpaca_data_feed="iex",
        gemini_model="gemini-2.5-flash-lite",
        ai_cache_max_age_minutes=360,
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_ai_validation_bypass_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_test_mode=False,
        prime_stocks_test_trigger=None,
        prime_stocks_test_symbol_override=None,
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
        prime_stocks_first_lot_notional=101.0,
        prime_stocks_multi_notional=73.0,
        prime_stocks_max_notional_per_order=303.0,
        prime_stocks_max_total_notional_per_symbol=707.0,
        prime_stocks_max_add_count=2,
        prime_stocks_daily_order_cap=None,
        prime_stocks_max_open_positions=None,
        prime_stocks_broker_retry_max_attempts=1,
        prime_stocks_force_candidate_action=None,
        prime_stocks_scheduler_job_name="prime-stocks-scheduled",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="secret-value",
        prime_stocks_ping_scheduler_job_name="prime-stocks-ping",
        prime_stocks_ping_scheduler_schedule="*/1 * * * *",
        prime_stocks_ping_scheduler_timezone="Etc/UTC",
        prime_stocks_ping_scheduler_header_value="ping-secret-value",
    )
