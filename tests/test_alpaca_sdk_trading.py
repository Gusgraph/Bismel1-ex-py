from __future__ import annotations

from dataclasses import replace

from alpaca.common.exceptions import APIError

from app.brokers.alpaca_paper_trading import AlpacaBrokerRetryPolicy, AlpacaPaperTradingAdapter
from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.brokers.contracts import BrokerAdapter
from app.brokers.factory import build_alpaca_broker_adapter, resolve_alpaca_transport
from app.brokers.models import BrokerOrderRequest
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


class FakeSdkClient:
    def __init__(self, *, fail_once: Exception | None = None) -> None:
        self.orders = []
        self.canceled = []
        self.closed = []
        self.fail_once = fail_once

    def submit_order(self, order_data):
        self.orders.append(order_data)
        if self.fail_once is not None:
            error = self.fail_once
            self.fail_once = None
            raise error
        return {
            "id": "order-123",
            "client_order_id": order_data.client_order_id,
            "symbol": order_data.symbol,
            "status": "accepted",
            "side": order_data.side.value,
            "qty": str(order_data.qty) if order_data.qty is not None else None,
            "notional": str(order_data.notional) if order_data.notional is not None else None,
            "filled_qty": "0",
        }

    def get_account(self):
        return {
            "status": "ACTIVE",
            "currency": "USD",
            "equity": "1000.50",
            "cash": "900.25",
            "buying_power": "1800.75",
            "portfolio_value": "1000.50",
        }

    def get_all_positions(self):
        return [
            {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "side": "long",
                "qty": "1.25",
                "avg_entry_price": "100.00",
                "current_price": "101.00",
                "market_value": "126.25",
                "unrealized_pl": "1.25",
                "unrealized_plpc": "0.01",
            }
        ]

    def get_open_position(self, symbol_or_asset_id):
        if symbol_or_asset_id == "MISSING":
            raise _api_error(status=404, message="position not found")
        return self.get_all_positions()[0] | {"symbol": symbol_or_asset_id}

    def get_asset(self, symbol_or_asset_id):
        if symbol_or_asset_id == "MISSING":
            raise _api_error(status=404, message="asset not found")
        return {
            "symbol": symbol_or_asset_id,
            "asset_class": "us_equity",
            "tradable": True,
            "marginable": True,
            "fractionable": True,
            "shortable": True,
            "status": "active",
        }

    def close_position(self, symbol_or_asset_id, close_options=None):
        self.closed.append((symbol_or_asset_id, close_options))
        return {
            "id": "close-123",
            "symbol": symbol_or_asset_id,
            "status": "accepted",
            "side": "sell",
        }

    def cancel_order_by_id(self, order_id):
        self.canceled.append(order_id)
        return None

    def get_orders(self, filter=None):
        return [
            {
                "id": "order-123",
                "symbol": "AAPL",
                "side": "buy",
                "status": "filled",
                "client_order_id": "prime-test",
            }
        ]


class FakeSdkFactory:
    def __init__(self, client: FakeSdkClient) -> None:
        self.client = client
        self.calls = []

    def __call__(self, *, key_id: str, secret: str, paper: bool, url_override: str | None):
        self.calls.append({"key_id": key_id, "secret": secret, "paper": paper, "url_override": url_override})
        return self.client


class _FakeHttpError:
    def __init__(self, status_code: int) -> None:
        self.response = type("Response", (), {"status_code": status_code})()
        self.request = None


def test_sdk_adapter_supports_broker_adapter_protocol_submit_order() -> None:
    client = FakeSdkClient()
    factory = FakeSdkFactory(client)
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=factory)

    assert isinstance(adapter, BrokerAdapter)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id=501,
            symbol="aapl",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=123.456,
            client_order_id="prime-buy-123",
            metadata={"credential_context": _credential_context(environment="paper")},
        )
    )

    assert result.ok is True
    assert result.status == "accepted"
    assert result.client_order_id == "prime-buy-123"
    assert result.requested_notional == 123.456
    assert client.orders[0].client_order_id == "prime-buy-123"
    assert client.orders[0].notional == 123.46
    assert factory.calls[0]["paper"] is True


def test_sdk_adapter_runtime_qty_sell_path_preserves_client_order_id_and_result_shape() -> None:
    client = FakeSdkClient()
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=FakeSdkFactory(client))

    result = adapter.submit_market_order_qty(
        symbol="AAPL",
        side="sell",
        qty=0.000000004,
        client_order_id="prime-cleanup-aapl",
        action="ResidualCleanup",
        credential_context=_credential_context(environment="live"),
    )

    assert result.submitted is True
    assert result.action == "ResidualCleanup"
    assert result.client_order_id == "prime-cleanup-aapl"
    assert result.raw_response is not None
    assert client.orders[0].client_order_id == "prime-cleanup-aapl"
    assert client.orders[0].qty == 0.000000004


def test_sdk_adapter_uses_crypto_compatible_time_in_force_for_crypto_notional_order() -> None:
    client = FakeSdkClient()
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=FakeSdkFactory(client))

    result = adapter.submit_market_order_notional(
        symbol="BTC/USD",
        side="buy",
        notional=1.0,
        client_order_id="sdk-crypto-validation-test",
        action="SdkCryptoPaperValidation",
        credential_context=_credential_context(environment="paper"),
    )

    assert result.submitted is True
    assert client.orders[0].symbol == "BTC/USD"
    assert client.orders[0].time_in_force.value == "gtc"


def test_sdk_adapter_preserves_day_time_in_force_for_stock_notional_order() -> None:
    client = FakeSdkClient()
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=FakeSdkFactory(client))

    result = adapter.submit_market_order_notional(
        symbol="AAPL",
        side="buy",
        notional=1.0,
        client_order_id="sdk-stock-validation-test",
        action="SdkStockValidation",
        credential_context=_credential_context(environment="paper"),
    )

    assert result.submitted is True
    assert client.orders[0].symbol == "AAPL"
    assert client.orders[0].time_in_force.value == "day"


def test_sdk_submit_order_respects_normalized_gtc_time_in_force() -> None:
    client = FakeSdkClient()
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=FakeSdkFactory(client))

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id=501,
            symbol="BTC/USD",
            side="buy",
            order_type="market",
            time_in_force="gtc",
            notional=1.0,
            client_order_id="sdk-submit-order-gtc",
            metadata={"credential_context": _credential_context(environment="paper")},
        )
    )

    assert result.ok is True
    assert client.orders[0].time_in_force.value == "gtc"


def test_sdk_adapter_retries_submit_with_same_client_order_id() -> None:
    client = FakeSdkClient(fail_once=_api_error(status=500, message="server error"))
    sleeps = []
    adapter = AlpacaSdkBrokerAdapter(
        settings=_settings(),
        client_factory=FakeSdkFactory(client),
        retry_policy=AlpacaBrokerRetryPolicy(max_attempts=2, base_delay_seconds=0, jitter_seconds=0),
        sleeper=sleeps.append,
    )

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id=501,
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=100.0,
            client_order_id="same-client-id",
            metadata={"credential_context": _credential_context(environment="paper")},
        )
    )

    assert result.ok is True
    assert len(client.orders) == 2
    assert [order.client_order_id for order in client.orders] == ["same-client-id", "same-client-id"]
    assert sleeps == [0]


def test_sdk_adapter_non_retryable_order_error_maps_to_broker_result() -> None:
    client = FakeSdkClient(fail_once=_api_error(status=422, message="insufficient buying power"))
    adapter = AlpacaSdkBrokerAdapter(
        settings=_settings(),
        client_factory=FakeSdkFactory(client),
        retry_policy=AlpacaBrokerRetryPolicy(max_attempts=3, base_delay_seconds=0, jitter_seconds=0),
    )

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id=501,
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=100.0,
            client_order_id="prime-buy-rejected",
            metadata={"credential_context": _credential_context(environment="paper")},
        )
    )

    assert result.ok is False
    assert result.error_category == "insufficient_buying_power"
    assert result.retryable is False
    assert result.attempt_count == 1
    assert len(client.orders) == 1


def test_sdk_adapter_maps_account_positions_assets_close_and_cancel() -> None:
    client = FakeSdkClient()
    adapter = AlpacaSdkBrokerAdapter(settings=_settings(), client_factory=FakeSdkFactory(client))

    account = adapter.get_account("501")
    positions = adapter.get_positions("501")
    position = adapter.get_position("501", "AAPL")
    missing_position = adapter.get_position("501", "MISSING")
    asset = adapter.get_asset("AAPL")
    missing_asset = adapter.get_asset("MISSING")
    close = adapter.broker_close_position(account_id="501", symbol="AAPL", metadata={"client_order_id": "close-id"})
    cancel = adapter.broker_cancel_order(account_id="501", order_id="order-123", metadata={"client_order_id": "cancel-id"})

    assert account.buying_power == 1800.75
    assert positions[0].symbol == "AAPL"
    assert position is not None and position.qty == 1.25
    assert missing_position is None
    assert asset is not None and asset.tradable is True
    assert missing_asset is None
    assert close.ok is True and close.status == "accepted"
    assert cancel.ok is True and cancel.status == "canceled"


def test_alpaca_transport_factory_defaults_to_rest_and_selects_sdk() -> None:
    rest_settings = _settings()
    sdk_settings = replace(rest_settings, alpaca_transport="sdk")

    assert resolve_alpaca_transport(rest_settings) == "rest"
    assert isinstance(build_alpaca_broker_adapter(rest_settings), AlpacaPaperTradingAdapter)
    assert resolve_alpaca_transport(sdk_settings) == "sdk"
    assert isinstance(build_alpaca_broker_adapter(sdk_settings), AlpacaSdkBrokerAdapter)


def _api_error(*, status: int, message: str) -> APIError:
    return APIError(
        f'{{"code": {status}000, "message": "{message}"}}',
        http_error=_FakeHttpError(status),
    )


def _credential_context(*, environment: str) -> ResolvedAlpacaAccountContext:
    return ResolvedAlpacaAccountContext(
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        broker_connection_id=201,
        broker_credential_id=301,
        environment=environment,
        data_feed="iex",
        access_mode=environment,
        trade_enabled=True,
        key_id="context-key",
        secret="context-secret",
        broker_name="alpaca",
    )


def _settings() -> AppConfig:
    return AppConfig(
        app_name="Bismel1-ex-py",
        environment="test",
        host="127.0.0.1",
        port=8080,
        cloud_run_target=False,
        pine_source_filename="Stocks-pine.pine",
        firestore_project_id=None,
        firestore_database_id="(default)",
        firestore_runtime_collection="runtime_products",
        firestore_product_document="prime_stocks",
        laravel_runtime_bridge_url=None,
        laravel_runtime_bridge_token=None,
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.test",
        alpaca_live_trading_base_url="https://api.alpaca.test",
        alpaca_api_key_id="settings-key",
        alpaca_api_secret="settings-secret",
        alpaca_data_feed="iex",
        gemini_model="gemini-test",
        ai_cache_max_age_minutes=360,
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_ai_validation_bypass_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_test_mode=False,
        prime_stocks_test_trigger=None,
        prime_stocks_test_symbol_override=None,
        prime_stocks_strategy_mode="scalper",
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
        prime_stocks_scheduler_region="us-east1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value=None,
        prime_stocks_ping_scheduler_job_name="prime-stocks-ping",
        prime_stocks_ping_scheduler_schedule="*/1 * * * *",
        prime_stocks_ping_scheduler_timezone="Etc/UTC",
        prime_stocks_ping_scheduler_header_value=None,
    )
