from __future__ import annotations

from io import BytesIO
from socket import timeout as SocketTimeout
import pytest
from urllib.error import HTTPError

from app.brokers.contracts import BrokerAdapter
from app.brokers.alpaca_paper_trading import AlpacaBrokerRetryPolicy, AlpacaPaperTradingAdapter
from app.brokers.models import BrokerOrderRequest, BrokerOrderResult
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext


def test_broker_order_request_validates_market_order_shape() -> None:
    request = BrokerOrderRequest(
        account_id=501,
        symbol=" aapl ",
        side="BUY",
        order_type="market",
        time_in_force="day",
        notional=120.0,
        client_order_id="prime-firstlot-test",
        product_key="stocks.bismel1",
        execution_mode="paper",
    )

    assert request.symbol == "AAPL"
    assert request.side == "buy"
    assert request.notional == 120.0

    with pytest.raises(ValueError, match="exactly one of qty or notional"):
        BrokerOrderRequest(
            account_id=501,
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            qty=1.0,
            notional=100.0,
            client_order_id="bad-request",
        )

    with pytest.raises(ValueError, match="client_order_id"):
        BrokerOrderRequest(
            account_id=501,
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=100.0,
            client_order_id=" ",
        )


def test_alpaca_adapter_converts_qty_order_through_internal_broker_request_model() -> None:
    http = FakeHttpClient(
        {
            "id": "order-1",
            "client_order_id": "execution-close-aapl",
            "symbol": "AAPL",
            "side": "sell",
            "status": "accepted",
            "qty": "1.123456789",
            "notional": None,
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    result = adapter.submit_market_order_qty(
        symbol="aapl",
        side="SELL",
        qty=1.123456789,
        client_order_id="execution-close-aapl",
        action="strategy_exit",
    )

    assert result.submitted is True
    assert result.order_id == "order-1"
    assert result.client_order_id == "execution-close-aapl"
    assert result.side == "sell"
    assert http.calls[0]["payload"] == {
        "symbol": "AAPL",
        "side": "sell",
        "type": "market",
        "time_in_force": "day",
        "qty": "1.123456789",
        "client_order_id": "execution-close-aapl",
    }


def test_alpaca_runtime_qty_sell_path_preserves_raw_response_and_live_context() -> None:
    http = FakeHttpClient(
        {
            "id": "close-order-live",
            "client_order_id": "prime-tp-live-aapl",
            "symbol": "AAPL",
            "side": "sell",
            "status": "accepted",
            "qty": "0.000000004",
            "filled_qty": "0",
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    result = adapter.submit_market_order_qty(
        symbol="aapl",
        side="sell",
        qty=0.000000004,
        client_order_id="prime-tp-live-aapl",
        action="take_profit",
        credential_context=_account_context(environment="live"),
    )

    assert result.submitted is True
    assert result.action == "take_profit"
    assert result.order_status == "accepted"
    assert result.order_id == "close-order-live"
    assert result.client_order_id == "prime-tp-live-aapl"
    assert result.side == "sell"
    assert result.raw_response == {
        "id": "close-order-live",
        "client_order_id": "prime-tp-live-aapl",
        "symbol": "AAPL",
        "side": "sell",
        "status": "accepted",
        "qty": "0.000000004",
        "filled_qty": "0",
    }
    assert http.calls[0]["url"].startswith("https://api.alpaca.test")
    assert http.calls[0]["headers"]["APCA-API-KEY-ID"] == "live-key"
    assert http.calls[0]["payload"] == {
        "symbol": "AAPL",
        "side": "sell",
        "type": "market",
        "time_in_force": "day",
        "qty": "0.000000004",
        "client_order_id": "prime-tp-live-aapl",
    }


def test_alpaca_runtime_qty_sell_path_preserves_normalized_broker_error_behavior() -> None:
    http = FakeHttpClient(_http_error(code=422, message="invalid order qty"))
    adapter = _adapter(http)

    result = adapter.submit_market_order_qty(
        symbol="AAPL",
        side="sell",
        qty=1.0,
        client_order_id="execution-close-invalid",
        action="strategy_exit",
    )

    assert result.submitted is False
    assert result.broker_error_code == "broker_invalid_order"
    assert result.broker_error_message == "invalid order qty"
    assert result.raw_response == {"message": "invalid order qty"}
    assert http.calls[0]["payload"]["client_order_id"] == "execution-close-invalid"


def test_submit_order_timeout_retries_same_client_order_id() -> None:
    http = FakeHttpClient([
        SocketTimeout("timed out"),
        {
            "id": "order-after-timeout",
            "client_order_id": "execution-buy-timeout",
            "symbol": "AAPL",
            "side": "buy",
            "status": "accepted",
            "notional": "120.00",
        },
    ])
    adapter = _adapter(http)

    result = adapter.submit_market_order_notional(
        symbol="AAPL",
        side="buy",
        notional=120.0,
        client_order_id="execution-buy-timeout",
        action="buy",
    )

    assert result.submitted is True
    assert result.client_order_id == "execution-buy-timeout"
    assert len(http.calls) == 2
    assert {call["payload"]["client_order_id"] for call in http.calls} == {"execution-buy-timeout"}


def test_submit_order_server_error_retries_then_succeeds() -> None:
    http = FakeHttpClient([
        _http_error(code=500, message="temporary broker outage"),
        {
            "id": "order-after-500",
            "client_order_id": "prime-firstlot-retry",
            "symbol": "AAPL",
            "side": "buy",
            "status": "accepted",
            "notional": "120.00",
        },
    ])
    adapter = _adapter(http)

    result = adapter.submit_market_order_notional(
        symbol="AAPL",
        side="buy",
        notional=120.0,
        client_order_id="prime-firstlot-retry",
        action="FirstLot",
    )

    assert result.submitted is True
    assert result.order_id == "order-after-500"
    assert len(http.calls) == 2


def test_submit_order_rate_limit_classifies_retryable_failure_after_attempts() -> None:
    http = FakeHttpClient([
        _http_error(code=429, message="rate limit exceeded"),
        _http_error(code=429, message="rate limit exceeded"),
        _http_error(code=429, message="rate limit exceeded"),
    ])
    adapter = _adapter(http)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id="paper-1",
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=120.0,
            client_order_id="prime-rate-limited",
        )
    )

    assert result.ok is False
    assert result.error_code == "broker_rate_limited"
    assert result.error_category == "retryable_rate_limit"
    assert result.retryable is True
    assert result.attempt_count == 3
    assert len(http.calls) == 3


def test_submit_order_invalid_order_does_not_retry() -> None:
    http = FakeHttpClient(_http_error(code=422, message="invalid order qty"))
    adapter = _adapter(http)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id="paper-1",
            symbol="AAPL",
            side="sell",
            order_type="market",
            time_in_force="day",
            qty=1.0,
            client_order_id="execution-invalid-no-retry",
        )
    )

    assert result.ok is False
    assert result.error_category == "invalid_order_request"
    assert result.retryable is False
    assert result.attempt_count == 1
    assert len(http.calls) == 1


def test_submit_order_insufficient_buying_power_does_not_retry() -> None:
    http = FakeHttpClient(_http_error(code=422, message="insufficient buying power"))
    adapter = _adapter(http)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id="paper-1",
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=100000.0,
            client_order_id="prime-insufficient-buying-power",
        )
    )

    assert result.ok is False
    assert result.error_code == "broker_insufficient_buying_power"
    assert result.error_category == "insufficient_buying_power"
    assert len(http.calls) == 1


def test_submit_order_duplicate_client_order_id_is_classified_without_retry() -> None:
    http = FakeHttpClient(_http_error(code=422, message="duplicate client_order_id"))
    adapter = _adapter(http)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id="paper-1",
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=120.0,
            client_order_id="prime-duplicate-client-order-id",
        )
    )

    assert result.ok is False
    assert result.error_code == "broker_duplicate_client_order_id"
    assert result.error_category == "duplicate_client_order_id"
    assert result.retryable is False
    assert len(http.calls) == 1


def test_get_position_404_returns_none_without_retry() -> None:
    http = FakeHttpClient(_http_error(code=404, message="position not found"))
    adapter = _adapter(http)

    assert adapter.get_position("paper-1", "AAPL") is None
    assert len(http.calls) == 1


def test_get_asset_not_tradable_classifies_rejected_asset() -> None:
    http = FakeHttpClient(_http_error(code=422, message="asset is not tradable"))
    adapter = _adapter(http)

    with pytest.raises(Exception) as exc:
        adapter.get_asset("AAPL")

    assert getattr(exc.value, "code") == "broker_asset_not_tradable"
    assert getattr(exc.value, "error_category") == "asset_not_tradable"
    assert len(http.calls) == 1


def test_cancel_order_404_does_not_retry() -> None:
    http = FakeHttpClient(_http_error(code=404, message="order not found"))
    adapter = _adapter(http)

    with pytest.raises(Exception) as exc:
        adapter.cancel_order("paper-1", "missing-order", {"client_order_id": "broker-cancel-missing"})

    assert getattr(exc.value, "http_status") == 404
    assert len(http.calls) == 1


def test_json_parse_error_returns_normalized_submit_result_without_retry() -> None:
    http = FakeHttpClient(ValueError("bad json"))
    adapter = _adapter(http)

    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id="paper-1",
            symbol="AAPL",
            side="buy",
            order_type="market",
            time_in_force="day",
            notional=120.0,
            client_order_id="prime-parse-error",
        )
    )

    assert result.ok is False
    assert result.error_code == "broker_parse_error"
    assert result.error_category == "parse_error"
    assert result.retryable is False
    assert len(http.calls) == 1


def test_alpaca_adapter_supports_broker_adapter_protocol_submit_order() -> None:
    http = FakeHttpClient(
        {
            "id": "order-protocol",
            "client_order_id": "prime-firstlot-protocol",
            "symbol": "AAPL",
            "side": "buy",
            "status": "accepted",
            "notional": "120.00",
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)
    request = BrokerOrderRequest(
        account_id="paper-1",
        symbol="AAPL",
        side="buy",
        order_type="market",
        time_in_force="day",
        notional=120.0,
        client_order_id="prime-firstlot-protocol",
    )

    assert isinstance(adapter, BrokerAdapter)
    result = adapter.submit_order(request)

    assert isinstance(result, BrokerOrderResult)
    assert result.ok is True
    assert result.broker == "alpaca"
    assert result.account_id == "paper-1"
    assert result.order_id == "order-protocol"
    assert result.client_order_id == "prime-firstlot-protocol"
    assert http.calls[0]["payload"]["client_order_id"] == "prime-firstlot-protocol"


def test_alpaca_adapter_supports_broker_adapter_protocol_state_methods() -> None:
    http = FakeHttpClient(
        {
            "/v2/account": {
                "status": "ACTIVE",
                "currency": "USD",
                "buying_power": "1000.50",
                "equity": "2500.25",
                "cash": "700.00",
                "portfolio_value": "2500.25",
            },
            "/v2/positions": [
                {"symbol": "AAPL", "qty": "1.123456789", "avg_entry_price": "100.00", "market_value": "123.45"},
            ],
            "/v2/positions/AAPL": {
                "symbol": "AAPL",
                "qty": "1.123456789",
                "avg_entry_price": "100.00",
                "market_value": "123.45",
            },
            "/v2/assets/AAPL": {
                "symbol": "AAPL",
                "class": "us_equity",
                "tradable": True,
                "status": "active",
            },
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    account = adapter.get_account("paper-1")
    positions = adapter.get_positions("paper-1")
    position = adapter.get_position("paper-1", "AAPL")
    asset = adapter.get_asset("AAPL")
    health = adapter.health_check("paper-1")

    assert account.broker == "alpaca"
    assert account.buying_power == 1000.50
    assert len(positions) == 1
    assert position is not None
    assert position.qty == 1.123456789
    assert asset is not None
    assert asset.tradable is True
    assert health["ok"] is True


def test_alpaca_adapter_protocol_close_and_cancel_preserve_raw_response() -> None:
    http = FakeHttpClient(
        {
            "/v2/positions/AAPL": {
                "id": "close-order",
                "symbol": "AAPL",
                "side": "sell",
                "status": "accepted",
            },
            "/v2/orders/order-1": {
                "id": "order-1",
                "side": "buy",
                "status": "canceled",
            },
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    close_result = adapter.close_position("paper-1", "AAPL", {"client_order_id": "broker-close-aapl"})
    cancel_result = adapter.cancel_order("paper-1", "order-1", {"client_order_id": "broker-cancel-order-1"})

    assert close_result.ok is True
    assert close_result.client_order_id == "broker-close-aapl"
    assert close_result.raw_response == {"id": "close-order", "symbol": "AAPL", "side": "sell", "status": "accepted"}
    assert cancel_result.ok is True
    assert cancel_result.status == "canceled"
    assert cancel_result.raw_response == {"id": "order-1", "side": "buy", "status": "canceled"}


def test_alpaca_adapter_converts_notional_order_through_internal_broker_request_model() -> None:
    http = FakeHttpClient(
        {
            "id": "order-2",
            "client_order_id": "prime-firstlot-aapl",
            "symbol": "AAPL",
            "side": "buy",
            "status": "accepted",
            "notional": "120.00",
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    result = adapter.submit_market_order_notional(
        symbol="AAPL",
        side="buy",
        notional=120.004,
        client_order_id="prime-firstlot-aapl",
        action="FirstLot",
    )

    assert result.submitted is True
    assert result.notional == 120.0
    assert http.calls[0]["payload"] == {
        "symbol": "AAPL",
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "notional": 120.0,
        "client_order_id": "prime-firstlot-aapl",
    }


def test_alpaca_runtime_notional_path_uses_protocol_submit_and_preserves_runtime_result() -> None:
    http = FakeHttpClient(
        {
            "id": "order-runtime",
            "client_order_id": "execution-buy-via-runtime",
            "symbol": "AAPL",
            "side": "buy",
            "status": "accepted",
            "notional": "99.99",
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    result = adapter.submit_market_order_notional(
        symbol="aapl",
        side="buy",
        notional=99.991,
        client_order_id="execution-buy-via-runtime",
        action="buy",
    )

    assert result.submitted is True
    assert result.action == "buy"
    assert result.order_status == "accepted"
    assert result.order_id == "order-runtime"
    assert result.client_order_id == "execution-buy-via-runtime"
    assert result.side == "buy"
    assert result.notional == 99.99
    assert result.raw_response == {
        "id": "order-runtime",
        "client_order_id": "execution-buy-via-runtime",
        "symbol": "AAPL",
        "side": "buy",
        "status": "accepted",
        "notional": "99.99",
    }
    assert http.calls[0]["payload"] == {
        "symbol": "AAPL",
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "notional": 99.99,
        "client_order_id": "execution-buy-via-runtime",
    }


def test_alpaca_adapter_normalizes_account_asset_and_position_state_without_changing_public_shape() -> None:
    http = FakeHttpClient(
        {
            "/v2/account": {
                "status": "ACTIVE",
                "currency": "USD",
                "buying_power": "1000.50",
                "equity": "2500.25",
                "cash": "700.00",
                "portfolio_value": "2500.25",
            },
            "/v2/positions": [
                {"symbol": "AAPL", "market_value": "300.00"},
                {"symbol": "MSFT", "market_value": "200.00"},
            ],
            "/v2/assets/AAPL": {
                "symbol": "AAPL",
                "class": "us_equity",
                "tradable": True,
                "fractionable": True,
                "status": "active",
            },
            "/v2/positions/AAPL": {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "qty": "1.123456789",
                "avg_entry_price": "100.00",
                "market_value": "123.45",
            },
        }
    )
    adapter = AlpacaPaperTradingAdapter(settings=_settings(), http_client=http)

    state = adapter.get_submission_state(symbol="AAPL")

    assert state.account.buying_power == 1000.50
    assert state.account.equity == 2500.25
    assert state.account.open_positions_count == 2
    assert state.account.total_exposure == 500.0
    assert state.asset.symbol == "AAPL"
    assert state.asset.tradable is True
    assert state.position is not None
    assert state.position.qty == 1.123456789
    assert state.position.avg_entry_price == 100.0


class FakeHttpClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def request_json(self, *, url: str, method: str, headers: dict[str, str], payload=None):
        self.calls.append({"url": url, "method": method, "headers": headers, "payload": payload})
        if isinstance(self.payload, list):
            response = self.payload.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        if isinstance(self.payload, Exception):
            raise self.payload
        if isinstance(self.payload, dict):
            for path, response in self.payload.items():
                if isinstance(path, str) and url.endswith(path):
                    return response
        return self.payload


def _account_context(*, environment: str = "paper") -> ResolvedAlpacaAccountContext:
    return ResolvedAlpacaAccountContext(
        uid="user-test",
        account_id=101,
        alpaca_account_id=501,
        broker_connection_id=301,
        broker_credential_id=401,
        environment=environment,
        data_feed="iex",
        access_mode="trade",
        trade_enabled=True,
        key_id=f"{environment}-key",
        secret=f"{environment}-secret",
        slot_number=1,
        entitlement={"runtime_allowed": True},
        product_id="prime_stocks",
        broker_name="alpaca",
    )


def _adapter(http: FakeHttpClient) -> AlpacaPaperTradingAdapter:
    return AlpacaPaperTradingAdapter(
        settings=_settings(),
        http_client=http,
        retry_policy=AlpacaBrokerRetryPolicy(max_attempts=3, base_delay_seconds=0, max_delay_seconds=0, jitter_seconds=0),
        sleeper=lambda _: None,
    )


def _http_error(*, code: int, message: str) -> HTTPError:
    return HTTPError(
        url="https://paper-api.alpaca.test/v2/orders",
        code=code,
        msg="Broker Error",
        hdrs=None,
        fp=BytesIO(json_bytes({"message": message})),
    )


def json_bytes(payload: dict[str, object]) -> bytes:
    import json

    return json.dumps(payload).encode("utf-8")


def _settings():
    class Settings:
        alpaca_api_key_id = "key"
        alpaca_api_secret = "secret"
        alpaca_trading_base_url = "https://paper-api.alpaca.test"
        alpaca_live_trading_base_url = "https://api.alpaca.test"

    return Settings()
