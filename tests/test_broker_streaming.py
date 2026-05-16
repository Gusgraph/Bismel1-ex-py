from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.brokers.streaming import (
    BrokerStreamContextKey,
    BrokerStreamEvent,
    BrokerStreamHealth,
    BrokerStreamMonitor,
    InMemoryBrokerStreamEventSink,
    SharedBrokerStreamRegistry,
    hashed_broker_account_ref,
    normalize_broker_stream_message,
    should_run_stream_for_context,
    summarize_stream_message,
)
from app.brokers.alpaca_streaming import (
    AlpacaStreamRuntimeScope,
    AlpacaStreamConfigurationError,
    FirestoreBrokerStreamEventSink,
    alpaca_stream_url,
    build_alpaca_auth_message,
    build_alpaca_subscribe_message,
    build_alpaca_stream_transport_from_context,
    redact_alpaca_stream_message,
    write_missing_credentials_health,
)
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext


class _FakeDocument:
    def __init__(self, root: "_FakeFirestoreClient", path: tuple[str, ...]) -> None:
        self._root = root
        self._path = path
        self.payloads: list[dict] = []

    def collection(self, name: str) -> "_FakeRef":
        return _FakeRef(self._root, self._path + (name,))

    def set(self, payload: dict, merge: bool = False) -> None:
        self.payloads.append({"payload": payload, "merge": merge})


class _FakeRef:
    def __init__(self, root: "_FakeFirestoreClient", path: tuple[str, ...] = ()) -> None:
        self._root = root
        self._path = path

    def collection(self, name: str) -> "_FakeRef":
        return _FakeRef(self._root, self._path + (name,))

    def document(self, name: str) -> _FakeDocument:
        path = self._path + (name,)
        return self._root.documents.setdefault(path, _FakeDocument(self._root, path))


class _FakeFirestoreClient:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, ...], _FakeDocument] = {}

    def collection(self, name: str) -> _FakeRef:
        return _FakeRef(self, (name,))


def test_order_filled_event_normalizes_and_marks_reconciliation_needed() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={
            "stream": "trade_updates",
            "data": {
                "event": "fill",
                "timestamp": "2026-05-16T14:30:00Z",
                "qty": "2",
                "price": "101.25",
                "order": {
                    "id": "broker-order-1",
                    "client_order_id": "prime-tp-aapl",
                    "symbol": "AAPL",
                    "side": "sell",
                    "status": "filled",
                    "qty": "2",
                    "filled_qty": "2",
                },
            },
        },
    )

    assert event.event_type == "order_filled"
    assert event.order_status == "filled"
    assert event.symbol == "AAPL"
    assert event.side == "sell"
    assert event.filled_qty == 2.0
    assert event.remaining_qty == 0.0
    assert event.avg_fill_price == 101.25
    assert event.reconciliation_needed is True
    assert event.safe_user_message == "Broker order filled."


def test_shared_stream_context_key_is_scoped_and_sanitized() -> None:
    account_hash = hashed_broker_account_ref("credential-1", "alpaca-account-1", "paper")
    key = BrokerStreamContextKey(
        broker_provider="alpaca",
        environment="paper",
        broker_account_hash=account_hash,
        product_code="prime_stocks",
        slot_number=1,
    )
    metadata = key.to_safe_metadata()

    assert key.key == f"alpaca:paper:{account_hash}:prime_stocks:slot_1"
    assert metadata["shared"] is True
    assert metadata["scope"] == "broker_account_product_slot"
    assert "credential-1" not in key.key
    assert "alpaca-account-1" not in key.key


def test_shared_stream_registry_prevents_duplicate_contexts() -> None:
    registry = SharedBrokerStreamRegistry()
    key = BrokerStreamContextKey(
        broker_provider="alpaca",
        environment="paper",
        broker_account_hash="accthash",
        product_code="execution",
        slot_number=1,
    )

    assert registry.acquire(key) is True
    assert registry.acquire(key) is False
    assert registry.is_active(key) is True
    registry.release(key)
    assert registry.acquire(key) is True


def test_market_hours_stream_policy_keeps_stock_stream_cost_bounded() -> None:
    assert should_run_stream_for_context(
        product_code="prime_stocks",
        asset_class="stock",
        market_open=True,
    ) == (True, "market_hours_active")
    assert should_run_stream_for_context(
        product_code="prime_stocks",
        asset_class="stock",
        market_open=False,
    ) == (False, "market_closed")
    assert should_run_stream_for_context(
        product_code="prime_stocks",
        asset_class="crypto",
        admin_monitor=True,
        market_open=False,
    ) == (True, "admin_crypto_24_7")


def test_partial_fill_event_normalizes() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={
            "event": "partial_fill",
            "qty": "0.75",
            "price": "100.50",
            "order": {
                "client_order_id": "execution-buy-aapl",
                "symbol": "AAPL",
                "side": "buy",
                "status": "partially_filled",
                "qty": "1.5",
                "filled_qty": "0.75",
            },
        },
    )

    assert event.event_type == "order_partially_filled"
    assert event.filled_qty == 0.75
    assert event.remaining_qty == 0.75
    assert event.reconciliation_needed is True


def test_rejected_event_normalizes() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "rejected", "order": {"symbol": "AAPL", "side": "buy", "client_order_id": "prime-open-aapl"}},
    )

    assert event.event_type == "order_rejected"
    assert event.order_status == "rejected"
    assert event.reconciliation_needed is True
    assert event.safe_user_message == "Order rejected by broker."


def test_canceled_event_normalizes() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "canceled", "order": {"symbol": "AAPL", "side": "buy", "client_order_id": "execution-cancel-aapl"}},
    )

    assert event.event_type == "order_canceled"
    assert event.order_status == "canceled"
    assert event.reconciliation_needed is True


def test_alpaca_array_wrapped_message_normalizes() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message=[
            {
                "stream": "trade_updates",
                "data": {
                    "event": "new",
                    "order": {"symbol": "AAPL", "side": "buy", "status": "new"},
                },
            }
        ],
    )

    assert event.event_type == "order_new"
    assert event.symbol == "AAPL"
    assert event.order_status == "new"
    assert event.reconciliation_needed is False


def test_alpaca_pending_and_cancel_rejected_events_normalize() -> None:
    pending_new = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "pending_new", "order": {"symbol": "AAPL", "status": "pending_new"}},
    )
    pending_cancel = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "pending_cancel", "order": {"symbol": "AAPL", "status": "pending_cancel"}},
    )
    cancel_rejected = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "order_cancel_rejected", "order": {"symbol": "AAPL"}},
    )

    assert pending_new.event_type == "order_pending_new"
    assert pending_new.reconciliation_needed is False
    assert pending_cancel.event_type == "order_pending_cancel"
    assert pending_cancel.reconciliation_needed is False
    assert cancel_rejected.event_type == "order_cancel_rejected"
    assert cancel_rejected.reconciliation_needed is True


def test_unknown_event_becomes_unknown_event() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message={"event": "mystery", "order": {"symbol": "AAPL"}},
    )

    assert event.event_type == "unknown_event"
    assert event.reason_code == "unknown_event"
    assert event.safe_user_message == "Broker stream event requires review."


def test_malformed_json_becomes_parse_error() -> None:
    event = normalize_broker_stream_message(
        broker="alpaca",
        account_ref="acct-internal",
        message="{bad json",
    )

    assert event.event_type == "parse_error"
    assert event.reason_code == "stream_parse_error"
    assert event.safe_user_message == "Broker stream message could not be read."


def test_auth_failure_becomes_stream_auth_failed() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    event = monitor.mark_auth_failed()

    assert event.event_type == "stream_auth_failed"
    assert sink.health[-1].status == "stream_auth_failed"
    assert event.safe_user_message == "Broker stream authentication failed."


def test_network_disconnect_schedules_reconnect() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    event = monitor.handle_disconnect(ConnectionError("network down"))

    assert event.event_type == "stream_reconnect_scheduled"
    assert sink.health[-1].status == "stream_reconnect_scheduled"
    assert sink.health[-1].reconnect_attempt == 1


def test_fill_event_marks_reconciliation_needed_in_sink() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    event = monitor.handle_message({"event": "fill", "order": {"symbol": "AAPL", "client_order_id": "prime-tp-aapl"}})

    assert event.reconciliation_needed is True
    assert sink.events[-1].event_type == "order_filled"
    assert sink.health[-1].status == "stream_connected"


def test_customer_payload_does_not_include_raw_or_internal_broker_ids() -> None:
    event = BrokerStreamEvent(
        broker="alpaca",
        account_ref="acct-internal",
        event_type="order_filled",
        symbol="AAPL",
        client_order_id="prime-tp-secret",
        broker_order_id="broker-order-secret",
        safe_user_message="Broker order filled.",
    )

    payload = event.to_customer_payload()

    assert "account_ref" not in payload
    assert "client_order_id" not in payload
    assert "broker_order_id" not in payload
    assert "raw_payload" not in payload
    assert payload["safe_user_message"] == "Broker order filled."


def test_stream_event_does_not_submit_or_cancel_orders() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    event = monitor.handle_message({"event": "fill", "order": {"symbol": "AAPL", "client_order_id": "prime-tp-aapl"}})

    assert not hasattr(event, "submit_order")
    assert not hasattr(event, "cancel_order")
    assert len(sink.events) == 1


def test_stream_health_detects_stale_connected_stream() -> None:
    health = BrokerStreamHealth(
        broker="alpaca",
        status="stream_connected",
        last_event_at=datetime.now(UTC) - timedelta(seconds=300),
        stale_after_seconds=120,
    )

    metadata = health.to_runtime_metadata()

    assert metadata["stream_status"] == "stream_stale"
    assert metadata["reason_code"] == "stream_stale"


def test_planned_validation_window_marks_idle_connected_not_disconnected() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    event = monitor.mark_idle_connected()

    assert event.event_type == "stream_idle_connected"
    assert sink.health[-1].status == "stream_idle_connected"
    assert sink.health[-1].safe_user_message == (
        "Broker stream connected. No trade update was received during the validation window."
    )


def test_stream_monitor_tracks_sanitized_diagnostics() -> None:
    sink = InMemoryBrokerStreamEventSink()
    monitor = BrokerStreamMonitor(broker="alpaca", account_ref="acct-internal", sink=sink)

    monitor.mark_auth_acknowledged()
    monitor.mark_subscribed({"stream": "authorization", "data": {"streams": ["trade_updates"]}})
    event = monitor.handle_message([
        {
            "stream": "trade_updates",
            "data": {"event": "canceled", "order": {"symbol": "AAPL", "status": "canceled"}},
        }
    ])

    diagnostics = sink.health[-1].to_runtime_metadata()["diagnostics"]
    rendered = str(diagnostics).lower()
    assert event.event_type == "order_canceled"
    assert monitor.diagnostics == diagnostics
    assert diagnostics["auth_acknowledged"] is True
    assert diagnostics["subscription_acknowledged"] is True
    assert diagnostics["message_count"] == 1
    assert diagnostics["event_count"] == 1
    assert diagnostics["last_event_type"] == "order_canceled"
    assert "client_order_id" not in rendered
    assert "broker_order" not in rendered


def test_summarize_stream_message_excludes_raw_payload_and_ids() -> None:
    summary = summarize_stream_message(
        {
            "stream": "trade_updates",
            "data": {
                "event": "fill",
                "order": {
                    "id": "broker-order-secret",
                    "client_order_id": "client-secret",
                    "symbol": "AAPL",
                    "status": "filled",
                },
            },
        }
    )

    rendered = str(summary)
    assert summary["top_level_keys"] == ["data", "stream"]
    assert summary["event"] == "fill"
    assert summary["stream"] == "trade_updates"
    assert "broker-order-secret" not in rendered
    assert "client-secret" not in rendered


def test_alpaca_stream_builds_auth_and_subscribe_messages_without_logging_secret() -> None:
    auth_message = build_alpaca_auth_message(key_id="PKSECRETKEY", secret="super-secret")
    redacted = redact_alpaca_stream_message(auth_message)
    subscribe_message = build_alpaca_subscribe_message()

    assert alpaca_stream_url("paper") == "wss://paper-api.alpaca.markets/stream"
    assert alpaca_stream_url("live") == "wss://api.alpaca.markets/stream"
    assert auth_message["key"] == "PKSECRETKEY"
    assert auth_message["secret"] == "super-secret"
    assert redacted["key"] == "***"
    assert redacted["secret"] == "***"
    assert subscribe_message == {"action": "listen", "data": {"streams": ["trade_updates"]}}


def test_firestore_stream_sink_writes_only_sanitized_public_state() -> None:
    client = _FakeFirestoreClient()
    scope = AlpacaStreamRuntimeScope(
        uid="uid-1",
        account_id=73,
        product_id="prime_stocks",
        slot_number=2,
        account_ref="hashed-account-ref",
    )
    sink = FirestoreBrokerStreamEventSink(firestore_client=client, scope=scope)
    event = BrokerStreamEvent(
        broker="alpaca",
        account_ref="raw-account-id",
        event_type="order_filled",
        symbol="AAPL",
        order_status="filled",
        client_order_id="prime-tp-secret",
        broker_order_id="broker-order-secret",
        safe_user_message="Broker order filled.",
        reconciliation_needed=True,
    )

    sink.write_stream_event(event)

    path = (
        "users",
        "uid-1",
        "accounts",
        "73",
        "prime_stocks",
        "current",
        "slots",
        "slot_2",
        "broker_stream",
        "current",
    )
    payload = client.documents[path].payloads[-1]["payload"]
    assert payload["event_type"] == "order_filled"
    assert payload["safe_user_message"] == "Broker order filled."
    assert payload["reconciliation_needed"] is True
    assert payload["account_ref"] == "hashed-account-ref"
    assert payload["stream_context"]["shared"] is True
    assert payload["stream_context"]["product_code"] == "prime_stocks"
    assert payload["stream_context"]["slot_number"] == 2
    assert payload["stream_context"]["key"].startswith("alpaca:paper:")
    assert "client_order_id" not in payload
    assert "broker_order_id" not in payload
    assert "raw_payload" not in payload


def test_alpaca_stream_transport_uses_resolved_rest_credential_context() -> None:
    sink = InMemoryBrokerStreamEventSink()
    context = ResolvedAlpacaAccountContext(
        uid="uid-1",
        account_id=73,
        alpaca_account_id=11,
        broker_connection_id=201,
        broker_credential_id=401,
        environment="paper",
        data_feed="iex",
        access_mode="trade",
        trade_enabled=True,
        key_id="paper-key-from-rest-context",
        secret="paper-secret-from-rest-context",
        slot_number=2,
        product_id="prime_stocks",
    )

    transport = build_alpaca_stream_transport_from_context(
        account_context=context,
        sink=sink,
        account_ref="safe-account-ref",
        max_messages=1,
    )

    assert transport._url == "wss://paper-api.alpaca.markets/stream"
    assert transport._key_id == "paper-key-from-rest-context"
    assert transport._secret == "paper-secret-from-rest-context"


def test_alpaca_stream_missing_resolved_credentials_writes_safe_health() -> None:
    sink = InMemoryBrokerStreamEventSink()
    context = ResolvedAlpacaAccountContext(
        uid="uid-1",
        account_id=73,
        alpaca_account_id=11,
        broker_connection_id=201,
        broker_credential_id=401,
        environment="paper",
        data_feed="iex",
        access_mode="trade",
        trade_enabled=True,
        key_id="",
        secret="",
        slot_number=2,
        product_id="prime_stocks",
    )

    try:
        build_alpaca_stream_transport_from_context(account_context=context, sink=sink)
    except AlpacaStreamConfigurationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("Expected missing credentials to block stream transport startup.")

    assert sink.health[-1].status == "broker_stream_credentials_missing"
    assert sink.health[-1].reason_code == "broker_stream_credentials_missing"
    assert sink.health[-1].safe_user_message == "Broker stream credentials are missing for this linked account."


def test_missing_credentials_health_does_not_expose_credential_material() -> None:
    sink = InMemoryBrokerStreamEventSink()

    write_missing_credentials_health(sink=sink, account_ref="safe-ref")

    payload = sink.health[-1].to_runtime_metadata()
    rendered = str(payload)
    assert payload["stream_status"] == "broker_stream_credentials_missing"
    assert "secret" not in rendered.lower()
    assert "api" not in rendered.lower()
