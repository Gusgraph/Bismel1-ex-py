from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.brokers.streaming import (
    BrokerStreamEvent,
    BrokerStreamHealth,
    BrokerStreamMonitor,
    InMemoryBrokerStreamEventSink,
    normalize_broker_stream_message,
)


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
