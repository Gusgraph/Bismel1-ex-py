from __future__ import annotations

from app.brokers.models import BrokerPositionState
from app.brokers.reconciliation import BrokerReconciliationExpectation, reconcile_broker_state


def test_reconciliation_submitted_order_becomes_filled() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl", local_order_status="accepted"),
        broker_orders=[_order(status="filled", client_order_id="prime-open-aapl", filled_qty="1.5")],
        broker_position=_position(qty=1.5),
    )

    assert result.status == "filled"
    assert result.order is not None
    assert result.order.status == "filled"
    assert result.action_recommended == "update_local_state"
    assert result.safe_user_message == "Broker order filled."


def test_reconciliation_submitted_order_remains_pending() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl", local_order_status="accepted"),
        broker_orders=[_order(status="accepted", client_order_id="prime-open-aapl")],
        broker_position=None,
    )

    assert result.status == "pending_fill"
    assert result.order is not None
    assert result.order.status == "pending_fill"
    assert result.action_recommended == "wait"


def test_reconciliation_submitted_order_partially_filled() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl"),
        broker_orders=[_order(status="partially_filled", client_order_id="prime-open-aapl", filled_qty="0.75")],
        broker_position=_position(qty=0.75),
    )

    assert result.status == "partially_filled"
    assert result.order is not None
    assert result.order.filled_qty == 0.75
    assert result.safe_user_message == "Broker order partially filled."


def test_reconciliation_submitted_order_rejected() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl"),
        broker_orders=[_order(status="rejected", client_order_id="prime-open-aapl")],
        broker_position=None,
    )

    assert result.status == "rejected"
    assert result.needs_review is True
    assert result.safe_user_message == "Order rejected by broker."


def test_reconciliation_close_filled_and_broker_position_gone() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="take_profit", client_order_id="prime-tp-aapl", local_position_qty=2.0, close_reason="take_profit"),
        broker_orders=[_order(status="filled", client_order_id="prime-tp-aapl", side="sell", filled_qty="2")],
        broker_position=None,
    )

    assert result.status == "in_sync"
    assert result.reason_code == "position_closed"
    assert result.close_reason == "take_profit"
    assert result.to_runtime_metadata()["broker_position_qty"] is None


def test_reconciliation_close_filled_but_residual_qty_remains() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="take_profit", client_order_id="prime-tp-aapl", local_position_qty=2.0),
        broker_orders=[_order(status="filled", client_order_id="prime-tp-aapl", side="sell", filled_qty="2")],
        broker_position=_position(qty=0.000000004, market_value=0.000001),
    )

    assert result.status == "residual_position_remaining"
    assert result.needs_review is True
    assert result.action_recommended == "residual_cleanup"
    assert result.position is not None
    assert result.position.residual_qty == 0.000000004


def test_reconciliation_local_position_exists_but_broker_position_missing() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", local_position_qty=3.0),
        broker_orders=[],
        broker_position=None,
    )

    assert result.status == "broker_position_missing"
    assert result.action_recommended == "update_local_state"
    assert result.safe_user_message == "Position state updated from broker."


def test_reconciliation_broker_position_exists_but_local_state_missing() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", local_position_qty=None),
        broker_orders=[],
        broker_position=_position(qty=3.0),
    )

    assert result.status == "local_position_stale"
    assert result.position is not None
    assert result.position.broker_position_qty == 3.0


def test_reconciliation_broker_read_timeout_needs_review_without_raw_payload() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl"),
        broker_orders=[],
        broker_position=None,
        broker_read_error="broker_api_timeout",
    )

    assert result.status == "broker_read_failed"
    assert result.needs_review is True
    assert result.safe_user_message == "Position requires review."
    assert "raw" not in result.to_runtime_metadata()


def test_reconciliation_404_position_is_broker_position_missing_safely() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", local_position_qty=1.0),
        broker_orders=[],
        broker_position=None,
    )

    assert result.status == "broker_position_missing"
    assert result.reason_code == "broker_position_missing"


def test_reconciliation_duplicate_client_order_id_does_not_recommend_new_order() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="buy", client_order_id="prime-open-aapl"),
        broker_orders=[_order(status="accepted", client_order_id="prime-open-aapl")],
        broker_position=None,
    )

    assert result.status == "pending_fill"
    assert result.action_recommended == "wait"


def test_prime_tp_reconciliation_does_not_allow_non_tp_close_action() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="take_profit", client_order_id="prime-tp-aapl", close_reason="take_profit"),
        broker_orders=[_order(status="filled", client_order_id="prime-tp-aapl", side="sell")],
        broker_position=None,
    )

    assert result.status == "in_sync"
    assert result.action_recommended == "none"
    assert result.close_reason == "take_profit"


def test_execution_stop_loss_reconciliation_preserves_exit_reason() -> None:
    result = reconcile_broker_state(
        expectation=_expectation(expected_action="stop_loss", client_order_id="execution-close-aapl", exit_reason="stop_loss"),
        broker_orders=[_order(status="filled", client_order_id="execution-close-aapl", side="sell")],
        broker_position=None,
    )

    assert result.status == "in_sync"
    assert result.exit_reason == "stop_loss"
    assert result.to_runtime_metadata()["exit_reason"] == "stop_loss"


def _expectation(
    *,
    expected_action: str,
    client_order_id: str | None = None,
    local_order_status: str | None = None,
    local_position_qty: float | None = None,
    close_reason: str | None = None,
    exit_reason: str | None = None,
) -> BrokerReconciliationExpectation:
    return BrokerReconciliationExpectation(
        symbol="AAPL",
        expected_action=expected_action,
        product_key="stocks.bismel1",
        runtime_source="test",
        client_order_id=client_order_id,
        local_order_status=local_order_status,
        local_position_qty=local_position_qty,
        close_reason=close_reason,
        exit_reason=exit_reason,
    )


def _order(
    *,
    status: str,
    client_order_id: str,
    side: str = "buy",
    filled_qty: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": f"order-{client_order_id}",
        "client_order_id": client_order_id,
        "symbol": "AAPL",
        "side": side,
        "status": status,
    }
    if filled_qty is not None:
        payload["filled_qty"] = filled_qty
    return payload


def _position(*, qty: float, market_value: float | None = None) -> BrokerPositionState:
    return BrokerPositionState(
        broker="alpaca",
        account_id="paper-1",
        symbol="AAPL",
        qty=qty,
        market_value=market_value,
    )
