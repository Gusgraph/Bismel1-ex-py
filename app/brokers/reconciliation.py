# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/reconciliation.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping

from app.brokers.models import BrokerOrderResult, BrokerPositionState


RECONCILIATION_TERMINAL_ORDER_STATUSES = frozenset({"filled", "rejected", "canceled", "cancelled", "expired"})
RECONCILIATION_PENDING_ORDER_STATUSES = frozenset({"accepted", "new", "submitted", "pending_new", "pending_replace", "pending_cancel"})
RECONCILIATION_PARTIAL_ORDER_STATUSES = frozenset({"partially_filled", "partial_fill"})
RESIDUAL_QTY_THRESHOLD = 0.000001
RESIDUAL_VALUE_THRESHOLD = 0.01


@dataclass(frozen=True)
class BrokerOrderReconciliationState:
    status: str
    broker_status: str | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    filled_qty: float | None = None
    filled_avg_price: float | None = None
    reason_code: str | None = None
    safe_user_message: str = "Position requires review."


@dataclass(frozen=True)
class BrokerPositionReconciliationState:
    status: str
    broker_position_qty: float | None = None
    local_position_qty: float | None = None
    residual_qty: float | None = None
    reason_code: str | None = None
    safe_user_message: str = "Position requires review."


@dataclass(frozen=True)
class BrokerReconciliationResult:
    status: str
    symbol: str
    expected_action: str | None = None
    product_key: str | None = None
    runtime_source: str | None = None
    close_reason: str | None = None
    exit_reason: str | None = None
    order: BrokerOrderReconciliationState | None = None
    position: BrokerPositionReconciliationState | None = None
    reason_code: str | None = None
    action_recommended: str = "none"
    needs_review: bool = False
    safe_user_message: str = "Position state updated from broker."
    last_reconciled_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_runtime_metadata(self) -> dict[str, Any]:
        return {
            "reconciliation_status": self.status,
            "reconciliation_reason": self.reason_code,
            "last_reconciled_at": self.last_reconciled_at.isoformat(),
            "broker_order_status": None if self.order is None else self.order.broker_status,
            "broker_position_qty": None if self.position is None else self.position.broker_position_qty,
            "local_position_qty": None if self.position is None else self.position.local_position_qty,
            "residual_qty": None if self.position is None else self.position.residual_qty,
            "needs_review": self.needs_review,
            "close_reason": self.close_reason,
            "exit_reason": self.exit_reason,
        }


@dataclass(frozen=True)
class BrokerReconciliationExpectation:
    symbol: str
    expected_action: str | None = None
    product_key: str | None = None
    runtime_source: str | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    local_order_status: str | None = None
    local_position_qty: float | None = None
    close_reason: str | None = None
    exit_reason: str | None = None


def reconcile_broker_state(
    *,
    expectation: BrokerReconciliationExpectation,
    broker_orders: Iterable[Mapping[str, Any] | BrokerOrderResult],
    broker_position: BrokerPositionState | Mapping[str, Any] | None,
    broker_read_error: str | None = None,
) -> BrokerReconciliationResult:
    symbol = expectation.symbol.strip().upper()
    if broker_read_error is not None:
        return _result(
            expectation=expectation,
            status="broker_read_failed",
            reason_code="broker_read_failed",
            needs_review=True,
            action_recommended="review",
            safe_user_message="Position requires review.",
        )

    order_payload = _find_matching_order(
        broker_orders=broker_orders,
        client_order_id=expectation.client_order_id,
        order_id=expectation.order_id,
        symbol=symbol,
    )
    position_qty = _position_qty(broker_position)
    local_qty = expectation.local_position_qty
    expected_action = _normalize_action(expectation.expected_action)
    order_state = _order_state(order_payload)
    position_state = _position_state(
        symbol=symbol,
        expected_action=expected_action,
        broker_position_qty=position_qty,
        local_position_qty=local_qty,
    )

    if order_state is not None:
        if order_state.status in {"rejected", "canceled"}:
            return _result(
                expectation=expectation,
                status=order_state.status,
                order=order_state,
                position=position_state,
                reason_code=order_state.reason_code,
                needs_review=True,
                action_recommended="review",
                safe_user_message=order_state.safe_user_message,
            )
        if order_state.status in {"pending_fill", "partially_filled"}:
            return _result(
                expectation=expectation,
                status=order_state.status,
                order=order_state,
                position=position_state,
                reason_code=order_state.reason_code,
                needs_review=False,
                action_recommended="wait",
                safe_user_message=order_state.safe_user_message,
            )

    if expected_action in {"close", "sell"}:
        if position_qty is None or position_qty <= 0:
            return _result(
                expectation=expectation,
                status="in_sync",
                order=order_state,
                position=position_state,
                reason_code="position_closed",
                safe_user_message="Position state updated from broker.",
            )
        if _is_residual_position(qty=position_qty, position=broker_position):
            residual_position = BrokerPositionReconciliationState(
                status="residual_position_remaining",
                broker_position_qty=position_qty,
                local_position_qty=local_qty,
                residual_qty=position_qty,
                reason_code="residual_position_remaining",
                safe_user_message="Position requires review.",
            )
            return _result(
                expectation=expectation,
                status="residual_position_remaining",
                order=order_state,
                position=residual_position,
                reason_code="residual_position_remaining",
                needs_review=True,
                action_recommended="residual_cleanup",
                safe_user_message="Position requires review.",
            )
        return _result(
            expectation=expectation,
            status="needs_review",
            order=order_state,
            position=position_state,
            reason_code="close_filled_but_position_remains",
            needs_review=True,
            action_recommended="review",
            safe_user_message="Position requires review.",
        )

    if order_state is not None and order_state.status == "filled":
        return _result(
            expectation=expectation,
            status="filled",
            order=order_state,
            position=position_state,
            reason_code="order_filled",
            action_recommended="update_local_state",
            safe_user_message="Broker order filled.",
        )

    if local_qty is not None and local_qty > 0 and (position_qty is None or position_qty <= 0):
        missing_position = BrokerPositionReconciliationState(
            status="broker_position_missing",
            broker_position_qty=position_qty,
            local_position_qty=local_qty,
            reason_code="broker_position_missing",
            safe_user_message="Position state updated from broker.",
        )
        return _result(
            expectation=expectation,
            status="broker_position_missing",
            order=order_state,
            position=missing_position,
            reason_code="broker_position_missing",
            action_recommended="update_local_state",
            safe_user_message="Position state updated from broker.",
        )

    if (local_qty is None or local_qty <= 0) and position_qty is not None and position_qty > 0:
        stale_local_position = BrokerPositionReconciliationState(
            status="local_position_stale",
            broker_position_qty=position_qty,
            local_position_qty=local_qty,
            reason_code="local_position_stale",
            safe_user_message="Position state updated from broker.",
        )
        return _result(
            expectation=expectation,
            status="local_position_stale",
            order=order_state,
            position=stale_local_position,
            reason_code="local_position_stale",
            action_recommended="update_local_state",
            safe_user_message="Position state updated from broker.",
        )

    if _local_pending_stale(expectation.local_order_status, order_state):
        return _result(
            expectation=expectation,
            status="local_pending_stale",
            order=order_state,
            position=position_state,
            reason_code="local_pending_stale",
            action_recommended="update_local_state",
            safe_user_message="Position state updated from broker.",
        )

    return _result(
        expectation=expectation,
        status="in_sync",
        order=order_state,
        position=position_state,
        reason_code="broker_state_in_sync",
        safe_user_message="Position state updated from broker.",
    )


def _result(
    *,
    expectation: BrokerReconciliationExpectation,
    status: str,
    reason_code: str,
    order: BrokerOrderReconciliationState | None = None,
    position: BrokerPositionReconciliationState | None = None,
    needs_review: bool = False,
    action_recommended: str = "none",
    safe_user_message: str = "Position state updated from broker.",
) -> BrokerReconciliationResult:
    return BrokerReconciliationResult(
        status=status,
        symbol=expectation.symbol.strip().upper(),
        expected_action=expectation.expected_action,
        product_key=expectation.product_key,
        runtime_source=expectation.runtime_source,
        close_reason=expectation.close_reason,
        exit_reason=expectation.exit_reason,
        order=order,
        position=position,
        reason_code=reason_code,
        action_recommended=action_recommended,
        needs_review=needs_review,
        safe_user_message=safe_user_message,
    )


def _find_matching_order(
    *,
    broker_orders: Iterable[Mapping[str, Any] | BrokerOrderResult],
    client_order_id: str | None,
    order_id: str | None,
    symbol: str,
) -> Mapping[str, Any] | BrokerOrderResult | None:
    for order in broker_orders:
        order_symbol = (_order_value(order, "symbol") or "").strip().upper()
        if order_symbol not in {"", symbol}:
            continue
        if client_order_id is not None and _order_value(order, "client_order_id") == client_order_id:
            return order
        if order_id is not None and _order_value(order, "order_id", "id", "broker_order_id") == order_id:
            return order
    return None


def _order_state(order: Mapping[str, Any] | BrokerOrderResult | None) -> BrokerOrderReconciliationState | None:
    if order is None:
        return None
    status = (_order_value(order, "status") or "").strip().lower()
    order_id = _order_value(order, "order_id", "id", "broker_order_id")
    client_order_id = _order_value(order, "client_order_id")
    filled_qty = _maybe_float(_order_value(order, "filled_qty"))
    filled_avg_price = _maybe_float(_order_value(order, "filled_avg_price"))
    if status in RECONCILIATION_PENDING_ORDER_STATUSES:
        return BrokerOrderReconciliationState(
            status="pending_fill",
            broker_status=status,
            order_id=order_id,
            client_order_id=client_order_id,
            filled_qty=filled_qty,
            filled_avg_price=filled_avg_price,
            reason_code="pending_fill",
            safe_user_message="Broker order is still pending.",
        )
    if status in RECONCILIATION_PARTIAL_ORDER_STATUSES:
        return BrokerOrderReconciliationState(
            status="partially_filled",
            broker_status=status,
            order_id=order_id,
            client_order_id=client_order_id,
            filled_qty=filled_qty,
            filled_avg_price=filled_avg_price,
            reason_code="partially_filled",
            safe_user_message="Broker order partially filled.",
        )
    if status == "filled":
        return BrokerOrderReconciliationState(
            status="filled",
            broker_status=status,
            order_id=order_id,
            client_order_id=client_order_id,
            filled_qty=filled_qty,
            filled_avg_price=filled_avg_price,
            reason_code="order_filled",
            safe_user_message="Broker order filled.",
        )
    if status in {"canceled", "cancelled", "expired"}:
        return BrokerOrderReconciliationState(
            status="canceled",
            broker_status=status,
            order_id=order_id,
            client_order_id=client_order_id,
            filled_qty=filled_qty,
            filled_avg_price=filled_avg_price,
            reason_code="order_canceled",
            safe_user_message="Position requires review.",
        )
    if status == "rejected":
        return BrokerOrderReconciliationState(
            status="rejected",
            broker_status=status,
            order_id=order_id,
            client_order_id=client_order_id,
            filled_qty=filled_qty,
            filled_avg_price=filled_avg_price,
            reason_code="order_rejected",
            safe_user_message="Order rejected by broker.",
        )
    return BrokerOrderReconciliationState(
        status="needs_review",
        broker_status=status or None,
        order_id=order_id,
        client_order_id=client_order_id,
        filled_qty=filled_qty,
        filled_avg_price=filled_avg_price,
        reason_code="unknown_order_status",
        safe_user_message="Position requires review.",
    )


def _position_state(
    *,
    symbol: str,
    expected_action: str | None,
    broker_position_qty: float | None,
    local_position_qty: float | None,
) -> BrokerPositionReconciliationState:
    if broker_position_qty is None:
        return BrokerPositionReconciliationState(
            status="broker_position_missing",
            broker_position_qty=None,
            local_position_qty=local_position_qty,
            reason_code="broker_position_missing",
            safe_user_message="Position state updated from broker.",
        )
    if broker_position_qty <= 0:
        return BrokerPositionReconciliationState(
            status="broker_position_missing",
            broker_position_qty=broker_position_qty,
            local_position_qty=local_position_qty,
            reason_code="broker_position_missing",
            safe_user_message="Position state updated from broker.",
        )
    if expected_action in {"close", "sell"} and broker_position_qty <= RESIDUAL_QTY_THRESHOLD:
        return BrokerPositionReconciliationState(
            status="residual_position_remaining",
            broker_position_qty=broker_position_qty,
            local_position_qty=local_position_qty,
            residual_qty=broker_position_qty,
            reason_code="residual_position_remaining",
            safe_user_message="Position requires review.",
        )
    return BrokerPositionReconciliationState(
        status="in_sync",
        broker_position_qty=broker_position_qty,
        local_position_qty=local_position_qty,
        reason_code="broker_position_present",
        safe_user_message="Position state updated from broker.",
    )


def _position_qty(position: BrokerPositionState | Mapping[str, Any] | None) -> float | None:
    if position is None:
        return None
    if isinstance(position, BrokerPositionState):
        return position.qty
    return _maybe_float(position.get("qty"))


def _is_residual_position(*, qty: float, position: BrokerPositionState | Mapping[str, Any] | None) -> bool:
    if qty <= 0 or qty > RESIDUAL_QTY_THRESHOLD:
        return False
    market_value = None
    if isinstance(position, BrokerPositionState):
        market_value = position.market_value
    elif isinstance(position, Mapping):
        market_value = _maybe_float(position.get("market_value"))
    return market_value is None or abs(market_value) <= RESIDUAL_VALUE_THRESHOLD


def _order_value(order: Mapping[str, Any] | BrokerOrderResult, *keys: str) -> str | None:
    for key in keys:
        if isinstance(order, BrokerOrderResult):
            value = getattr(order, key, None)
        else:
            value = order.get(key)
        if value is not None:
            return str(value)
    return None


def _normalize_action(action: str | None) -> str | None:
    normalized = (action or "").strip().lower()
    if normalized in {"take_profit", "strategy_exit", "stop_loss", "manual_close", "fractional_residual_cleanup"}:
        return "close"
    if normalized in {"buy", "open", "firstlot", "multi", "add"}:
        return "buy"
    if normalized in {"sell", "close"}:
        return normalized
    return normalized or None


def _local_pending_stale(local_order_status: str | None, order_state: BrokerOrderReconciliationState | None) -> bool:
    local_status = (local_order_status or "").strip().lower()
    if local_status not in RECONCILIATION_PENDING_ORDER_STATUSES:
        return False
    return order_state is not None and order_state.status in {"filled", "rejected", "canceled"}


def _maybe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
