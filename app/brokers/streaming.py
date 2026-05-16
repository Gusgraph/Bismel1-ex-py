# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/streaming.py
# ======================================================

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable, Mapping, Protocol


STREAM_RECONCILIATION_EVENT_TYPES = frozenset(
    {
        "order_filled",
        "order_partially_filled",
        "order_canceled",
        "order_rejected",
        "order_expired",
        "order_replaced",
        "order_cancel_rejected",
    }
)
STREAM_HEALTHY_STATES = frozenset({"stream_connected", "stream_idle_connected"})
STREAM_DEGRADED_STATES = frozenset({"stream_disconnected", "stream_reconnect_scheduled", "stream_stale"})
STREAM_FAILED_STATES = frozenset({"stream_auth_failed", "parse_error"})


@dataclass(frozen=True)
class BrokerStreamContextKey:
    broker_provider: str
    environment: str
    broker_account_hash: str
    product_code: str
    slot_number: int = 1

    @property
    def key(self) -> str:
        return (
            f"{self.broker_provider}:{self.environment}:{self.broker_account_hash}:"
            f"{self.product_code}:slot_{self.slot_number}"
        )

    def to_safe_metadata(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "broker_provider": self.broker_provider,
            "environment": self.environment,
            "product_code": self.product_code,
            "slot_number": self.slot_number,
            "scope": "broker_account_product_slot",
            "shared": True,
        }


class SharedBrokerStreamRegistry:
    """Tracks shared stream contexts in-process so duplicate runners can be rejected early."""

    def __init__(self) -> None:
        self._active: set[str] = set()

    def acquire(self, context_key: BrokerStreamContextKey) -> bool:
        if context_key.key in self._active:
            return False
        self._active.add(context_key.key)
        return True

    def release(self, context_key: BrokerStreamContextKey) -> None:
        self._active.discard(context_key.key)

    def is_active(self, context_key: BrokerStreamContextKey) -> bool:
        return context_key.key in self._active


def hashed_broker_account_ref(*parts: object) -> str:
    material = ":".join(str(part).strip() for part in parts if part is not None and str(part).strip())
    if not material:
        material = "unknown"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def should_run_stream_for_context(
    *,
    product_code: str,
    asset_class: str = "stock",
    environment: str = "paper",
    admin_monitor: bool = False,
    market_open: bool = False,
) -> tuple[bool, str]:
    normalized_asset = asset_class.strip().lower()
    normalized_product = product_code.strip().lower()
    if normalized_asset == "crypto" and admin_monitor:
        return True, "admin_crypto_24_7"
    if normalized_product in {"prime_stocks", "execution"} and market_open:
        return True, "market_hours_active"
    if normalized_product in {"prime_stocks", "execution"}:
        return False, "market_closed"
    return environment.strip().lower() == "paper", "paper_context_active"


@dataclass(frozen=True)
class BrokerStreamEvent:
    broker: str
    event_type: str
    account_ref: str | int | None = None
    symbol: str | None = None
    order_status: str | None = None
    side: str | None = None
    filled_qty: float | None = None
    remaining_qty: float | None = None
    avg_fill_price: float | None = None
    client_order_id: str | None = None
    broker_order_id: str | None = None
    occurred_at: datetime | None = None
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    reason_code: str | None = None
    safe_user_message: str = "Position requires review."
    reconciliation_needed: bool = False

    def to_runtime_metadata(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "account_ref": self.account_ref,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "order_status": self.order_status,
            "side": self.side,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "avg_fill_price": self.avg_fill_price,
            "client_order_id": self.client_order_id,
            "broker_order_id": self.broker_order_id,
            "occurred_at": None if self.occurred_at is None else self.occurred_at.isoformat(),
            "received_at": self.received_at.isoformat(),
            "reason_code": self.reason_code,
            "safe_user_message": self.safe_user_message,
            "reconciliation_needed": self.reconciliation_needed,
        }

    def to_customer_payload(self) -> dict[str, Any]:
        return {
            "broker": self.broker,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "order_status": self.order_status,
            "side": self.side,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "avg_fill_price": self.avg_fill_price,
            "occurred_at": None if self.occurred_at is None else self.occurred_at.isoformat(),
            "received_at": self.received_at.isoformat(),
            "reason_code": self.reason_code,
            "safe_user_message": self.safe_user_message,
            "reconciliation_needed": self.reconciliation_needed,
        }


@dataclass(frozen=True)
class BrokerStreamHealth:
    broker: str
    status: str
    account_ref: str | int | None = None
    connected_at: datetime | None = None
    last_event_at: datetime | None = None
    stale_after_seconds: int = 120
    reconnect_attempt: int = 0
    reason_code: str | None = None
    safe_user_message: str = "Broker stream status updated."
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def is_stale(self, *, now: datetime | None = None) -> bool:
        if self.last_event_at is None:
            return self.status == "stream_connected"
        resolved_now = now or datetime.now(UTC)
        return resolved_now - self.last_event_at > timedelta(seconds=max(1, self.stale_after_seconds))

    def to_runtime_metadata(self) -> dict[str, Any]:
        status = "stream_stale" if self.is_stale() else self.status
        return {
            "broker": self.broker,
            "account_ref": self.account_ref,
            "stream_status": status,
            "connected_at": None if self.connected_at is None else self.connected_at.isoformat(),
            "last_event_at": None if self.last_event_at is None else self.last_event_at.isoformat(),
            "stale_after_seconds": self.stale_after_seconds,
            "reconnect_attempt": self.reconnect_attempt,
            "reason_code": "stream_stale" if status == "stream_stale" else self.reason_code,
            "safe_user_message": "Broker stream is stale." if status == "stream_stale" else self.safe_user_message,
            "diagnostics": self.diagnostics,
        }


class BrokerStreamEventSink(Protocol):
    def write_stream_event(self, event: BrokerStreamEvent) -> None:
        ...

    def write_stream_health(self, health: BrokerStreamHealth) -> None:
        ...


class BrokerEventStream(Protocol):
    def events(self) -> Iterable[str | Mapping[str, Any] | BrokerStreamEvent]:
        ...

    def close(self) -> None:
        ...


class InMemoryBrokerStreamEventSink:
    def __init__(self) -> None:
        self.events: list[BrokerStreamEvent] = []
        self.health: list[BrokerStreamHealth] = []

    def write_stream_event(self, event: BrokerStreamEvent) -> None:
        self.events.append(event)

    def write_stream_health(self, health: BrokerStreamHealth) -> None:
        self.health.append(health)


class BrokerStreamMonitor:
    def __init__(
        self,
        *,
        broker: str,
        account_ref: str | int | None,
        sink: BrokerStreamEventSink,
        stale_after_seconds: int = 120,
        max_reconnect_attempts: int = 5,
    ) -> None:
        self._broker = broker
        self._account_ref = account_ref
        self._sink = sink
        self._stale_after_seconds = stale_after_seconds
        self._max_reconnect_attempts = max(1, max_reconnect_attempts)
        self._closed = False
        self._reconnect_attempt = 0
        self._diagnostics: dict[str, Any] = {
            "message_count": 0,
            "event_count": 0,
            "unknown_message_count": 0,
            "parse_error_count": 0,
            "subscription_acknowledged": False,
            "auth_acknowledged": False,
            "last_message_at": None,
            "last_message_keys": [],
            "last_event_type": None,
        }

    def mark_connected(self) -> BrokerStreamEvent:
        event = BrokerStreamEvent(
            broker=self._broker,
            account_ref=self._account_ref,
            event_type="stream_connected",
            reason_code="stream_connected",
            safe_user_message="Broker stream connected.",
        )
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_connected",
                connected_at=event.received_at,
                last_event_at=event.received_at,
                stale_after_seconds=self._stale_after_seconds,
                diagnostics=dict(self._diagnostics),
                safe_user_message="Broker stream connected.",
            )
        )
        return event

    def mark_auth_acknowledged(self) -> None:
        self._diagnostics["auth_acknowledged"] = True
        self._diagnostics["auth_acknowledged_at"] = datetime.now(UTC).isoformat()

    def mark_subscribed(self, message: str | bytes | Mapping[str, Any] | list[Any] | None = None) -> None:
        self._diagnostics["subscription_acknowledged"] = True
        self._diagnostics["subscribed_at"] = datetime.now(UTC).isoformat()
        if message is not None:
            summary = summarize_stream_message(message)
            self._diagnostics["subscription_message_keys"] = summary.get("top_level_keys", [])
            self._diagnostics["subscription_streams"] = summary.get("streams", [])
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_connected",
                stale_after_seconds=self._stale_after_seconds,
                reason_code="trade_updates_subscribed",
                safe_user_message="Broker stream subscribed to trade updates.",
                diagnostics=dict(self._diagnostics),
            )
        )

    def mark_idle_connected(self) -> BrokerStreamEvent:
        event = BrokerStreamEvent(
            broker=self._broker,
            account_ref=self._account_ref,
            event_type="stream_idle_connected",
            reason_code="stream_idle_connected",
            safe_user_message="Broker stream connected. No trade update was received during the validation window.",
        )
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_idle_connected",
                last_event_at=event.received_at,
                stale_after_seconds=self._stale_after_seconds,
                diagnostics=dict(self._diagnostics),
                safe_user_message=event.safe_user_message,
            )
        )
        return event

    def mark_disconnected(self, *, reason_code: str = "stream_disconnected") -> BrokerStreamEvent:
        event = BrokerStreamEvent(
            broker=self._broker,
            account_ref=self._account_ref,
            event_type="stream_disconnected",
            reason_code=reason_code,
            safe_user_message="Broker stream disconnected.",
        )
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_disconnected",
                stale_after_seconds=self._stale_after_seconds,
                reason_code=reason_code,
                diagnostics=dict(self._diagnostics),
                safe_user_message="Broker stream disconnected.",
            )
        )
        return event

    def schedule_reconnect(self, *, reason_code: str = "stream_reconnect_scheduled") -> BrokerStreamEvent:
        self._reconnect_attempt = min(self._reconnect_attempt + 1, self._max_reconnect_attempts)
        event = BrokerStreamEvent(
            broker=self._broker,
            account_ref=self._account_ref,
            event_type="stream_reconnect_scheduled",
            reason_code=reason_code,
            safe_user_message="Broker stream reconnect scheduled.",
        )
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_reconnect_scheduled",
                stale_after_seconds=self._stale_after_seconds,
                reconnect_attempt=self._reconnect_attempt,
                reason_code=reason_code,
                diagnostics=dict(self._diagnostics),
                safe_user_message="Broker stream reconnect scheduled.",
            )
        )
        return event

    def mark_auth_failed(self) -> BrokerStreamEvent:
        event = BrokerStreamEvent(
            broker=self._broker,
            account_ref=self._account_ref,
            event_type="stream_auth_failed",
            reason_code="stream_auth_failed",
            safe_user_message="Broker stream authentication failed.",
        )
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_auth_failed",
                stale_after_seconds=self._stale_after_seconds,
                reason_code="stream_auth_failed",
                diagnostics=dict(self._diagnostics),
                safe_user_message="Broker stream authentication failed.",
            )
        )
        return event

    def handle_message(self, message: str | bytes | Mapping[str, Any] | list[Any] | BrokerStreamEvent) -> BrokerStreamEvent:
        self._record_message_diagnostics(message)
        event = normalize_broker_stream_message(
            broker=self._broker,
            account_ref=self._account_ref,
            message=message,
        )
        if event.event_type == "parse_error":
            self._diagnostics["parse_error_count"] = int(self._diagnostics.get("parse_error_count") or 0) + 1
        elif event.event_type == "unknown_event":
            self._diagnostics["unknown_message_count"] = int(self._diagnostics.get("unknown_message_count") or 0) + 1
        else:
            self._diagnostics["event_count"] = int(self._diagnostics.get("event_count") or 0) + 1
        self._diagnostics["last_event_type"] = event.event_type
        self._sink.write_stream_event(event)
        self._sink.write_stream_health(
            BrokerStreamHealth(
                broker=self._broker,
                account_ref=self._account_ref,
                status="stream_connected",
                last_event_at=event.received_at,
                stale_after_seconds=self._stale_after_seconds,
                reason_code=event.reason_code,
                diagnostics=dict(self._diagnostics),
                safe_user_message="Broker stream status updated.",
            )
        )
        return event

    def handle_disconnect(self, exc: Exception | None = None) -> BrokerStreamEvent:
        reason = "stream_disconnected" if exc is None else "stream_reconnect_scheduled"
        if exc is None:
            return self.mark_disconnected(reason_code=reason)
        return self.schedule_reconnect(reason_code=reason)

    def close(self) -> BrokerStreamEvent:
        self._closed = True
        return self.mark_disconnected(reason_code="stream_closed")

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def diagnostics(self) -> dict[str, Any]:
        return dict(self._diagnostics)

    def _record_message_diagnostics(self, message: str | bytes | Mapping[str, Any] | list[Any] | BrokerStreamEvent) -> None:
        summary = summarize_stream_message(message)
        self._diagnostics["message_count"] = int(self._diagnostics.get("message_count") or 0) + 1
        self._diagnostics["last_message_at"] = datetime.now(UTC).isoformat()
        self._diagnostics["last_message_keys"] = summary.get("top_level_keys", [])
        self._diagnostics["last_message_event"] = summary.get("event")
        self._diagnostics["last_message_stream"] = summary.get("stream")


def normalize_broker_stream_message(
    *,
    broker: str,
    account_ref: str | int | None,
    message: str | bytes | Mapping[str, Any] | list[Any] | BrokerStreamEvent,
) -> BrokerStreamEvent:
    if isinstance(message, BrokerStreamEvent):
        return message
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="ignore")
    if isinstance(message, str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return BrokerStreamEvent(
                broker=broker,
                account_ref=account_ref,
                event_type="parse_error",
                reason_code="stream_parse_error",
                safe_user_message="Broker stream message could not be read.",
            )
    elif isinstance(message, list):
        payload = _first_mapping_from_list(message)
        if payload is None:
            return BrokerStreamEvent(
                broker=broker,
                account_ref=account_ref,
                event_type="unknown_event",
                reason_code="unknown_event",
                safe_user_message="Broker stream event requires review.",
            )
    else:
        payload = dict(message)
    if isinstance(payload, list):
        resolved_payload = _first_mapping_from_list(payload)
        if resolved_payload is None:
            return BrokerStreamEvent(
                broker=broker,
                account_ref=account_ref,
                event_type="unknown_event",
                reason_code="unknown_event",
                safe_user_message="Broker stream event requires review.",
            )
        payload = resolved_payload
    if broker.strip().lower() == "alpaca":
        return normalize_alpaca_trade_update(payload=payload, account_ref=account_ref)
    return _unknown_event(broker=broker, account_ref=account_ref, payload=payload)


def summarize_stream_message(message: str | bytes | Mapping[str, Any] | list[Any] | BrokerStreamEvent) -> dict[str, Any]:
    payload: Any
    if isinstance(message, BrokerStreamEvent):
        payload = message.to_customer_payload()
    elif isinstance(message, bytes):
        payload = message.decode("utf-8", errors="ignore")
    else:
        payload = message
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return {"top_level_keys": [], "event": "parse_error", "stream": None, "streams": []}
    if isinstance(payload, list):
        payload = _first_mapping_from_list(payload) or {}
    if not isinstance(payload, Mapping):
        return {"top_level_keys": [], "event": None, "stream": None, "streams": []}
    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else payload
    order = data.get("order") if isinstance(data.get("order"), Mapping) else {}
    listen = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
    streams = listen.get("streams") if isinstance(listen.get("streams"), list) else []
    return {
        "top_level_keys": sorted(str(key) for key in payload.keys()),
        "event": _safe_string(data.get("event") or data.get("T") or data.get("type") or order.get("status")),
        "stream": _safe_string(payload.get("stream") or data.get("stream")),
        "streams": [_safe_string(item) for item in streams if _safe_string(item) is not None],
    }


def _first_mapping_from_list(payload: list[Any]) -> Mapping[str, Any] | None:
    for item in payload:
        if isinstance(item, Mapping):
            return item
    return None


def normalize_alpaca_trade_update(*, payload: Mapping[str, Any], account_ref: str | int | None = None) -> BrokerStreamEvent:
    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else payload
    event_name = str(data.get("event") or data.get("T") or data.get("type") or "").strip().lower()
    order = data.get("order") if isinstance(data.get("order"), Mapping) else data
    event_type = _map_alpaca_event_type(event_name=event_name, order=order)
    symbol = _safe_string(order.get("symbol"))
    side = _safe_string(order.get("side"))
    broker_order_id = _safe_string(order.get("id") or order.get("order_id"))
    client_order_id = _safe_string(order.get("client_order_id"))
    filled_qty = _maybe_float(data.get("qty") or data.get("filled_qty") or order.get("filled_qty"))
    remaining_qty = _resolve_remaining_qty(order=order, filled_qty=filled_qty)
    avg_fill_price = _maybe_float(data.get("price") or order.get("filled_avg_price") or order.get("avg_fill_price"))
    occurred_at = _parse_datetime(data.get("timestamp") or data.get("updated_at") or order.get("updated_at") or order.get("filled_at"))
    return BrokerStreamEvent(
        broker="alpaca",
        account_ref=account_ref,
        symbol=symbol,
        event_type=event_type,
        order_status=_status_from_event_type(event_type=event_type, order=order),
        side=side,
        filled_qty=filled_qty,
        remaining_qty=remaining_qty,
        avg_fill_price=avg_fill_price,
        client_order_id=client_order_id,
        broker_order_id=broker_order_id,
        occurred_at=occurred_at,
        reason_code=event_type,
        safe_user_message=_safe_message_for_event(event_type),
        reconciliation_needed=event_type in STREAM_RECONCILIATION_EVENT_TYPES,
    )


def _map_alpaca_event_type(*, event_name: str, order: Mapping[str, Any]) -> str:
    if event_name in {"new", "accepted"}:
        return "order_accepted" if event_name == "accepted" else "order_new"
    if event_name == "pending_new":
        return "order_pending_new"
    if event_name == "pending_cancel":
        return "order_pending_cancel"
    if event_name == "order_cancel_rejected":
        return "order_cancel_rejected"
    if event_name in {"fill", "filled"}:
        return "order_filled"
    if event_name in {"partial_fill", "partially_filled"}:
        return "order_partially_filled"
    if event_name in {"canceled", "cancelled"}:
        return "order_canceled"
    if event_name == "rejected":
        return "order_rejected"
    if event_name == "expired":
        return "order_expired"
    if event_name in {"replaced", "replaced_by"}:
        return "order_replaced"
    status = str(order.get("status") or "").strip().lower()
    if status in {
        "accepted",
        "new",
        "pending_new",
        "pending_cancel",
        "filled",
        "partially_filled",
        "canceled",
        "cancelled",
        "rejected",
        "expired",
        "replaced",
    }:
        return _map_alpaca_event_type(event_name=status, order={})
    return "unknown_event"


def _status_from_event_type(*, event_type: str, order: Mapping[str, Any]) -> str | None:
    status = _safe_string(order.get("status"))
    if status is not None:
        return status
    mapping = {
        "order_accepted": "accepted",
        "order_new": "new",
        "order_pending_new": "pending_new",
        "order_pending_cancel": "pending_cancel",
        "order_filled": "filled",
        "order_partially_filled": "partially_filled",
        "order_canceled": "canceled",
        "order_rejected": "rejected",
        "order_expired": "expired",
        "order_replaced": "replaced",
        "order_cancel_rejected": "cancel_rejected",
    }
    return mapping.get(event_type)


def _safe_message_for_event(event_type: str) -> str:
    return {
        "stream_connected": "Broker stream connected.",
        "stream_disconnected": "Broker stream disconnected.",
        "stream_reconnect_scheduled": "Broker stream reconnect scheduled.",
        "stream_auth_failed": "Broker stream authentication failed.",
        "order_accepted": "Broker order accepted.",
        "order_new": "Broker order accepted.",
        "order_pending_new": "Broker order is pending broker acceptance.",
        "order_pending_cancel": "Broker order cancel is pending.",
        "order_partially_filled": "Broker order partially filled.",
        "order_filled": "Broker order filled.",
        "order_canceled": "Broker order canceled.",
        "order_rejected": "Order rejected by broker.",
        "order_expired": "Broker order expired.",
        "order_replaced": "Broker order updated.",
        "order_cancel_rejected": "Broker could not cancel the order.",
        "parse_error": "Broker stream message could not be read.",
        "unknown_event": "Broker stream event requires review.",
    }.get(event_type, "Broker stream event requires review.")


def _unknown_event(*, broker: str, account_ref: str | int | None, payload: Mapping[str, Any]) -> BrokerStreamEvent:
    symbol = _safe_string(payload.get("symbol"))
    return BrokerStreamEvent(
        broker=broker,
        account_ref=account_ref,
        symbol=symbol,
        event_type="unknown_event",
        reason_code="unknown_event",
        safe_user_message="Broker stream event requires review.",
    )


def _resolve_remaining_qty(*, order: Mapping[str, Any], filled_qty: float | None) -> float | None:
    explicit_remaining = _maybe_float(order.get("remaining_qty") or order.get("leaves_qty"))
    if explicit_remaining is not None:
        return explicit_remaining
    qty = _maybe_float(order.get("qty"))
    if qty is None or filled_qty is None:
        return None
    return max(0.0, qty - filled_qty)


def _safe_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _maybe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if text == "":
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
