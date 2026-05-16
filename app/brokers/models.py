# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/models.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


BrokerRawPayload = dict[str, Any] | list[Any] | None


@dataclass(frozen=True)
class BrokerOrderRequest:
    account_id: str | int | None
    symbol: str
    side: str
    order_type: str
    time_in_force: str
    client_order_id: str
    qty: float | None = None
    notional: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    asset_class: str | None = None
    product_key: str | None = None
    strategy_key: str | None = None
    execution_mode: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_symbol = self.symbol.strip().upper()
        normalized_side = self.side.strip().lower()
        normalized_order_type = self.order_type.strip().lower()
        normalized_time_in_force = self.time_in_force.strip().lower()
        normalized_client_order_id = self.client_order_id.strip()

        if normalized_symbol == "":
            raise ValueError("Broker order request requires a symbol.")
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("Broker order request side must be buy or sell.")
        if normalized_order_type == "":
            raise ValueError("Broker order request requires an order_type.")
        if normalized_order_type not in {"market", "limit"}:
            raise ValueError("Broker order request order_type must be market or limit.")
        if normalized_time_in_force == "":
            raise ValueError("Broker order request requires time_in_force.")
        if normalized_client_order_id == "":
            raise ValueError("Broker order request requires client_order_id.")
        if self.qty is not None and self.qty <= 0:
            raise ValueError("Broker order request qty must be greater than zero.")
        if self.notional is not None and self.notional <= 0:
            raise ValueError("Broker order request notional must be greater than zero.")
        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError("Broker order request limit_price must be greater than zero.")
        if normalized_order_type == "market" and (self.qty is None) == (self.notional is None):
            raise ValueError("Broker market order request requires exactly one of qty or notional.")
        if normalized_order_type == "limit":
            if self.limit_price is None:
                raise ValueError("Broker limit order request requires limit_price.")
            if self.qty is None:
                raise ValueError("Broker limit order request requires qty.")
            if self.notional is not None:
                raise ValueError("Broker limit order request does not support notional.")

        object.__setattr__(self, "symbol", normalized_symbol)
        object.__setattr__(self, "side", normalized_side)
        object.__setattr__(self, "order_type", normalized_order_type)
        object.__setattr__(self, "time_in_force", normalized_time_in_force)
        object.__setattr__(self, "client_order_id", normalized_client_order_id)


@dataclass(frozen=True)
class BrokerOrderResult:
    ok: bool
    broker: str
    account_id: str | int | None
    symbol: str | None = None
    order_id: str | None = None
    broker_order_id: str | None = None
    client_order_id: str | None = None
    status: str | None = None
    side: str | None = None
    qty: float | None = None
    notional: float | None = None
    requested_qty: float | None = None
    requested_notional: float | None = None
    filled_qty: float | None = None
    filled_avg_price: float | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    received_at: datetime | None = None
    raw_response: BrokerRawPayload = None
    error_code: str | None = None
    error_category: str | None = None
    error_message: str | None = None
    error_message_safe: str | None = None
    retryable: bool = False
    attempt_count: int = 1
    broker_status_code: int | None = None


@dataclass(frozen=True)
class BrokerAccountState:
    broker: str
    account_id: str | int | None
    status: str | None
    currency: str | None = None
    equity: float | None = None
    cash: float | None = None
    buying_power: float | None = None
    portfolio_value: float | None = None
    raw_response: BrokerRawPayload = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class BrokerPositionState:
    broker: str
    account_id: str | int | None
    symbol: str
    qty: float
    asset_class: str | None = None
    side: str | None = None
    avg_entry_price: float | None = None
    current_price: float | None = None
    market_value: float | None = None
    unrealized_pl: float | None = None
    unrealized_plpc: float | None = None
    raw_response: BrokerRawPayload = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_symbol = self.symbol.strip().upper()
        if normalized_symbol == "":
            raise ValueError("Broker position state requires a symbol.")
        object.__setattr__(self, "symbol", normalized_symbol)


@dataclass(frozen=True)
class BrokerAssetState:
    broker: str
    symbol: str
    asset_class: str
    tradable: bool
    marginable: bool | None = None
    fractionable: bool | None = None
    shortable: bool | None = None
    status: str | None = None
    raw_response: BrokerRawPayload = None

    def __post_init__(self) -> None:
        normalized_symbol = self.symbol.strip().upper()
        if normalized_symbol == "":
            raise ValueError("Broker asset state requires a symbol.")
        object.__setattr__(self, "symbol", normalized_symbol)
