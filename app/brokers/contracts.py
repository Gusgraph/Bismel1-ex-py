# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/contracts.py
# ======================================================

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from app.brokers.models import (
    BrokerAccountState,
    BrokerAssetState,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPositionState,
)


@runtime_checkable
class BrokerAdapter(Protocol):
    def submit_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        ...

    def close_position(self, account_id: str, symbol: str, metadata: dict[str, Any] | None = None) -> BrokerOrderResult:
        ...

    def cancel_order(self, account_id: str, order_id: str, metadata: dict[str, Any] | None = None) -> BrokerOrderResult:
        ...

    def get_account(self, account_id: str) -> BrokerAccountState:
        ...

    def get_positions(self, account_id: str) -> list[BrokerPositionState]:
        ...

    def get_position(self, account_id: str, symbol: str) -> BrokerPositionState | None:
        ...

    def get_asset(self, symbol: str) -> BrokerAssetState | None:
        ...

    def health_check(self, account_id: str | None = None) -> dict[str, Any]:
        ...
