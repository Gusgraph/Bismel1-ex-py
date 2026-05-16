# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/factory.py
# ======================================================

from __future__ import annotations

from typing import Any

from app.brokers.alpaca_paper_trading import AlpacaPaperTradingAdapter
from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.brokers.models import (
    BrokerAccountState,
    BrokerAssetState,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPositionState,
)
from app.brokers.transport_policy import (
    is_admin_runtime_monitor_context,
    normalize_transport,
    resolve_alpaca_transport_decision,
)
from app.shared.config import AppConfig


def resolve_alpaca_transport(settings: AppConfig) -> str:
    return normalize_transport(settings.alpaca_transport)


def resolve_admin_runtime_monitor_alpaca_transport(settings: AppConfig) -> str:
    return normalize_transport(settings.admin_runtime_monitor_alpaca_transport)


class ScopedAlpacaBrokerAdapter:
    """Route admin monitor paper contexts to SDK while customer contexts stay on REST."""

    def __init__(
        self,
        *,
        rest_adapter: AlpacaPaperTradingAdapter,
        sdk_adapter: AlpacaSdkBrokerAdapter,
        settings: AppConfig,
    ) -> None:
        self.rest_adapter = rest_adapter
        self.sdk_adapter = sdk_adapter
        self._settings = settings

    def _adapter_for_context(self, credential_context: object | None) -> AlpacaPaperTradingAdapter | AlpacaSdkBrokerAdapter:
        decision = resolve_alpaca_transport_decision(settings=self._settings, context=credential_context)
        if decision.selected == "sdk":
            return self.sdk_adapter
        return self.rest_adapter

    def _adapter_for_metadata(self, metadata: dict[str, object] | None) -> AlpacaPaperTradingAdapter | AlpacaSdkBrokerAdapter:
        context = metadata.get("credential_context") if isinstance(metadata, dict) else None
        return self._adapter_for_context(context)

    def submit_first_lot_buy(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).submit_first_lot_buy(**kwargs)

    def submit_multi_buy(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).submit_multi_buy(**kwargs)

    def submit_market_order_qty(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).submit_market_order_qty(**kwargs)

    def submit_market_order_notional(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).submit_market_order_notional(**kwargs)

    def close_position(self, *args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        credential_context = kwargs.get("credential_context")
        if credential_context is None and isinstance(metadata, dict):
            credential_context = metadata.get("credential_context")
        return self._adapter_for_context(credential_context).close_position(*args, **kwargs)

    def close_position_symbol(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).close_position_symbol(**kwargs)

    def cancel_order(self, *args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        credential_context = kwargs.get("credential_context")
        if credential_context is None and isinstance(metadata, dict):
            credential_context = metadata.get("credential_context")
        return self._adapter_for_context(credential_context).cancel_order(*args, **kwargs)

    def get_submission_state(self, **kwargs: Any) -> Any:
        return self._adapter_for_context(kwargs.get("credential_context")).get_submission_state(**kwargs)

    def list_recent_orders(self, **kwargs: Any) -> list[dict[str, object]]:
        return self._adapter_for_context(kwargs.get("credential_context")).list_recent_orders(**kwargs)

    def submit_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        return self._adapter_for_metadata(request.metadata).submit_order(request)

    def get_account(self, account_id: str) -> BrokerAccountState:
        return self.rest_adapter.get_account(account_id)

    def get_positions(self, account_id: str) -> list[BrokerPositionState]:
        return self.rest_adapter.get_positions(account_id)

    def get_position(self, account_id: str, symbol: str) -> BrokerPositionState | None:
        return self.rest_adapter.get_position(account_id, symbol)

    def get_asset(self, symbol: str) -> BrokerAssetState | None:
        return self.rest_adapter.get_asset(symbol)

    def health_check(self, account_id: str | None = None) -> dict[str, Any]:
        return self.rest_adapter.health_check(account_id)


def build_alpaca_broker_adapter(
    settings: AppConfig,
) -> AlpacaPaperTradingAdapter | AlpacaSdkBrokerAdapter | ScopedAlpacaBrokerAdapter:
    if resolve_alpaca_transport(settings) == "sdk":
        return AlpacaSdkBrokerAdapter(settings=settings)
    if normalize_transport(settings.alpaca_transport_primary, default="sdk") == "sdk":
        return ScopedAlpacaBrokerAdapter(
            rest_adapter=AlpacaPaperTradingAdapter(settings=settings),
            sdk_adapter=AlpacaSdkBrokerAdapter(settings=settings),
            settings=settings,
        )
    return AlpacaPaperTradingAdapter(settings=settings)
