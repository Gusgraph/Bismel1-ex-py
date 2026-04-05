# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_paper_trading.py
# ======================================================

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol
from urllib.request import Request, urlopen

from app.shared.config import AppConfig


SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
SUPPORTED_PRODUCT_KEYS = frozenset({"stocks.bismel1"})


class HttpRequestProtocol(Protocol):
    def request_json(
        self,
        *,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        ...


class UrllibRequestJsonClient:
    def request_json(
        self,
        *,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        resolved_headers = dict(headers)
        if body is not None:
            resolved_headers["Content-Type"] = "application/json"
        request = Request(url=url, headers=resolved_headers, data=body, method=method)
        with urlopen(request, timeout=27) as response:
            raw_payload = response.read().decode("utf-8")
        return json.loads(raw_payload) if raw_payload else {}


@dataclass(frozen=True)
class AlpacaPaperExecutionResult:
    action: str
    submitted: bool
    order_status: str
    order_id: str | None
    client_order_id: str | None
    side: str | None
    notional: float | None
    skipped_reason: str | None = None
    raw_response: dict[str, object] | None = None


class AlpacaPaperTradingAdapter:
    def __init__(
        self,
        settings: AppConfig,
        http_client: HttpRequestProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or UrllibRequestJsonClient()

    def submit_first_lot_buy(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        notional: float,
        client_order_id: str,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
        return self._submit_notional_buy(
            symbol=symbol,
            notional=notional,
            client_order_id=client_order_id,
            action="FirstLot",
        )

    def submit_multi_buy(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        notional: float,
        client_order_id: str,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
        return self._submit_notional_buy(
            symbol=symbol,
            notional=notional,
            client_order_id=client_order_id,
            action="MULTI",
        )

    def close_position(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        action: str,
        client_order_id: str,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
        base_url = self._settings.alpaca_trading_base_url.rstrip("/")
        payload = self._http_client.request_json(
            url=f"{base_url}/v2/positions/{symbol.upper()}",
            method="DELETE",
            headers=self._headers(client_order_id=client_order_id),
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=client_order_id,
            side="sell",
            notional=None,
            raw_response=payload,
        )

    def _submit_notional_buy(
        self,
        *,
        symbol: str,
        notional: float,
        client_order_id: str,
        action: str,
    ) -> AlpacaPaperExecutionResult:
        base_url = self._settings.alpaca_trading_base_url.rstrip("/")
        payload = self._http_client.request_json(
            url=f"{base_url}/v2/orders",
            method="POST",
            headers=self._headers(client_order_id=client_order_id),
            payload={
                "symbol": symbol.upper(),
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "notional": round(notional, 2),
                "client_order_id": client_order_id,
            },
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=_maybe_string(payload.get("client_order_id")) or client_order_id,
            side="buy",
            notional=round(notional, 2),
            raw_response=payload,
        )

    def _headers(self, *, client_order_id: str) -> dict[str, str]:
        del client_order_id
        if not self._settings.alpaca_api_key_id or not self._settings.alpaca_api_secret:
            raise RuntimeError("Alpaca credentials are required for Prime Stocks paper execution.")
        return {
            "Accept": "application/json",
            "APCA-API-KEY-ID": self._settings.alpaca_api_key_id,
            "APCA-API-SECRET-KEY": self._settings.alpaca_api_secret,
        }

    @staticmethod
    def _ensure_stock_context(*, asset_type: str, product_key: str) -> None:
        normalized_asset_type = (asset_type or "").strip().lower()
        normalized_product_key = (product_key or "").strip().lower()
        if normalized_product_key not in SUPPORTED_PRODUCT_KEYS:
            raise ValueError(f"Prime Stocks paper execution only supports product_key='stocks.bismel1'. Received {product_key!r}.")
        if normalized_asset_type not in SUPPORTED_STOCK_ASSET_TYPES:
            raise ValueError(f"Prime Stocks paper execution accepts stock/equity symbols only. Received asset_type={asset_type!r}.")


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)
