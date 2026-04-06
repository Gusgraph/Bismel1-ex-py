# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_market_data.py
# ======================================================

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.products.stocks.bismel1.models import PriceBar
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


ALPACA_TIMEFRAME_EXECUTION = "1Hour"
ALPACA_TIMEFRAME_TREND = "1Day"
SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
SUPPORTED_PRODUCT_KEYS = frozenset({"stocks.bismel1"})


class HttpClientProtocol(Protocol):
    def fetch_json(self, url: str, headers: dict[str, str]) -> dict[str, object]:
        ...


class UrllibJsonClient:
    def fetch_json(self, url: str, headers: dict[str, str]) -> dict[str, object]:
        request = Request(url=url, headers=headers, method="GET")
        with urlopen(request, timeout=27) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)


@dataclass(frozen=True)
class PrimeStocksBarSet:
    symbol: str
    execution_bars: list[PriceBar]
    trend_bars: list[PriceBar]


class AlpacaMarketDataAdapter:
    def __init__(
        self,
        settings: AppConfig,
        http_client: HttpClientProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or UrllibJsonClient()

    def fetch_prime_stocks_bars(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> PrimeStocksBarSet:
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
        execution_bars = self.fetch_stock_bars(
            symbol=symbol,
            timeframe=ALPACA_TIMEFRAME_EXECUTION,
            limit=execution_limit or self._settings.prime_stocks_execution_bar_limit,
            credential_context=credential_context,
        )
        trend_bars = self.fetch_stock_bars(
            symbol=symbol,
            timeframe=ALPACA_TIMEFRAME_TREND,
            limit=trend_limit or self._settings.prime_stocks_trend_bar_limit,
            credential_context=credential_context,
        )
        return PrimeStocksBarSet(
            symbol=symbol.upper(),
            execution_bars=execution_bars,
            trend_bars=trend_bars,
        )

    def fetch_stock_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        limit: int,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> list[PriceBar]:
        base_url = self._settings.alpaca_data_base_url.rstrip("/")
        query = urlencode(
            {
                "symbols": symbol.upper(),
                "timeframe": timeframe,
                "limit": limit,
                "sort": "asc",
                "feed": credential_context.data_feed if credential_context is not None else self._settings.alpaca_data_feed,
            }
        )
        payload = self._http_client.fetch_json(
            url=f"{base_url}/v2/stocks/bars?{query}",
            headers=self._headers(credential_context=credential_context),
        )
        return normalize_alpaca_bars(payload=payload, symbol=symbol)

    def _headers(self, *, credential_context: ResolvedAlpacaAccountContext | None = None) -> dict[str, str]:
        key_id = credential_context.key_id if credential_context is not None else self._settings.alpaca_api_key_id
        secret = credential_context.secret if credential_context is not None else self._settings.alpaca_api_secret
        if not key_id or not secret:
            raise RuntimeError("Alpaca credentials are required for Prime Stocks market-data fetches.")
        return {
            "Accept": "application/json",
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret,
        }

    @staticmethod
    def _ensure_stock_context(*, asset_type: str, product_key: str) -> None:
        normalized_asset_type = (asset_type or "").strip().lower()
        normalized_product_key = (product_key or "").strip().lower()
        if normalized_product_key not in SUPPORTED_PRODUCT_KEYS:
            raise ValueError(f"Prime Stocks runtime only supports product_key='stocks.bismel1'. Received {product_key!r}.")
        if normalized_asset_type not in SUPPORTED_STOCK_ASSET_TYPES:
            raise ValueError(f"Prime Stocks runtime accepts stock/equity symbols only. Received asset_type={asset_type!r}.")


def normalize_alpaca_bars(payload: dict[str, object], symbol: str) -> list[PriceBar]:
    bars_by_symbol = payload.get("bars", {})
    if not isinstance(bars_by_symbol, dict):
        return []
    raw_bars = bars_by_symbol.get(symbol.upper(), [])
    if not isinstance(raw_bars, list):
        return []
    normalized: list[PriceBar] = []
    for bar in raw_bars:
        if not isinstance(bar, dict):
            continue
        timestamp = _parse_alpaca_timestamp(bar["t"])
        normalized.append(
            PriceBar(
                starts_at=timestamp,
                ends_at=timestamp,
                open=float(bar["o"]),
                high=float(bar["h"]),
                low=float(bar["l"]),
                close=float(bar["c"]),
                volume=float(bar["v"]) if bar.get("v") is not None else None,
            )
        )
    return normalized


def _parse_alpaca_timestamp(raw: str) -> datetime:
    resolved = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(resolved)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
