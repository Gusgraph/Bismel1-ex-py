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
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import ceil
from socket import timeout as SocketTimeout
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.products.stocks.bismel1.models import PriceBar
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
SUPPORTED_VALIDATION_CRYPTO_ASSET_TYPES = frozenset({"crypto"})
SUPPORTED_PRODUCT_KEYS = frozenset({"stocks.bismel1"})
SUPPORTED_ALPACA_TIMEFRAMES = {
    "15M": "15Min",
    "15MIN": "15Min",
    "1M": "1Min",
    "1MIN": "1Min",
    "1H": "1Hour",
    "1HOUR": "1Hour",
    "4H": "4Hour",
    "4HOUR": "4Hour",
    "1D": "1Day",
    "1DAY": "1Day",
    "D": "1Day",
}


class HttpClientProtocol(Protocol):
    def fetch_json(self, url: str, headers: dict[str, str]) -> dict[str, object]:
        ...


class UrllibJsonClient:
    def fetch_json(self, url: str, headers: dict[str, str]) -> dict[str, object]:
        request = Request(url=url, headers=headers, method="GET")
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                with urlopen(request, timeout=27) as response:
                    payload = response.read().decode("utf-8")
                return json.loads(payload)
            except HTTPError as exc:
                if attempt == 0 and exc.code in {408, 425, 429, 500, 502, 503, 504}:
                    last_error = exc
                    time.sleep(0.75)
                    continue
                raise
            except (URLError, TimeoutError, SocketTimeout) as exc:
                if attempt == 0 and _is_retryable_network_error(exc):
                    last_error = exc
                    time.sleep(0.75)
                    continue
                raise

        if last_error is not None:
            raise last_error

        raise RuntimeError("Alpaca market-data fetch failed without a concrete error.")


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
        execution_timeframe: str | None = None,
        trend_timeframe: str | None = None,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> PrimeStocksBarSet:
        normalized_asset_type = (asset_type or "").strip().lower()
        if normalized_asset_type in SUPPORTED_VALIDATION_CRYPTO_ASSET_TYPES:
            self._ensure_validation_crypto_context(asset_type=asset_type, product_key=product_key, symbol=symbol)
            resolved_execution_timeframe = _normalize_alpaca_timeframe(execution_timeframe or "1M")
            resolved_trend_timeframe = _normalize_alpaca_timeframe(trend_timeframe or resolved_execution_timeframe)
            execution_bars = self.fetch_crypto_bars(
                symbol=symbol,
                timeframe=resolved_execution_timeframe,
                limit=execution_limit or 19,
                credential_context=credential_context,
            )
            trend_bars = self.fetch_crypto_bars(
                symbol=symbol,
                timeframe=resolved_trend_timeframe,
                limit=trend_limit or 11,
                credential_context=credential_context,
            )
            return PrimeStocksBarSet(
                symbol=symbol.upper(),
                execution_bars=execution_bars,
                trend_bars=trend_bars,
            )
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
        resolved_execution_timeframe = _normalize_alpaca_timeframe(execution_timeframe or "15M")
        resolved_trend_timeframe = _normalize_alpaca_timeframe(trend_timeframe or "1D")
        execution_bars = self.fetch_stock_bars(
            symbol=symbol,
            timeframe=resolved_execution_timeframe,
            limit=execution_limit or self._settings.prime_stocks_execution_bar_limit,
            credential_context=credential_context,
        )
        trend_bars = self.fetch_stock_bars(
            symbol=symbol,
            timeframe=resolved_trend_timeframe,
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
        start_at, end_at = _stock_bar_time_window(timeframe=timeframe, limit=limit)
        query = urlencode(
            {
                "symbols": symbol.upper(),
                "timeframe": timeframe,
                "limit": limit,
                "sort": "desc",
                "start": start_at,
                "end": end_at,
                "feed": credential_context.data_feed if credential_context is not None else self._settings.alpaca_data_feed,
            }
        )
        payload = self._http_client.fetch_json(
            url=f"{base_url}/v2/stocks/bars?{query}",
            headers=self._headers(credential_context=credential_context),
        )
        return sorted(
            normalize_alpaca_bars(payload=payload, symbol=symbol),
            key=lambda bar: bar.starts_at,
        )

    def fetch_crypto_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        limit: int,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> list[PriceBar]:
        base_url = self._settings.alpaca_data_base_url.rstrip("/")
        query_symbol = _normalize_crypto_symbol_for_query(symbol)
        query = urlencode(
            {
                "symbols": query_symbol,
                "timeframe": timeframe,
                "limit": limit,
                "sort": "asc",
            }
        )
        payload = self._http_client.fetch_json(
            url=f"{base_url}/v1beta3/crypto/us/bars?{query}",
            headers=self._headers(credential_context=credential_context),
        )
        return normalize_alpaca_bars(payload=payload, symbol=query_symbol)

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

    @staticmethod
    def _ensure_validation_crypto_context(*, asset_type: str, product_key: str, symbol: str) -> None:
        normalized_asset_type = (asset_type or "").strip().lower()
        normalized_product_key = (product_key or "").strip().lower()
        normalized_symbol = (symbol or "").strip().upper()
        if normalized_product_key != "stocks.bismel1":
            raise ValueError(f"Prime Stocks runtime only supports product_key='stocks.bismel1'. Received {product_key!r}.")
        if normalized_asset_type not in SUPPORTED_VALIDATION_CRYPTO_ASSET_TYPES:
            raise ValueError(f"Prime Stocks validation runtime accepts crypto override only. Received asset_type={asset_type!r}.")
        if normalized_symbol != "SHIBUSD":
            raise ValueError(f"Prime Stocks validation runtime only supports test symbol override 'SHIBUSD'. Received {symbol!r}.")


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


def _normalize_alpaca_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().upper()
    if normalized not in SUPPORTED_ALPACA_TIMEFRAMES:
        raise ValueError(f"Unsupported Alpaca timeframe for Prime Stocks runtime: {timeframe!r}.")
    return SUPPORTED_ALPACA_TIMEFRAMES[normalized]


def _stock_bar_time_window(*, timeframe: str, limit: int) -> tuple[str, str]:
    resolved_limit = max(1, int(limit))
    bar_minutes = _market_minutes_per_bar(timeframe)
    trading_days_needed = max(1, ceil((resolved_limit * bar_minutes) / 390))
    calendar_days = max(7, ceil(trading_days_needed * 1.8) + 3)
    end_at = datetime.now(UTC).replace(microsecond=0)
    start_at = end_at - timedelta(days=calendar_days)
    return start_at.isoformat(), end_at.isoformat()


def _market_minutes_per_bar(timeframe: str) -> int:
    normalized = (timeframe or "").strip().lower()
    mapping = {
        "1min": 1,
        "15min": 15,
        "1hour": 60,
        "4hour": 240,
        "1day": 390,
    }
    return mapping.get(normalized, 15)


def _is_retryable_network_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "timed out" in message
        or "timeout" in message
        or "temporarily unavailable" in message
        or "connection reset" in message
        or "connection aborted" in message
    )


def _normalize_crypto_symbol_for_query(symbol: str) -> str:
    resolved = symbol.strip().upper().replace("/", "")
    if resolved == "SHIBUSD":
        return "SHIB/USD"
    return symbol.strip().upper()
