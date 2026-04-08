# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_alpaca_market_data.py
# ======================================================

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter, normalize_alpaca_bars
from app.shared.config import AppConfig


def test_normalize_alpaca_bars_maps_payload_to_internal_price_bars() -> None:
    bars = normalize_alpaca_bars(
        payload={
            "bars": {
                "AAPL": [
                    {
                        "t": "2026-04-05T13:00:00Z",
                        "o": 101.0,
                        "h": 103.0,
                        "l": 99.0,
                        "c": 102.0,
                        "v": 1500,
                    }
                ]
            }
        },
        symbol="AAPL",
    )

    assert len(bars) == 1
    assert bars[0].starts_at == datetime(2026, 4, 5, 13, 0, tzinfo=UTC)
    assert bars[0].open == 101.0
    assert bars[0].high == 103.0
    assert bars[0].low == 99.0
    assert bars[0].close == 102.0
    assert bars[0].volume == 1500.0


def test_market_data_adapter_rejects_non_stock_context() -> None:
    adapter = AlpacaMarketDataAdapter(settings=_settings())

    with pytest.raises(ValueError, match="stock/equity"):
        adapter.fetch_prime_stocks_bars(
            symbol="BTCUSD",
            asset_type="crypto",
            product_key="stocks.bismel1",
        )


def test_market_data_adapter_maps_runtime_timeframes_to_native_alpaca_values() -> None:
    client = FakeHttpClient(
        payload={
            "bars": {
                "AAPL": [
                    {
                        "t": "2026-04-05T13:00:00Z",
                        "o": 101.0,
                        "h": 103.0,
                        "l": 99.0,
                        "c": 102.0,
                        "v": 1500,
                    }
                ]
            }
        }
    )
    adapter = AlpacaMarketDataAdapter(settings=_settings(), http_client=client)

    adapter.fetch_prime_stocks_bars(
        symbol="AAPL",
        asset_type="stock",
        product_key="stocks.bismel1",
        execution_timeframe="1H",
        trend_timeframe="1D",
    )

    assert "timeframe=1Hour" in client.urls[0]
    assert "timeframe=1Day" in client.urls[1]


class FakeHttpClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def fetch_json(self, url: str, headers: dict[str, str]) -> dict[str, object]:
        assert headers["APCA-API-KEY-ID"] == "key-123"
        assert headers["APCA-API-SECRET-KEY"] == "secret-123"
        self.urls.append(url)
        return self.payload


def _settings() -> AppConfig:
    return AppConfig(
        app_name="Bismel1-ex-py",
        environment="test",
        host="0.0.0.0",
        port=8080,
        cloud_run_target=True,
        pine_source_filename="Stocks-pine.pine",
        firestore_project_id=None,
        firestore_database_id="(default)",
        firestore_runtime_collection="runtime_products",
        firestore_product_document="prime_stocks",
        laravel_runtime_bridge_url="https://bismel1.test",
        laravel_runtime_bridge_token="bridge-token",
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_live_trading_base_url="https://api.alpaca.markets",
        alpaca_api_key_id="key-123",
        alpaca_api_secret="secret-123",
        alpaca_data_feed="iex",
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=False,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
        prime_stocks_first_lot_notional=101.0,
        prime_stocks_multi_notional=73.0,
        prime_stocks_scheduler_job_name="prime-stocks-scheduled",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="secret-value",
    )
