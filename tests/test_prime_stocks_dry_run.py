# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_prime_stocks_dry_run.py
# ======================================================

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from app.brokers.alpaca_market_data import PrimeStocksBarSet
from app.products.stocks.bismel1.models import PriceBar
from app.runtime.prime_stocks_dry_run import PrimeStocksDryRunService
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    build_default_runtime_config,
)
from app.shared.config import AppConfig


def test_dry_run_service_writes_snapshot_signal_state_and_log_records() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksDryRunService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
    )

    result = service.run_once(symbol="AAPL")

    assert result.mode == "dry-run"
    assert result.runtime_target == "cloud_run"
    assert result.symbol == "AAPL"
    assert result.asset_type == "stock"
    assert result.candidate_action == "HOLD"
    assert result.status == "ok"
    assert result.bars_processed_execution == 11
    assert result.bars_processed_trend == 11
    assert result.firestore_paths["config_document"] == "runtime_products/prime_stocks/config/current"

    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["config"]["current"]["product_key"] == "stocks.bismel1"
    assert root["state"]["current"]["last_processed_bar_time"] is not None
    assert root["snapshots"]["latest"]["dry_run"] is True
    assert root["signals"]["latest"]["candidate_action"] == "HOLD"
    assert len(root["logs"]) == 1


def test_dry_run_service_rejects_non_stock_runtime_config() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["asset_type"] = "crypto"
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(default_config)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksDryRunService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
    )

    try:
        service.run_once()
    except ValueError as exc:
        assert "asset_type='stock'" in str(exc)
    else:
        raise AssertionError("Expected Prime Stocks dry-run to reject non-stock runtime config.")


class FakeMarketData:
    def fetch_prime_stocks_bars(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
    ) -> PrimeStocksBarSet:
        del asset_type, product_key, execution_limit, trend_limit
        return PrimeStocksBarSet(
            symbol=symbol,
            execution_bars=_bars(),
            trend_bars=_bars(),
        )


class FakeSnapshot:
    def __init__(self, payload):
        self._payload = payload
        self.exists = payload is not None

    def to_dict(self):
        return self._payload


class FakeDocumentReference:
    def __init__(self, storage: dict, path: list[str]) -> None:
        self._storage = storage
        self._path = path

    def collection(self, name: str):
        return FakeCollectionReference(self._storage, [*self._path, name])

    def get(self):
        return FakeSnapshot(_resolve_payload(self._storage, self._path))

    def set(self, payload, merge: bool = False):
        _write_payload(self._storage, self._path, payload, merge=merge)


class FakeCollectionReference:
    def __init__(self, storage: dict, path: list[str]) -> None:
        self._storage = storage
        self._path = path

    def document(self, name: str):
        return FakeDocumentReference(self._storage, [*self._path, name])


class FakeFirestoreClient:
    def __init__(self) -> None:
        self.storage: dict = {}

    def collection(self, name: str):
        return FakeCollectionReference(self.storage, [name])


def _resolve_payload(storage: dict, path: list[str]):
    cursor = storage
    for part in path:
        if part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def _write_payload(storage: dict, path: list[str], payload, merge: bool) -> None:
    cursor = storage
    for part in path[:-1]:
        cursor = cursor.setdefault(part, {})
    existing = cursor.get(path[-1], {})
    if merge and isinstance(existing, dict):
        existing.update(payload)
        cursor[path[-1]] = existing
        return
    cursor[path[-1]] = payload


def _bars() -> list[PriceBar]:
    start = datetime(2026, 4, 5, 13, 0, tzinfo=UTC)
    closes = [101.0, 102.0, 103.0, 104.0, 103.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    return [
        PriceBar(
            starts_at=start + timedelta(hours=index),
            ends_at=start + timedelta(hours=index + 1),
            open=close - 1.0,
            high=close + 1.0,
            low=close - 2.0,
            close=close,
            volume=1000.0 + index,
        )
        for index, close in enumerate(closes)
    ]


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
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_api_key_id="key-123",
        alpaca_api_secret="secret-123",
        alpaca_data_feed="iex",
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
    )
