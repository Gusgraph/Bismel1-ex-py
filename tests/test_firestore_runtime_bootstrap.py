from __future__ import annotations

from datetime import UTC, datetime

from app.services.firestore_runtime_store import (
    BOOTSTRAP_RUN_ID,
    build_prime_stocks_runtime_bootstrap_documents,
)
from app.shared.config import AppConfig
from scripts.bootstrap_prime_stocks_firestore_runtime import seed_prime_stocks_runtime_documents


def test_build_prime_stocks_runtime_bootstrap_documents_matches_runtime_defaults() -> None:
    settings = _settings()
    now = datetime(2026, 4, 5, 12, 0, tzinfo=UTC)

    documents = build_prime_stocks_runtime_bootstrap_documents(settings, updated_at=now)

    assert set(documents) == {
        "config/current",
        "state/current",
        "snapshots/latest",
        "signals/latest",
        "execution/current",
        "actions/latest",
    }
    assert documents["config/current"] == {
        "product_key": "stocks.bismel1",
        "strategy_key": "prime_stocks",
        "strategy_title": "Prime Stocks Bot Trader",
        "symbol": "AAPL",
        "asset_type": "stock",
        "enabled": True,
        "dry_run": False,
        "paper_execution_enabled": True,
        "execution_timeframe": "1H",
        "trend_timeframe": "1D",
        "pullback_window": 5,
        "execution_bar_limit": 351,
        "trend_bar_limit": 221,
        "first_lot_notional": 101.0,
        "multi_notional": 73.0,
        "account_id": None,
        "alpaca_account_id": None,
        "runtime_target": "cloud_run",
    }
    assert documents["state/current"]["run_id"] == BOOTSTRAP_RUN_ID
    assert documents["state/current"]["last_processed_bar_time"] is None
    assert documents["state/current"]["latest_execution_decision"] == "not_started"
    assert documents["state/current"]["updated_at"] == now.isoformat()
    assert documents["snapshots/latest"]["status"] == "initialized"
    assert documents["snapshots/latest"]["execution_timeframe"] == "1H"
    assert documents["snapshots/latest"]["trend_timeframe"] == "1D"
    assert documents["snapshots/latest"]["pullback_window"] == 5
    assert documents["snapshots/latest"]["signal"]["base_entry_trigger"] is False
    assert documents["snapshots/latest"]["state"]["add_count"] == 0
    assert documents["signals/latest"]["candidate_action"] == "BOOTSTRAPPED"
    assert documents["execution/current"]["execution_mode"] == "bootstrapped"
    assert documents["execution/current"]["order_status"] == "not_submitted"
    assert documents["actions/latest"]["execution"]["skipped_reason"] == "bootstrap_seeded"


def test_seed_prime_stocks_runtime_documents_writes_full_document_family() -> None:
    client = FakeFirestoreClient()
    written_paths = seed_prime_stocks_runtime_documents(
        client=client,
        collection_name="runtime_products",
        product_document="prime_stocks",
        documents=build_prime_stocks_runtime_bootstrap_documents(
            _settings(),
            updated_at=datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
        ),
    )

    assert written_paths == [
        "runtime_products/prime_stocks/config/current",
        "runtime_products/prime_stocks/state/current",
        "runtime_products/prime_stocks/snapshots/latest",
        "runtime_products/prime_stocks/signals/latest",
        "runtime_products/prime_stocks/execution/current",
        "runtime_products/prime_stocks/actions/latest",
    ]
    root = client.storage["runtime_products"]["prime_stocks"]
    assert root["config"]["current"]["paper_execution_enabled"] is True
    assert root["state"]["current"]["latest_status"] == "initialized"
    assert root["snapshots"]["latest"]["runtime_message"] == (
        "Prime Stocks Firestore runtime bootstrap seeded default documents."
    )
    assert root["signals"]["latest"]["trigger_source"] == "firestore_seed"
    assert root["execution"]["current"]["execution_key"] == "BOOTSTRAPPED:none"
    assert root["actions"]["latest"]["execution_decision"] == "not_started"


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


def _settings() -> AppConfig:
    return AppConfig(
        app_name="Bismel1-ex-py",
        environment="test",
        host="0.0.0.0",
        port=8080,
        cloud_run_target=True,
        pine_source_filename="Stocks-pine.pine",
        firestore_project_id="servgraph",
        firestore_database_id="bismel1-01",
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
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
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
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
