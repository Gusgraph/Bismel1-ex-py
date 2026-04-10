# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_gemini_ai_scoring.py
# ======================================================

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.products.stocks.bismel1.models import AiCacheRecord
from app.services.firestore_runtime_store import PrimeStocksFirestoreRuntimeStore
from app.services.gemini_ai_scoring import (
    GeminiAiScoringService,
    build_ai_cache_record,
    merge_ai_cache_records,
    normalize_ai_classification_payload,
)
from app.shared.config import AppConfig


def test_normalize_ai_classification_payload_maps_avoid_and_clamps_confidence() -> None:
    payload = normalize_ai_classification_payload(
        {
            "Ai_regime_label": "risk_off",
            "Ai_sentiment_label": "bearish",
            "Ai_safety_label": "avoid",
            "Ai_confidence": 4,
            "Ai_reason": "Major macro headline raised downside risk.",
        }
    )

    assert payload["Ai_regime_label"] == "risk_off"
    assert payload["Ai_sentiment_label"] == "bearish"
    assert payload["Ai_safety_label"] == "unsafe"
    assert payload["Ai_confidence"] == 1.0


def test_gemini_scoring_service_returns_normalized_record() -> None:
    scorer = GeminiAiScoringService(
        api_key="test-key",
        model="gemini-2.5-flash-lite",
        http_client=FakeGeminiClient(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": '{"Ai_regime_label":"risk_on","Ai_sentiment_label":"bullish","Ai_safety_label":"safe","Ai_confidence":0.91,"Ai_reason":"Headline supports constructive AI demand."}'
                                }
                            ]
                        }
                    }
                ]
            }
        ),
    )

    record = scorer.score_headline(scope="symbol", symbol="AAPL", headline="AI demand improves.")

    assert record.symbol == "AAPL"
    assert record.Ai_regime_label == "risk_on"
    assert record.Ai_sentiment_label == "bullish"
    assert record.Ai_safety_label == "safe"
    assert record.Ai_blocked_reason is None


def test_firestore_runtime_store_reads_and_writes_ai_cache_records() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    record = build_ai_cache_record(
        scope="symbol",
        symbol="AAPL",
        ai_payload={
            "Ai_regime_label": "neutral",
            "Ai_sentiment_label": "bearish",
            "Ai_safety_label": "caution",
            "Ai_confidence": 0.73,
            "Ai_reason": "Short-term downgrade pressure remains active.",
        },
        updated_at=datetime.now(tz=UTC),
        source="gemini:test",
    )

    store.write_ai_symbol_record(record)
    loaded = store.load_ai_symbol_record("AAPL")

    assert loaded is not None
    assert loaded.symbol == "AAPL"
    assert loaded.Ai_sentiment_label == "bearish"
    assert loaded.Ai_block_new_entries is True


def test_merge_ai_cache_records_blocks_when_symbol_record_is_missing() -> None:
    market_record = AiCacheRecord(
        scope="market",
        symbol=None,
        Ai_regime_label="risk_on",
        Ai_sentiment_label="neutral",
        Ai_safety_label="safe",
        Ai_confidence=0.84,
        Ai_reason="Market okay.",
        Ai_updated_at=datetime.now(tz=UTC).isoformat(),
        Ai_source="gemini:test",
        Ai_execution_allowed=True,
        Ai_block_new_entries=False,
        Ai_block_adds=False,
        Ai_blocked_reason=None,
    )

    decision = merge_ai_cache_records(
        market_record=market_record,
        symbol_record=None,
        max_age_minutes=360,
        now=datetime.now(tz=UTC),
    )

    assert decision.Ai_blocked_reason == "ai_cache_unavailable"
    assert decision.Ai_execution_allowed is False


def test_merge_ai_cache_records_marks_stale_ai() -> None:
    stale_time = (datetime.now(tz=UTC) - timedelta(hours=27)).isoformat()
    market_record = AiCacheRecord(
        scope="market",
        symbol=None,
        Ai_regime_label="risk_on",
        Ai_sentiment_label="neutral",
        Ai_safety_label="safe",
        Ai_confidence=0.84,
        Ai_reason="Market okay.",
        Ai_updated_at=stale_time,
        Ai_source="gemini:test",
        Ai_execution_allowed=True,
        Ai_block_new_entries=False,
        Ai_block_adds=False,
        Ai_blocked_reason=None,
    )
    symbol_record = AiCacheRecord(
        scope="symbol",
        symbol="AAPL",
        Ai_regime_label="neutral",
        Ai_sentiment_label="bullish",
        Ai_safety_label="safe",
        Ai_confidence=0.88,
        Ai_reason="Symbol okay.",
        Ai_updated_at=stale_time,
        Ai_source="gemini:test",
        Ai_execution_allowed=True,
        Ai_block_new_entries=False,
        Ai_block_adds=False,
        Ai_blocked_reason=None,
    )

    decision = merge_ai_cache_records(
        market_record=market_record,
        symbol_record=symbol_record,
        max_age_minutes=60,
        now=datetime.now(tz=UTC),
    )

    assert decision.Ai_blocked_reason == "ai_cache_stale"
    assert decision.is_stale is True


class FakeGeminiClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def generate(self, *, url: str, payload: dict[str, object]) -> dict[str, object]:
        assert "gemini-2.5-flash-lite" in url
        assert payload["generationConfig"]["responseMimeType"] == "application/json"
        return self.payload


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
        gemini_model="gemini-2.5-flash-lite",
        ai_cache_max_age_minutes=360,
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=False,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_ai_validation_bypass_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_test_mode=False,
        prime_stocks_test_trigger=None,
        prime_stocks_test_symbol_override=None,
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
        prime_stocks_first_lot_notional=101.0,
        prime_stocks_multi_notional=73.0,
        prime_stocks_max_notional_per_order=303.0,
        prime_stocks_max_total_notional_per_symbol=707.0,
        prime_stocks_max_add_count=2,
        prime_stocks_daily_order_cap=None,
        prime_stocks_max_open_positions=None,
        prime_stocks_broker_retry_max_attempts=1,
        prime_stocks_force_candidate_action=None,
        prime_stocks_scheduler_job_name="prime-stocks-scheduled",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="secret-value",
        prime_stocks_ping_scheduler_job_name="prime-stocks-ping",
        prime_stocks_ping_scheduler_schedule="*/1 * * * *",
        prime_stocks_ping_scheduler_timezone="Etc/UTC",
        prime_stocks_ping_scheduler_header_value="ping-secret-value",
    )
