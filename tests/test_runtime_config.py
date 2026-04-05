# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_runtime_config.py
# ======================================================

from __future__ import annotations

from app.shared.config import get_settings


def test_runtime_config_loads_prime_stocks_dry_run_defaults(monkeypatch) -> None:
    monkeypatch.setenv("FIRESTORE_RUNTIME_COLLECTION", "runtime_products")
    monkeypatch.setenv("FIRESTORE_PRODUCT_DOCUMENT", "prime_stocks")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "key-123")
    monkeypatch.setenv("ALPACA_API_SECRET", "secret-123")
    monkeypatch.setenv("PRIME_STOCKS_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("PRIME_STOCKS_DRY_RUN", "true")
    monkeypatch.setenv("PRIME_STOCKS_EXECUTION_BAR_LIMIT", "351")
    monkeypatch.setenv("PRIME_STOCKS_TREND_BAR_LIMIT", "221")

    get_settings.cache_clear()
    settings = get_settings()

    assert settings.firestore_runtime_collection == "runtime_products"
    assert settings.firestore_product_document == "prime_stocks"
    assert settings.alpaca_api_key_id == "key-123"
    assert settings.alpaca_api_secret == "secret-123"
    assert settings.prime_stocks_runtime_enabled is True
    assert settings.prime_stocks_dry_run is True
    assert settings.prime_stocks_execution_bar_limit == 351
    assert settings.prime_stocks_trend_bar_limit == 221
