# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/shared/config.py
# ======================================================

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class AppConfig:
    app_name: str
    environment: str
    host: str
    port: int
    cloud_run_target: bool
    pine_source_filename: str
    firestore_project_id: str | None
    firestore_database_id: str
    firestore_runtime_collection: str
    firestore_product_document: str
    alpaca_data_base_url: str
    alpaca_api_key_id: str | None
    alpaca_api_secret: str | None
    alpaca_data_feed: str
    prime_stocks_runtime_enabled: bool
    prime_stocks_dry_run: bool
    prime_stocks_default_symbol: str
    prime_stocks_asset_type: str
    prime_stocks_execution_bar_limit: int
    prime_stocks_trend_bar_limit: int


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@lru_cache(maxsize=1)
def get_settings() -> AppConfig:
    return AppConfig(
        app_name=os.getenv("APP_NAME", "Bismel1-ex-py"),
        environment=os.getenv("APP_ENV", "development"),
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8080")),
        cloud_run_target=_env_flag("CLOUD_RUN_TARGET", True),
        pine_source_filename=os.getenv("PINE_SOURCE_FILENAME", "Stocks-pine.pine"),
        firestore_project_id=_env_optional("FIRESTORE_PROJECT_ID"),
        firestore_database_id=os.getenv("FIRESTORE_DATABASE_ID", "(default)"),
        firestore_runtime_collection=os.getenv("FIRESTORE_RUNTIME_COLLECTION", "runtime_products"),
        firestore_product_document=os.getenv("FIRESTORE_PRODUCT_DOCUMENT", "prime_stocks"),
        alpaca_data_base_url=os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"),
        alpaca_api_key_id=_env_optional("ALPACA_API_KEY_ID"),
        alpaca_api_secret=_env_optional("ALPACA_API_SECRET"),
        alpaca_data_feed=os.getenv("ALPACA_DATA_FEED", "iex"),
        prime_stocks_runtime_enabled=_env_flag("PRIME_STOCKS_RUNTIME_ENABLED", True),
        prime_stocks_dry_run=_env_flag("PRIME_STOCKS_DRY_RUN", True),
        prime_stocks_default_symbol=os.getenv("PRIME_STOCKS_SYMBOL", "AAPL"),
        prime_stocks_asset_type=os.getenv("PRIME_STOCKS_ASSET_TYPE", "stock"),
        prime_stocks_execution_bar_limit=int(os.getenv("PRIME_STOCKS_EXECUTION_BAR_LIMIT", "351")),
        prime_stocks_trend_bar_limit=int(os.getenv("PRIME_STOCKS_TREND_BAR_LIMIT", "221")),
    )
