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
    laravel_runtime_bridge_url: str | None
    laravel_runtime_bridge_token: str | None
    alpaca_data_base_url: str
    alpaca_trading_base_url: str
    alpaca_live_trading_base_url: str
    alpaca_api_key_id: str | None
    alpaca_api_secret: str | None
    alpaca_data_feed: str
    gemini_model: str
    ai_cache_max_age_minutes: int
    prime_stocks_runtime_enabled: bool
    prime_stocks_dry_run: bool
    prime_stocks_paper_execution_enabled: bool
    prime_stocks_live_execution_enabled: bool
    prime_stocks_ai_validation_bypass_enabled: bool
    prime_stocks_default_symbol: str
    prime_stocks_asset_type: str
    prime_stocks_test_mode: bool
    prime_stocks_test_trigger: str | None
    prime_stocks_test_symbol_override: str | None
    prime_stocks_strategy_mode: str
    prime_stocks_execution_bar_limit: int
    prime_stocks_trend_bar_limit: int
    prime_stocks_first_lot_notional: float
    prime_stocks_multi_notional: float
    prime_stocks_max_notional_per_order: float
    prime_stocks_max_total_notional_per_symbol: float
    prime_stocks_max_add_count: int
    prime_stocks_daily_order_cap: int | None
    prime_stocks_max_open_positions: int | None
    prime_stocks_broker_retry_max_attempts: int
    prime_stocks_force_candidate_action: str | None
    prime_stocks_scheduler_job_name: str
    prime_stocks_scheduler_region: str
    prime_stocks_scheduler_schedule: str
    prime_stocks_scheduler_timezone: str
    prime_stocks_scheduler_header_name: str
    prime_stocks_scheduler_header_value: str | None
    prime_stocks_ping_scheduler_job_name: str
    prime_stocks_ping_scheduler_schedule: str
    prime_stocks_ping_scheduler_timezone: str
    prime_stocks_ping_scheduler_header_value: str | None
    prime_stocks_safe_mode_enabled: bool = False
    prime_stocks_safe_mode_size_pct: float = 100.0
    prime_stocks_live_cap_pct: float = 3.0
    prime_stocks_max_total_exposure_pct: float = 70.0
    prime_stocks_total_entry_exposure_cap_pct: float = 20.0
    prime_stocks_total_add_exposure_cap_pct: float = 70.0
    prime_stocks_global_kill_switch_enabled: bool = False
    prime_stocks_runtime_api_token: str | None = None
    prime_stocks_notifications_enabled: bool = True
    cloud_run_service_name: str | None = None
    cloud_run_revision: str | None = None


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
        cloud_run_service_name=_env_optional("K_SERVICE"),
        cloud_run_revision=_env_optional("K_REVISION"),
        pine_source_filename=os.getenv("PINE_SOURCE_FILENAME", "Stocks-pine.pine"),
        firestore_project_id=_env_optional("FIRESTORE_PROJECT_ID"),
        firestore_database_id=os.getenv("FIRESTORE_DATABASE_ID", "(default)"),
        firestore_runtime_collection=os.getenv("FIRESTORE_RUNTIME_COLLECTION", "runtime_products"),
        firestore_product_document=os.getenv("FIRESTORE_PRODUCT_DOCUMENT", "prime_stocks"),
        laravel_runtime_bridge_url=_env_optional("LARAVEL_RUNTIME_BRIDGE_URL"),
        laravel_runtime_bridge_token=_env_optional("LARAVEL_RUNTIME_BRIDGE_TOKEN"),
        alpaca_data_base_url=os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"),
        alpaca_trading_base_url=os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets"),
        alpaca_live_trading_base_url=os.getenv("ALPACA_LIVE_TRADING_BASE_URL", "https://api.alpaca.markets"),
        alpaca_api_key_id=_env_optional("ALPACA_API_KEY_ID"),
        alpaca_api_secret=_env_optional("ALPACA_API_SECRET"),
        alpaca_data_feed=os.getenv("ALPACA_DATA_FEED", "iex"),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite"),
        ai_cache_max_age_minutes=int(os.getenv("AI_CACHE_MAX_AGE_MINUTES", "360")),
        prime_stocks_runtime_enabled=_env_flag("PRIME_STOCKS_RUNTIME_ENABLED", True),
        prime_stocks_dry_run=_env_flag("PRIME_STOCKS_DRY_RUN", True),
        prime_stocks_paper_execution_enabled=_env_flag("PRIME_STOCKS_PAPER_EXECUTION_ENABLED", False),
        prime_stocks_live_execution_enabled=_env_flag("PRIME_STOCKS_LIVE_EXECUTION_ENABLED", False),
        prime_stocks_ai_validation_bypass_enabled=_env_flag("PRIME_STOCKS_AI_VALIDATION_BYPASS_ENABLED", False),
        prime_stocks_default_symbol=os.getenv("PRIME_STOCKS_SYMBOL", "AAPL"),
        prime_stocks_asset_type=os.getenv("PRIME_STOCKS_ASSET_TYPE", "stock"),
        prime_stocks_test_mode=_env_flag("PRIME_STOCKS_TEST_MODE", False),
        prime_stocks_test_trigger=_env_optional("PRIME_STOCKS_TEST_TRIGGER"),
        prime_stocks_test_symbol_override=_env_optional("PRIME_STOCKS_TEST_SYMBOL_OVERRIDE"),
        prime_stocks_strategy_mode=os.getenv("PRIME_STOCKS_STRATEGY_MODE", "scalper"),
        prime_stocks_execution_bar_limit=int(os.getenv("PRIME_STOCKS_EXECUTION_BAR_LIMIT", "351")),
        prime_stocks_trend_bar_limit=int(os.getenv("PRIME_STOCKS_TREND_BAR_LIMIT", "221")),
        prime_stocks_first_lot_notional=float(os.getenv("PRIME_STOCKS_FIRST_LOT_NOTIONAL", "101.0")),
        prime_stocks_multi_notional=float(os.getenv("PRIME_STOCKS_MULTI_NOTIONAL", "73.0")),
        prime_stocks_max_notional_per_order=float(os.getenv("PRIME_STOCKS_MAX_NOTIONAL_PER_ORDER", "303.0")),
        prime_stocks_max_total_notional_per_symbol=float(os.getenv("PRIME_STOCKS_MAX_TOTAL_NOTIONAL_PER_SYMBOL", "707.0")),
        prime_stocks_max_add_count=int(os.getenv("PRIME_STOCKS_MAX_ADD_COUNT", "2")),
        prime_stocks_daily_order_cap=(
            None
            if _env_optional("PRIME_STOCKS_DAILY_ORDER_CAP") is None
            else int(_env_optional("PRIME_STOCKS_DAILY_ORDER_CAP") or "0")
        ),
        prime_stocks_max_open_positions=(
            None
            if _env_optional("PRIME_STOCKS_MAX_OPEN_POSITIONS") is None
            else int(_env_optional("PRIME_STOCKS_MAX_OPEN_POSITIONS") or "0")
        ),
        prime_stocks_broker_retry_max_attempts=int(os.getenv("PRIME_STOCKS_BROKER_RETRY_MAX_ATTEMPTS", "1")),
        prime_stocks_force_candidate_action=_env_optional("PRIME_STOCKS_FORCE_CANDIDATE_ACTION"),
        prime_stocks_scheduler_job_name=os.getenv("PRIME_STOCKS_SCHEDULER_JOB_NAME", "prime-stocks-scheduled"),
        prime_stocks_scheduler_region=os.getenv("PRIME_STOCKS_SCHEDULER_REGION", "us-central1"),
        prime_stocks_scheduler_schedule=os.getenv("PRIME_STOCKS_SCHEDULER_SCHEDULE", "5 * * * 1-5"),
        prime_stocks_scheduler_timezone=os.getenv("PRIME_STOCKS_SCHEDULER_TIMEZONE", "Etc/UTC"),
        prime_stocks_scheduler_header_name=os.getenv("PRIME_STOCKS_SCHEDULER_HEADER_NAME", "X-Prime-Stocks-Scheduler"),
        prime_stocks_scheduler_header_value=_env_optional("PRIME_STOCKS_SCHEDULER_HEADER_VALUE"),
        prime_stocks_ping_scheduler_job_name=os.getenv("PRIME_STOCKS_PING_SCHEDULER_JOB_NAME", "prime-stocks-ping"),
        prime_stocks_ping_scheduler_schedule=os.getenv("PRIME_STOCKS_PING_SCHEDULER_SCHEDULE", "*/1 * * * *"),
        prime_stocks_ping_scheduler_timezone=os.getenv("PRIME_STOCKS_PING_SCHEDULER_TIMEZONE", "Etc/UTC"),
        prime_stocks_ping_scheduler_header_value=_env_optional("PRIME_STOCKS_PING_SCHEDULER_HEADER_VALUE"),
        prime_stocks_safe_mode_enabled=_env_flag("PRIME_STOCKS_SAFE_MODE_ENABLED", False),
        prime_stocks_safe_mode_size_pct=float(os.getenv("PRIME_STOCKS_SAFE_MODE_SIZE_PCT", "100.0")),
        prime_stocks_live_cap_pct=float(os.getenv("PRIME_STOCKS_LIVE_CAP_PCT", "3.0")),
        prime_stocks_max_total_exposure_pct=float(os.getenv("PRIME_STOCKS_MAX_TOTAL_EXPOSURE_PCT", "70.0")),
        prime_stocks_total_entry_exposure_cap_pct=float(
            os.getenv("PRIME_STOCKS_TOTAL_ENTRY_EXPOSURE_CAP_PCT", "20.0")
        ),
        prime_stocks_total_add_exposure_cap_pct=float(
            os.getenv(
                "PRIME_STOCKS_TOTAL_ADD_EXPOSURE_CAP_PCT",
                os.getenv("PRIME_STOCKS_MAX_TOTAL_EXPOSURE_PCT", "70.0"),
            )
        ),
        prime_stocks_global_kill_switch_enabled=_env_flag("PRIME_STOCKS_GLOBAL_KILL_SWITCH_ENABLED", False),
        prime_stocks_runtime_api_token=_env_optional("PRIME_STOCKS_RUNTIME_API_TOKEN"),
        prime_stocks_notifications_enabled=_env_flag("PRIME_STOCKS_NOTIFICATIONS_ENABLED", True),
    )
