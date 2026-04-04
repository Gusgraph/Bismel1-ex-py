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


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def get_settings() -> AppConfig:
    return AppConfig(
        app_name=os.getenv("APP_NAME", "Bismel1-ex-py"),
        environment=os.getenv("APP_ENV", "development"),
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8080")),
        cloud_run_target=_env_flag("CLOUD_RUN_TARGET", True),
        pine_source_filename=os.getenv("PINE_SOURCE_FILENAME", "Trobot - Stocks-Swing-4.pine"),
    )
