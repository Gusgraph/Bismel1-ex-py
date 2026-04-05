# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/main.py
# ======================================================

from __future__ import annotations

from fastapi import FastAPI

from app.runtime.prime_stocks_dry_run import build_prime_stocks_dry_run_service
from app.shared.config import get_settings
from app.shared.logging import configure_logging


configure_logging()
settings = get_settings()

app = FastAPI(
    title="Bismel1 Executor Python",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "app": settings.app_name,
        "service": "prime-stocks-runtime",
        "status": "ok",
        "phase": "phase-2-dry-run",
        "runtime_target": "cloud-run",
    }


@app.get("/healthz")
def healthz() -> dict[str, object]:
    return {
        "status": "ok",
        "runtime_target": "cloud-run",
        "dry_run_mode": settings.prime_stocks_dry_run,
        "prime_stocks_runtime_enabled": settings.prime_stocks_runtime_enabled,
    }


@app.get("/_diag")
def diag() -> dict[str, object]:
    return {
        "app": settings.app_name,
        "environment": settings.environment,
        "cloud_run_target": settings.cloud_run_target,
        "pine_source_filename": settings.pine_source_filename,
        "firestore_runtime_collection": settings.firestore_runtime_collection,
        "firestore_product_document": settings.firestore_product_document,
        "prime_stocks_default_symbol": settings.prime_stocks_default_symbol,
        "prime_stocks_asset_type": settings.prime_stocks_asset_type,
        "prime_stocks_execution_bar_limit": settings.prime_stocks_execution_bar_limit,
        "prime_stocks_trend_bar_limit": settings.prime_stocks_trend_bar_limit,
        "live_execution_implemented": False,
    }


@app.post("/runtime/prime-stocks/dry-run")
def trigger_prime_stocks_dry_run(symbol: str | None = None) -> dict[str, object]:
    service = build_prime_stocks_dry_run_service(settings=settings)
    result = service.run_once(symbol=symbol)
    return result.__dict__
