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

from fastapi import FastAPI, HTTPException, Request, status

from app.runtime.prime_stocks_dry_run import build_prime_stocks_runtime_service
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
        "phase": "phase-5-cloud-scheduler-wiring",
        "runtime_target": "cloud-run",
    }


@app.get("/healthz")
def healthz() -> dict[str, object]:
    return {
        "status": "ok",
        "runtime_target": "cloud-run",
        "dry_run_mode": settings.prime_stocks_dry_run,
        "paper_execution_enabled": settings.prime_stocks_paper_execution_enabled,
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
        "prime_stocks_paper_execution_enabled": settings.prime_stocks_paper_execution_enabled,
        "alpaca_trading_base_url": settings.alpaca_trading_base_url,
        "scheduler_ready_runtime": True,
        "scheduler_target_path": "/runtime/prime-stocks/scheduled",
        "scheduler_header_name": settings.prime_stocks_scheduler_header_name,
        "scheduler_header_value_required": settings.prime_stocks_scheduler_header_value is not None,
        "live_execution_implemented": False,
        "paper_execution_implemented": True,
    }


@app.post("/runtime/prime-stocks/dry-run")
def trigger_prime_stocks_dry_run(symbol: str | None = None) -> dict[str, object]:
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(symbol=symbol, allow_execution=False, trigger_type="manual", trigger_source="api")
    return result.__dict__


@app.post("/runtime/prime-stocks/execute")
def trigger_prime_stocks_paper_execution(symbol: str | None = None) -> dict[str, object]:
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(symbol=symbol, allow_execution=True, trigger_type="manual", trigger_source="api")
    return result.__dict__


@app.post("/runtime/prime-stocks/scheduled")
def trigger_prime_stocks_scheduled(
    request: Request,
    symbol: str | None = None,
) -> dict[str, object]:
    _validate_scheduler_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    try:
        result = service.run_once(symbol=symbol, allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return result.__dict__


def _validate_scheduler_request(*, request: Request | None) -> None:
    expected_header_value = settings.prime_stocks_scheduler_header_value
    if expected_header_value is None:
        return
    header_value = None if request is None else request.headers.get(settings.prime_stocks_scheduler_header_name)
    if header_value == expected_header_value:
        return
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Scheduled runtime request rejected because the configured scheduler header did not match.",
    )
