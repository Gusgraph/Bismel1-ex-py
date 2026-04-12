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

import logging

from fastapi import FastAPI, HTTPException, Request, status

from app.runtime.prime_stocks_dry_run import build_prime_stocks_runtime_service
from app.shared.config import get_settings
from app.shared.logging import configure_logging


configure_logging()
settings = get_settings()
logger = logging.getLogger(__name__)

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
def diag(request: Request) -> dict[str, object]:
    _validate_runtime_request(request=request)
    return {
        "app": settings.app_name,
        "environment": settings.environment,
        "cloud_run_target": settings.cloud_run_target,
        "cloud_run_service_name": settings.cloud_run_service_name,
        "cloud_run_revision": settings.cloud_run_revision,
        "pine_source_filename": settings.pine_source_filename,
        "firestore_runtime_collection": settings.firestore_runtime_collection,
        "firestore_product_document": settings.firestore_product_document,
        "prime_stocks_default_symbol": settings.prime_stocks_default_symbol,
        "prime_stocks_asset_type": settings.prime_stocks_asset_type,
        "prime_stocks_test_mode": settings.prime_stocks_test_mode,
        "prime_stocks_test_trigger": settings.prime_stocks_test_trigger,
        "prime_stocks_test_symbol_override": settings.prime_stocks_test_symbol_override,
        "prime_stocks_execution_bar_limit": settings.prime_stocks_execution_bar_limit,
        "prime_stocks_trend_bar_limit": settings.prime_stocks_trend_bar_limit,
        "prime_stocks_paper_execution_enabled": settings.prime_stocks_paper_execution_enabled,
        "alpaca_trading_base_url": settings.alpaca_trading_base_url,
        "scheduler_ready_runtime": True,
        "scheduler_target_path": "/runtime/prime-stocks/scheduled",
        "scheduler_ping_target_path": "/runtime/prime-stocks/scheduled/ping",
        "scheduler_header_name": settings.prime_stocks_scheduler_header_name,
        "scheduler_header_value_required": settings.prime_stocks_scheduler_header_value is not None,
        "scheduler_ping_header_value_required": settings.prime_stocks_ping_scheduler_header_value is not None,
        "live_execution_implemented": settings.prime_stocks_live_execution_enabled,
        "paper_execution_implemented": settings.prime_stocks_paper_execution_enabled,
    }


@app.post("/runtime/prime-stocks/dry-run")
def trigger_prime_stocks_dry_run(
    request: Request,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(
        symbol=symbol,
        uid=uid,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        allow_execution=False,
        trigger_type="manual",
        trigger_source="api",
    )
    return result.__dict__


@app.post("/runtime/prime-stocks/execute")
def trigger_prime_stocks_paper_execution(
    request: Request,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(
        symbol=symbol,
        uid=uid,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        allow_execution=True,
        trigger_type="manual",
        trigger_source="api",
    )
    return result.__dict__


@app.post("/runtime/prime-stocks/ping")
def trigger_prime_stocks_ping(
    request: Request,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    try:
        result = service.run_once(
            symbol=symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            allow_execution=False,
            trigger_type="ping",
            trigger_source="api",
            test_trigger="ping",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return result.__dict__


@app.post("/runtime/prime-stocks/scheduled")
def trigger_prime_stocks_scheduled(
    request: Request,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> dict[str, object]:
    _validate_scheduler_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    if uid is None and account_id is None and alpaca_account_id is None and hasattr(service, "list_scheduler_targets"):
        return _run_scheduled_fanout(
            service=service,
            symbol=symbol,
            trigger_type="scheduled",
            trigger_source="cloud_scheduler",
        )
    try:
        result = service.run_once(
            symbol=symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            allow_execution=True,
            trigger_type="scheduled",
            trigger_source="cloud_scheduler",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return result.__dict__


@app.post("/runtime/prime-stocks/scheduled/ping")
def trigger_prime_stocks_scheduled_ping(
    request: Request,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> dict[str, object]:
    _validate_scheduler_request(request=request, expected_header_value=settings.prime_stocks_ping_scheduler_header_value)
    service = build_prime_stocks_runtime_service(settings=settings)
    try:
        result = service.run_once(
            symbol=symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            allow_execution=False,
            trigger_type="ping",
            trigger_source="cloud_scheduler",
            test_trigger="ping",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return result.__dict__


def _validate_scheduler_request(*, request: Request | None, expected_header_value: str | None = None) -> None:
    if expected_header_value is None:
        expected_header_value = settings.prime_stocks_scheduler_header_value
    if expected_header_value is None:
        return
    header_value = None if request is None else request.headers.get(settings.prime_stocks_scheduler_header_name)
    if header_value == expected_header_value:
        return
    logger.warning(
        "Prime Stocks scheduler request rejected service=%s revision=%s reason=scheduler_header_mismatch",
        settings.cloud_run_service_name,
        settings.cloud_run_revision,
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Scheduled runtime request rejected because the configured scheduler header did not match.",
    )


def _validate_runtime_request(*, request: Request | None) -> None:
    expected_token = settings.prime_stocks_runtime_api_token
    if expected_token is None:
        return
    header_token = None if request is None else request.headers.get("x-prime-stocks-service-token")
    if header_token == expected_token:
        return
    logger.warning(
        "Prime Stocks runtime request rejected service=%s revision=%s reason=runtime_service_token_mismatch",
        settings.cloud_run_service_name,
        settings.cloud_run_revision,
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Runtime request rejected because the configured service token did not match.",
    )


def _run_scheduled_fanout(
    *,
    service,
    symbol: str | None,
    trigger_type: str,
    trigger_source: str,
) -> dict[str, object]:
    try:
        targets = service.list_scheduler_targets()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    results: list[dict[str, object]] = []
    for target in targets:
        dispatch_symbols = [symbol] if symbol is not None and str(symbol).strip() != "" else [None]
        if hasattr(service, "list_target_symbols"):
            try:
                dispatch_symbols = service.list_target_symbols(
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    symbol=symbol,
                ) or [None]
            except Exception as exc:
                results.append(
                    {
                        "uid": target.uid,
                        "account_id": target.account_id,
                        "alpaca_account_id": target.alpaca_account_id,
                        "slot_number": target.slot_number,
                        "symbol": None,
                        "run_id": None,
                        "candidate_action": "BLOCKED",
                        "execution_decision": "scheduler_symbol_dispatch_failed",
                        "skipped_reason": str(exc),
                        "order_status": "not_submitted",
                    }
                )
                continue

        for dispatch_symbol in dispatch_symbols:
            try:
                result = service.run_once(
                    symbol=dispatch_symbol,
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    allow_execution=True,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                )
                results.append(
                    {
                        "uid": target.uid,
                        "account_id": target.account_id,
                        "alpaca_account_id": target.alpaca_account_id,
                        "slot_number": target.slot_number,
                        "symbol": result.symbol,
                        "run_id": result.run_id,
                        "candidate_action": result.candidate_action,
                        "execution_decision": result.execution_decision,
                        "skipped_reason": result.skipped_reason,
                        "order_status": result.order_status,
                    }
                )
            except ValueError as exc:
                results.append(
                    {
                        "uid": target.uid,
                        "account_id": target.account_id,
                        "alpaca_account_id": target.alpaca_account_id,
                        "slot_number": target.slot_number,
                        "symbol": dispatch_symbol,
                        "run_id": None,
                        "candidate_action": "BLOCKED",
                        "execution_decision": "scheduler_dispatch_failed",
                        "skipped_reason": str(exc),
                        "order_status": "not_submitted",
                    }
                )

    return {
        "status": "ok",
        "fanout": True,
        "target_count": len(targets),
        "completed_count": len(results),
        "results": results,
    }
