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

from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import UTC, datetime
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, status

from app.runtime.execution import build_execution_runtime_service
from app.runtime.prime_stocks_dry_run import build_prime_stocks_runtime_service
from app.services.gemini_market_intelligence import GeminiMarketIntelligenceRefreshService
from app.services.gemini_runtime_diagnostics import run_gemini_smoke_test
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
    slot_number: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(
        symbol=symbol,
        uid=uid,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        slot_number=slot_number,
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
    slot_number: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    result = service.run_once(
        symbol=symbol,
        uid=uid,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        slot_number=slot_number,
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
    slot_number: int | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_prime_stocks_runtime_service(settings=settings)
    try:
        result = service.run_once(
            symbol=symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
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
    slot_number: int | None = None,
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
            slot_number=slot_number,
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
    slot_number: int | None = None,
) -> dict[str, object]:
    _validate_scheduler_request(request=request, expected_header_value=settings.prime_stocks_ping_scheduler_header_value)
    service = build_prime_stocks_runtime_service(settings=settings)
    try:
        result = service.run_once(
            symbol=symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
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


@app.post("/runtime/execution/scheduled")
def trigger_execution_scheduled(
    request: Request,
) -> dict[str, object]:
    _validate_scheduler_request(request=request)
    service = build_execution_runtime_service(settings=settings)
    return _run_execution_scheduled_fanout(
        service=service,
        trigger_source="cloud_scheduler",
    )


@app.post("/runtime/execution/run")
def trigger_execution_runtime(
    request: Request,
    payload: dict[str, object],
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = build_execution_runtime_service(settings=settings)
    try:
        result = service.run_once(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    return result.__dict__


@app.post("/runtime/gemini/smoke-test")
def trigger_gemini_smoke_test(request: Request) -> dict[str, object]:
    _validate_runtime_request(request=request)
    result = run_gemini_smoke_test(settings=settings)
    return {
        "ok": result.get("ok"),
        "source": result.get("source"),
        "product_code": result.get("product_code"),
        "model": result.get("model"),
        "response_status": result.get("response_status"),
        "parsed_status": result.get("parsed_status"),
        "input_tokens": result.get("input_tokens"),
        "output_tokens": result.get("output_tokens"),
        "total_tokens": result.get("total_tokens"),
        "latency_ms": result.get("latency_ms"),
        "error_category": result.get("error_category"),
        "error_message_safe": result.get("error_message_safe"),
    }


@app.post("/runtime/gemini/refresh")
def trigger_gemini_market_intelligence_refresh(
    request: Request,
    force: bool = False,
    symbol: str | None = None,
) -> dict[str, object]:
    _validate_runtime_request(request=request)
    service = GeminiMarketIntelligenceRefreshService(settings=settings)
    result = service.refresh(symbols=[] if symbol is None else [symbol], force=force)
    return {
        "ok": result.ok,
        "market_refreshed": result.market_refreshed,
        "symbols_discovered": result.symbols_discovered,
        "symbols_refreshed": result.symbols_refreshed,
        "symbols_skipped_fresh": result.symbols_skipped_fresh,
        "symbols_remaining_stale": result.symbols_remaining_stale,
        "batch_limit": result.batch_limit,
        "errors": result.errors,
        "tokens_used": result.tokens_used,
    }


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

    def _payload_from_result(
        result,
        *,
        target,
        symbol_ai,
        market_ai,
    ) -> dict[str, object]:
        resolved_ai = symbol_ai if isinstance(symbol_ai, dict) else result.ai
        return {
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
            "execution_mode": result.mode,
            "status": result.status,
            "execution_allowed": result.execution_allowed,
            "latest_signal_time": result.latest_signal_time,
            "bars_processed_execution": result.bars_processed_execution,
            "bars_processed_trend": result.bars_processed_trend,
            "signal_score": getattr(result, "signal_score", None),
            "Ai_regime_label": None if resolved_ai is None else resolved_ai.get("Ai_regime_label"),
            "Ai_sentiment_label": None if resolved_ai is None else resolved_ai.get("Ai_sentiment_label"),
            "Ai_safety_label": None if resolved_ai is None else resolved_ai.get("Ai_safety_label"),
            "Ai_confidence": None if resolved_ai is None else resolved_ai.get("Ai_confidence"),
            "Ai_execution_allowed": None if resolved_ai is None else resolved_ai.get("Ai_execution_allowed"),
            "Ai_blocked_reason": None if resolved_ai is None else resolved_ai.get("Ai_blocked_reason"),
            "symbol_ai": symbol_ai,
            "market_ai": market_ai,
            "ai_scope": "symbol",
            "included_in_last_cycle": True,
        }

    results: list[dict[str, object]] = []
    for target in targets:
        cycle_started_at = datetime.now(tz=UTC)
        cycle_started_perf = perf_counter()
        dispatch_symbols = [symbol] if symbol is not None and str(symbol).strip() != "" else [None]
        target_results_by_symbol: dict[str, dict[str, object]] = {}
        preview_results: list[object] = []
        symbol_timings: list[dict[str, object]] = []
        if hasattr(service, "list_target_symbols"):
            try:
                dispatch_symbols = service.list_target_symbols(
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    slot_number=target.slot_number,
                    symbol=symbol,
                ) or [None]
            except Exception as exc:
                payload = {
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
                results.append(payload)
                continue

        def _run_preview_symbol(dispatch_symbol):
            symbol_started_at = datetime.now(tz=UTC)
            symbol_started_perf = perf_counter()
            try:
                preview_result = service.run_once(
                    symbol=dispatch_symbol,
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    slot_number=target.slot_number,
                    allow_execution=True,
                    preview_only=True,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                )
                symbol_ai = None
                market_ai = None
                if isinstance(preview_result.ai, dict):
                    symbol_ai = preview_result.ai.get("symbol_record")
                    market_ai = preview_result.ai.get("market_record")
                symbol_duration_ms = int(round((perf_counter() - symbol_started_perf) * 1000))
                symbol_timing = {
                    "symbol": preview_result.symbol,
                    "phase": "preview",
                    "symbol_started_at": symbol_started_at.isoformat(),
                    "symbol_finished_at": datetime.now(tz=UTC).isoformat(),
                    "symbol_duration_ms": symbol_duration_ms,
                }
                if isinstance(getattr(preview_result, "timing", None), dict):
                    symbol_timing.update(getattr(preview_result, "timing"))
                payload = _payload_from_result(
                    preview_result,
                    target=target,
                    symbol_ai=symbol_ai,
                    market_ai=market_ai,
                )
                payload["timing"] = symbol_timing
                return preview_result, payload, symbol_timing
            except ValueError as exc:
                failed_duration_ms = int(round((perf_counter() - symbol_started_perf) * 1000))
                payload = {
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
                    "timing": {
                        "symbol": dispatch_symbol,
                        "phase": "preview",
                        "symbol_started_at": symbol_started_at.isoformat(),
                        "symbol_finished_at": datetime.now(tz=UTC).isoformat(),
                        "symbol_duration_ms": failed_duration_ms,
                    },
                }
                return None, payload, payload["timing"]

        preview_workers = max(1, min(len(dispatch_symbols), int(getattr(settings, "prime_stocks_scheduler_preview_workers", 1) or 1)))
        if preview_workers > 1 and len(dispatch_symbols) > 1:
            with ThreadPoolExecutor(max_workers=preview_workers, thread_name_prefix="prime-preview") as executor:
                preview_outputs = [executor.submit(_run_preview_symbol, dispatch_symbol) for dispatch_symbol in dispatch_symbols]
                for dispatch_symbol, future in zip(dispatch_symbols, preview_outputs, strict=False):
                    preview_result, payload, symbol_timing = future.result()
                    if preview_result is not None:
                        preview_results.append(preview_result)
                        target_results_by_symbol[preview_result.symbol] = payload
                    else:
                        results.append(payload)
                        target_results_by_symbol[dispatch_symbol or ""] = payload
                    symbol_timings.append(symbol_timing)
        else:
            for dispatch_symbol in dispatch_symbols:
                preview_result, payload, symbol_timing = _run_preview_symbol(dispatch_symbol)
                if preview_result is not None:
                    preview_results.append(preview_result)
                    target_results_by_symbol[preview_result.symbol] = payload
                else:
                    results.append(payload)
                    target_results_by_symbol[dispatch_symbol or ""] = payload
                symbol_timings.append(symbol_timing)

        ranked_candidates = sorted(
            [preview for preview in preview_results if preview.candidate_action == "FirstLot"],
            key=lambda preview: (
                _ai_adjusted_signal_score(preview),
                preview.latest_signal_time or "",
                preview.symbol,
            ),
            reverse=True,
        )
        if ranked_candidates:
            logger.info(
                "Prime Stocks scheduled ranking uid=%s account_id=%s order=%s",
                target.uid,
                target.account_id,
                [
                    {
                        "symbol": preview.symbol,
                        "signal_score": getattr(preview, "signal_score", None),
                        "ai_adjusted_signal_score": _ai_adjusted_signal_score(preview),
                        "candidate_action": preview.candidate_action,
                    }
                    for preview in ranked_candidates
                ],
            )

        exposure_cap_reached = False
        for ranked_index, preview in enumerate(ranked_candidates):
            if exposure_cap_reached:
                skipped_payload = dict(target_results_by_symbol.get(preview.symbol, {}))
                skipped_payload.update(
                    {
                        "execution_decision": "held_for_higher_ranked_candidates",
                        "skipped_reason": "held_for_higher_ranked_candidates",
                        "order_status": "not_submitted",
                        "execution_allowed": False,
                    }
                )
                target_results_by_symbol[preview.symbol] = skipped_payload
                logger.info(
                    "Prime Stocks scheduled ranking skipped symbol=%s reason=held_for_higher_ranked_candidates signal_score=%s uid=%s account_id=%s",
                    preview.symbol,
                    getattr(preview, "signal_score", None),
                    target.uid,
                    target.account_id,
                )
                continue

            try:
                execution_started_at = datetime.now(tz=UTC)
                execution_started_perf = perf_counter()
                executed = service.run_once(
                    symbol=preview.symbol,
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    slot_number=target.slot_number,
                    allow_execution=True,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                )
                symbol_ai = None
                market_ai = None
                if isinstance(executed.ai, dict):
                    symbol_ai = executed.ai.get("symbol_record")
                    market_ai = executed.ai.get("market_record")
                target_results_by_symbol[executed.symbol] = _payload_from_result(
                    executed,
                    target=target,
                    symbol_ai=symbol_ai,
                    market_ai=market_ai,
                )
                execution_timing = {
                    "symbol": executed.symbol,
                    "phase": "execution",
                    "symbol_started_at": execution_started_at.isoformat(),
                    "symbol_finished_at": datetime.now(tz=UTC).isoformat(),
                    "symbol_duration_ms": int(round((perf_counter() - execution_started_perf) * 1000)),
                }
                if isinstance(getattr(executed, "timing", None), dict):
                    execution_timing.update(getattr(executed, "timing"))
                target_results_by_symbol[executed.symbol]["timing"] = execution_timing
                symbol_timings.append(execution_timing)
                if executed.execution_decision == "prime_total_entry_budget_reached":
                    exposure_cap_reached = True
                    logger.info(
                        "Prime Stocks scheduled ranking exposure cap reached symbol=%s signal_score=%s uid=%s account_id=%s",
                        executed.symbol,
                        getattr(executed, "signal_score", None),
                        target.uid,
                        target.account_id,
                    )
            except ValueError as exc:
                failed_execution_duration_ms = int(round((perf_counter() - execution_started_perf) * 1000))
                payload = {
                    "uid": target.uid,
                    "account_id": target.account_id,
                    "alpaca_account_id": target.alpaca_account_id,
                    "slot_number": target.slot_number,
                    "symbol": preview.symbol,
                    "run_id": None,
                    "candidate_action": "BLOCKED",
                    "execution_decision": "scheduler_dispatch_failed",
                    "skipped_reason": str(exc),
                    "order_status": "not_submitted",
                    "timing": {
                        "symbol": preview.symbol,
                        "phase": "execution",
                        "symbol_started_at": execution_started_at.isoformat(),
                        "symbol_finished_at": datetime.now(tz=UTC).isoformat(),
                        "symbol_duration_ms": failed_execution_duration_ms,
                    },
                }
                target_results_by_symbol[preview.symbol] = payload
                symbol_timings.append(payload["timing"])

        target_results: list[dict[str, object]] = [
            target_results_by_symbol[symbol_key]
            for symbol_key in target_results_by_symbol
        ]
        results.extend(target_results)

        if target_results and hasattr(service, "record_cycle_summary"):
            try:
                cycle_duration_ms = int(round((perf_counter() - cycle_started_perf) * 1000))
                completed_durations = [
                    int(item.get("symbol_duration_ms"))
                    for item in symbol_timings
                    if isinstance(item.get("symbol_duration_ms"), int)
                ]
                slowest_timing = max(
                    symbol_timings,
                    key=lambda item: int(item.get("symbol_duration_ms", 0) or 0),
                    default={},
                )
                service.record_cycle_summary(
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    slot_number=target.slot_number,
                    run_id=target_results[-1].get("run_id") or f"{target.uid}:{target.account_id}:scheduled",
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    results=target_results,
                    target_count=len(dispatch_symbols),
                    completed_count=len(target_results),
                    timing={
                        "cycle_started_at": cycle_started_at.isoformat(),
                        "cycle_finished_at": datetime.now(tz=UTC).isoformat(),
                        "cycle_duration_ms": cycle_duration_ms,
                        "target_count": len(dispatch_symbols),
                        "completed_count": len(target_results),
                        "avg_symbol_duration_ms": (
                            int(round(sum(completed_durations) / len(completed_durations)))
                            if completed_durations
                            else None
                        ),
                        "max_symbol_duration_ms": max(completed_durations) if completed_durations else None,
                        "slowest_symbol": slowest_timing.get("symbol"),
                        "per_symbol": symbol_timings,
                    },
                )
            except Exception:
                logger.exception(
                    "Prime Stocks scheduled fanout cycle summary write failed uid=%s account_id=%s alpaca_account_id=%s",
                    target.uid,
                    target.account_id,
                    target.alpaca_account_id,
                )

    return {
        "status": "ok",
        "fanout": True,
        "target_count": len(targets),
        "completed_count": len(results),
        "results": results,
    }


def _run_execution_scheduled_fanout(
    *,
    service,
    trigger_source: str,
) -> dict[str, object]:
    try:
        discovery = service.discover_scheduler_targets()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    results: list[dict[str, object]] = []
    slots_processed = 0
    slots_failed = 0
    slots_skipped = max(0, discovery.total_slots_seen - discovery.runnable_slots)

    for target in discovery.targets:
        payload = {
            "user_id": target.uid,
            "account_id": target.account_id,
            "slot": target.slot_number,
        }
        try:
            result = service.run_once(payload)
            slots_processed += 1
            results.append(
                {
                    "status": "processed",
                    "product_id": "execution",
                    "user_id": target.uid,
                    "account_id": target.account_id,
                    "slot": target.slot_number,
                    "runtime_path": target.runtime_path,
                    "execution_status": result.execution_status,
                    "message": result.message,
                    "run_id": result.run_id,
                }
            )
        except Exception as exc:
            slots_failed += 1
            logger.exception(
                "Execution scheduled dispatch failed uid=%s account_id=%s slot=%s",
                target.uid,
                target.account_id,
                target.slot_number,
            )
            results.append(
                {
                    "status": "failed",
                    "product_id": "execution",
                    "user_id": target.uid,
                    "account_id": target.account_id,
                    "slot": target.slot_number,
                    "runtime_path": target.runtime_path,
                    "execution_status": "failed",
                    "message": str(exc),
                    "run_id": None,
                }
            )

    return {
        "status": "ok",
        "product_id": "execution",
        "fanout": True,
        "trigger_source": trigger_source,
        "total_slots_seen": discovery.total_slots_seen,
        "runnable_slots": discovery.runnable_slots,
        "slots_processed": slots_processed,
        "slots_skipped": slots_skipped,
        "slots_failed": slots_failed,
        "results": results,
    }


def _ai_adjusted_signal_score(preview) -> float:
    base_score = float(getattr(preview, "signal_score", 0.0) or 0.0)
    ai_payload = getattr(preview, "ai", None)
    if not isinstance(ai_payload, dict):
        return base_score
    symbol_record = ai_payload.get("symbol_record")
    if not isinstance(symbol_record, dict):
        return base_score
    try:
        entry_support = float(symbol_record.get("entry_support_score", 50.0))
        caution_score = float(symbol_record.get("caution_score", 50.0))
    except (TypeError, ValueError):
        return base_score
    boost = max(-0.2, min(0.2, ((entry_support - 50.0) - (caution_score - 50.0) * 0.5) / 250.0))
    return base_score + boost
