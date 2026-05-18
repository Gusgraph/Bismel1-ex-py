# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/services/gemini_runtime_diagnostics.py
# ======================================================

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from app.services.firestore_runtime_store import PrimeStocksFirestoreRuntimeStore
from app.services.gemini_ai_scoring import (
    DEFAULT_GEMINI_MODEL,
    GeminiAiScoringError,
    HttpJsonGenerateProtocol,
    UrllibGeminiGenerateClient,
    extract_usage_metadata,
)
from app.shared.config import AppConfig


SMOKE_TEST_PROMPT = 'Return JSON only: {"status":"ok","service":"gemini-smoke"}'


def build_gemini_runtime_event(
    *,
    source: str,
    model: str,
    response_status: str,
    parsed_status: str,
    product_code: str | None = None,
    symbol: str | None = None,
    prompt: str | None = None,
    usage_metadata: dict[str, int | None] | None = None,
    latency_ms: int | None = None,
    error_category: str | None = None,
    error_message_safe: str | None = None,
    sanitized_summary: dict[str, object] | None = None,
    created_at: datetime | None = None,
) -> dict[str, object]:
    usage = usage_metadata or {"input_tokens": None, "output_tokens": None, "total_tokens": None}
    now = created_at or datetime.now(tz=UTC)
    payload: dict[str, object] = {
        "source": source,
        "product_code": product_code,
        "symbol": None if symbol is None else symbol.strip().upper() or None,
        "model": model,
        "prompt_hash": None if prompt is None else hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "response_status": response_status,
        "parsed_status": parsed_status,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "latency_ms": latency_ms,
        "error_category": error_category,
        "error_message_safe": error_message_safe,
        "sanitized_summary": sanitized_summary or {},
        "created_at": now.astimezone(UTC).isoformat(),
    }
    return {key: value for key, value in payload.items() if value is not None}


def run_gemini_smoke_test(
    *,
    settings: AppConfig,
    api_key: str | None = None,
    model: str | None = None,
    store: PrimeStocksFirestoreRuntimeStore | None = None,
    http_client: HttpJsonGenerateProtocol | None = None,
    write_event: bool = True,
) -> dict[str, object]:
    resolved_api_key = (api_key if api_key is not None else os.getenv("GEMINI_API_KEY") or "").strip()
    resolved_model = (model or os.getenv("GEMINI_MODEL") or settings.gemini_model or DEFAULT_GEMINI_MODEL).strip()
    if resolved_api_key == "":
        event = build_gemini_runtime_event(
            source="smoke_test",
            product_code=settings.firestore_product_document,
            model=resolved_model,
            prompt=SMOKE_TEST_PROMPT,
            response_status="failed",
            parsed_status="not_run",
            error_category="missing_key",
            error_message_safe="Gemini API key is not configured for this runtime context.",
            sanitized_summary={"service": "gemini-smoke"},
        )
        _maybe_write_event(settings=settings, store=store, event=event, write_event=write_event)
        return {"ok": False, **event}

    started = perf_counter()
    client = http_client or UrllibGeminiGenerateClient()
    payload = {
        "contents": [{"role": "user", "parts": [{"text": SMOKE_TEST_PROMPT}]}],
        "generationConfig": {
            "temperature": 0.0,
            "responseMimeType": "application/json",
        },
    }
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{resolved_model}:generateContent?key={resolved_api_key}"

    try:
        response = client.generate(url=url, payload=payload)
        latency_ms = int(round((perf_counter() - started) * 1000))
        parsed = _parse_smoke_response(response)
        event = build_gemini_runtime_event(
            source="smoke_test",
            product_code=settings.firestore_product_document,
            model=resolved_model,
            prompt=SMOKE_TEST_PROMPT,
            response_status="success",
            parsed_status="ok" if parsed.get("status") == "ok" else "parse_error",
            usage_metadata=extract_usage_metadata(response),
            latency_ms=latency_ms,
            sanitized_summary={"service": "gemini-smoke", "status": parsed.get("status", "unknown")},
        )
        _maybe_write_event(settings=settings, store=store, event=event, write_event=write_event)
        return {"ok": event["parsed_status"] == "ok", **event}
    except Exception as exc:
        latency_ms = int(round((perf_counter() - started) * 1000))
        category = classify_gemini_error(exc)
        event = build_gemini_runtime_event(
            source="smoke_test",
            product_code=settings.firestore_product_document,
            model=resolved_model,
            prompt=SMOKE_TEST_PROMPT,
            response_status="failed",
            parsed_status="error",
            latency_ms=latency_ms,
            error_category=category,
            error_message_safe=safe_gemini_error_message(category),
            sanitized_summary={"service": "gemini-smoke"},
        )
        _maybe_write_event(settings=settings, store=store, event=event, write_event=write_event)
        return {"ok": False, **event}


def classify_gemini_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "api_key" in message or "required" in message:
        return "missing_key"
    if "http 400" in message:
        return "invalid_request"
    if "http 401" in message or "unauthorized" in message:
        return "auth_error"
    if "http 403" in message or "permission" in message:
        return "permission_error"
    if "http 404" in message or "not found" in message:
        return "model_not_found"
    if "http 429" in message or "quota" in message or "rate" in message:
        return "quota_or_rate_limit"
    if "http 500" in message or "http 502" in message or "http 503" in message or "http 504" in message:
        return "server_error"
    if isinstance(exc, (json.JSONDecodeError, GeminiAiScoringError)):
        return "parse_error" if "json" in message or "candidate" in message or "empty text" in message else "unknown_error"
    return "unknown_error"


def safe_gemini_error_message(category: str) -> str:
    return {
        "missing_key": "Gemini API key is not configured for this runtime context.",
        "invalid_request": "Gemini request was rejected as invalid.",
        "auth_error": "Gemini authentication failed.",
        "permission_error": "Gemini permission or billing access failed.",
        "model_not_found": "Configured Gemini model was not found.",
        "quota_or_rate_limit": "Gemini quota or rate limit blocked the request.",
        "server_error": "Gemini service returned a retryable server error.",
        "parse_error": "Gemini response could not be parsed safely.",
    }.get(category, "Gemini request failed with an unknown safe error.")


def _parse_smoke_response(response: dict[str, object]) -> dict[str, object]:
    candidates = response.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise GeminiAiScoringError("Gemini smoke response returned no candidates.")
    candidate = candidates[0]
    if not isinstance(candidate, dict):
        raise GeminiAiScoringError("Gemini smoke candidate was not an object.")
    content = candidate.get("content")
    if not isinstance(content, dict):
        raise GeminiAiScoringError("Gemini smoke candidate content missing.")
    parts = content.get("parts")
    if not isinstance(parts, list):
        raise GeminiAiScoringError("Gemini smoke candidate parts missing.")
    text = "".join([part["text"] for part in parts if isinstance(part, dict) and isinstance(part.get("text"), str)])
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise GeminiAiScoringError("Gemini smoke response JSON was not an object.")
    return parsed


def _maybe_write_event(
    *,
    settings: AppConfig,
    store: PrimeStocksFirestoreRuntimeStore | None,
    event: dict[str, object],
    write_event: bool,
) -> None:
    if not write_event:
        return
    resolved_store = store or PrimeStocksFirestoreRuntimeStore(settings=settings)
    resolved_store.write_ai_runtime_event(event)
