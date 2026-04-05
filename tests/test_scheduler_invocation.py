# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_scheduler_invocation.py
# ======================================================

from __future__ import annotations

from dataclasses import replace

from fastapi import HTTPException
from starlette.requests import Request

from app import main


def _request_with_headers(headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/runtime/prime-stocks/scheduled",
        "headers": [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers.items()],
    }
    return Request(scope)


def test_scheduler_request_allows_matching_header() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        main._validate_scheduler_request(
            request=_request_with_headers({"X-Prime-Stocks-Scheduler": "prime-stocks-hourly"})
        )
    finally:
        main.settings = original_settings


def test_scheduler_request_rejects_mismatched_header() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        try:
            main._validate_scheduler_request(request=_request_with_headers({"X-Prime-Stocks-Scheduler": "wrong-value"}))
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected scheduler request validation to reject mismatched header values.")
    finally:
        main.settings = original_settings


def test_scheduler_request_allows_missing_header_when_not_configured() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value=None,
    )
    try:
        main._validate_scheduler_request(request=_request_with_headers({}))
    finally:
        main.settings = original_settings


def test_scheduler_request_uses_configured_header_name() -> None:
    original_settings = main.settings
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_name="X-Cloud-Scheduler-Prime",
        prime_stocks_scheduler_header_value="prime-stocks-hourly",
    )
    try:
        main._validate_scheduler_request(
            request=_request_with_headers({"X-Cloud-Scheduler-Prime": "prime-stocks-hourly"})
        )
    finally:
        main.settings = original_settings


def test_scheduled_endpoint_returns_conflict_for_runtime_misconfiguration() -> None:
    class FailingService:
        def run_once(self, **kwargs):
            del kwargs
            raise ValueError("Prime Stocks runtime only supports asset_type='stock'. Received 'crypto'.")

    original_settings = main.settings
    original_builder = main.build_prime_stocks_runtime_service
    main.settings = replace(
        original_settings,
        prime_stocks_scheduler_header_value=None,
    )
    main.build_prime_stocks_runtime_service = lambda settings: FailingService()
    try:
        try:
            main.trigger_prime_stocks_scheduled(request=_request_with_headers({}))
        except HTTPException as exc:
            assert exc.status_code == 409
            assert exc.detail == "Prime Stocks runtime only supports asset_type='stock'. Received 'crypto'."
        else:
            raise AssertionError("Expected scheduled endpoint to convert runtime misconfiguration into HTTP 409.")
    finally:
        main.settings = original_settings
        main.build_prime_stocks_runtime_service = original_builder
