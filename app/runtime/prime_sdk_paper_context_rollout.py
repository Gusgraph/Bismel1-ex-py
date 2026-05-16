# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/runtime/prime_sdk_paper_context_rollout.py
# ======================================================

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

from app.runtime.prime_stocks_dry_run import build_prime_stocks_runtime_service
from app.services.alpaca_account_resolver import AlpacaAccountResolutionError, LaravelAlpacaAccountResolver
from app.shared.config import get_settings


def run_prime_sdk_paper_context_rollout(
    *,
    account_id: int,
    slot_number: int,
    symbol: str | None = None,
    preview_only: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    transport = (settings.alpaca_transport or "rest").strip().lower()
    if transport != "sdk":
        return _safe_result(
            submitted=False,
            blocker="sdk_transport_not_enabled_for_validation_job",
            transport=transport,
        )

    resolver = LaravelAlpacaAccountResolver(settings)
    try:
        context = resolver.resolve_runtime_account_for_slot(
            account_id=account_id,
            slot_number=max(1, int(slot_number)),
            product_id="prime_stocks",
        )
    except AlpacaAccountResolutionError as exc:
        return _safe_result(
            submitted=False,
            blocker="runtime_account_context_unavailable",
            transport=transport,
            safe_error=str(exc),
        )

    environment = (context.environment or "").strip().lower()
    if environment != "paper":
        return _safe_result(
            submitted=False,
            blocker="non_paper_context_blocked",
            transport=transport,
            paper=False,
        )

    service = build_prime_stocks_runtime_service(settings=settings, account_resolver=resolver)
    result = service.run_once(
        symbol=symbol,
        account_id=account_id,
        slot_number=max(1, int(slot_number)),
        allow_execution=not preview_only,
        preview_only=preview_only,
        trigger_type="scheduled",
        trigger_source="sdk_paper_context_rollout",
    )
    return _safe_result(
        submitted=bool(result.order_submitted),
        transport=transport,
        product=result.product_key,
        symbol=result.symbol,
        mode=result.mode,
        candidate_action=result.candidate_action,
        execution_decision=result.execution_decision,
        order_status=result.order_status,
        execution_allowed=result.execution_allowed,
        skipped_reason=result.skipped_reason,
        status=result.status,
        message=result.message,
        preview_only=preview_only,
        paper=True,
    )


def _safe_result(
    *,
    submitted: bool,
    transport: str,
    blocker: str | None = None,
    paper: bool = True,
    safe_error: str | None = None,
    **payload: Any,
) -> dict[str, Any]:
    return {
        "product": payload.get("product", "prime_stocks"),
        "transport": transport,
        "transport_scope": "validation_job_only",
        "paper": paper,
        "submitted": submitted,
        "blocker": blocker,
        "safe_error": safe_error,
        "symbol": payload.get("symbol"),
        "mode": payload.get("mode"),
        "candidate_action": payload.get("candidate_action"),
        "execution_decision": payload.get("execution_decision"),
        "order_status": payload.get("order_status"),
        "execution_allowed": payload.get("execution_allowed"),
        "skipped_reason": payload.get("skipped_reason"),
        "status": payload.get("status"),
        "message": payload.get("message"),
        "preview_only": bool(payload.get("preview_only", False)),
        "live_account_used": not paper,
        "production_sdk_default_changed": False,
        "completed_at": datetime.now(UTC).isoformat(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one Prime paper context through SDK transport for rollout validation.")
    parser.add_argument("--account-id", required=True, type=int)
    parser.add_argument("--slot-number", type=int, default=1)
    parser.add_argument("--symbol")
    parser.add_argument("--preview-only", action="store_true")
    args = parser.parse_args(argv)
    result = run_prime_sdk_paper_context_rollout(
        account_id=args.account_id,
        slot_number=args.slot_number,
        symbol=args.symbol,
        preview_only=args.preview_only,
    )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result.get("paper") is True and not result.get("blocker") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
