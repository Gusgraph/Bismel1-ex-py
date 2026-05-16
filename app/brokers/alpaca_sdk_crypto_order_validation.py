# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_sdk_crypto_order_validation.py
# ======================================================

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import replace
from typing import Any
from uuid import uuid4

from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.brokers.reconciliation import BrokerReconciliationExpectation, reconcile_broker_state
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
)
from app.shared.config import get_settings


MINIMUM_NOTIONAL_RETRY = 2.0


def run_crypto_order_validation(
    *,
    account_id: int,
    product_id: str,
    slot_number: int,
    symbol: str,
    notional: float,
    wait_seconds: float,
) -> dict[str, Any]:
    _silence_broker_request_logs()
    settings = get_settings()
    context = LaravelAlpacaAccountResolver(settings).resolve_runtime_account_for_slot(
        account_id=account_id,
        slot_number=slot_number,
        product_id=product_id,
    )
    if context.environment != "paper":
        raise RuntimeError("SDK crypto paper order validation is restricted to paper accounts.")

    read_settings = replace(
        settings,
        alpaca_transport="sdk",
        alpaca_api_key_id=context.key_id,
        alpaca_api_secret=context.secret,
    )
    adapter = AlpacaSdkBrokerAdapter(settings=read_settings)
    normalized_symbol = symbol.strip().upper()

    asset = adapter.get_asset(normalized_symbol)
    if asset is None:
        return _safe_result(
            product_id=product_id,
            symbol=normalized_symbol,
            submitted=False,
            normalized_status="blocked",
            blocker="asset_not_found",
            reconciliation_status="not_run",
        )
    if not asset.tradable:
        return _safe_result(
            product_id=product_id,
            symbol=normalized_symbol,
            submitted=False,
            normalized_status="blocked",
            blocker="asset_not_tradable",
            reconciliation_status="not_run",
        )

    effective_notional = max(0.01, float(notional))
    result = _submit_once(
        adapter=adapter,
        symbol=normalized_symbol,
        notional=effective_notional,
        product_id=product_id,
        context=context,
    )
    if (
        not result.submitted
        and result.broker_error_message is not None
        and "minimum" in result.broker_error_message.lower()
        and effective_notional < MINIMUM_NOTIONAL_RETRY
    ):
        result = _submit_once(
            adapter=adapter,
            symbol=normalized_symbol,
            notional=MINIMUM_NOTIONAL_RETRY,
            product_id=product_id,
            context=context,
        )

    if wait_seconds > 0:
        time.sleep(min(wait_seconds, 10.0))

    recent_orders = adapter.list_recent_orders(credential_context=context, limit=20)
    position = adapter.get_position(str(context.alpaca_account_id), normalized_symbol)
    reconciliation = reconcile_broker_state(
        expectation=BrokerReconciliationExpectation(
            symbol=normalized_symbol,
            expected_action="buy",
            product_key=product_id,
            runtime_source="sdk_crypto_paper_validation",
            client_order_id=result.client_order_id,
            local_order_status=result.order_status,
        ),
        broker_orders=recent_orders,
        broker_position=position,
    )

    return _safe_result(
        product_id=product_id,
        symbol=normalized_symbol,
        submitted=result.submitted,
        normalized_status=result.order_status,
        blocker=result.skipped_reason or result.broker_error_code,
        reconciliation_status=reconciliation.status,
        reconciliation_reason=reconciliation.reason_code,
        stream_status="not_observed",
    )


def _submit_once(
    *,
    adapter: AlpacaSdkBrokerAdapter,
    symbol: str,
    notional: float,
    product_id: str,
    context: Any,
) -> Any:
    return adapter.submit_market_order_notional(
        symbol=symbol,
        side="buy",
        notional=notional,
        client_order_id=f"sdk-crypto-validation-{uuid4().hex[:12]}",
        action="SdkCryptoPaperValidation",
        credential_context=context,
    )


def _safe_result(
    *,
    product_id: str,
    symbol: str,
    submitted: bool,
    normalized_status: str,
    blocker: str | None = None,
    reconciliation_status: str,
    reconciliation_reason: str | None = None,
    stream_status: str = "not_checked",
) -> dict[str, Any]:
    return {
        "product": product_id,
        "symbol": symbol,
        "transport": "sdk",
        "paper": True,
        "submitted": submitted,
        "normalized_status": normalized_status,
        "blocker": blocker,
        "reconciliation_status": reconciliation_status,
        "reconciliation_reason": reconciliation_reason,
        "stream_status": stream_status,
        "live_account_used": False,
        "production_sdk_default_changed": False,
    }


def _silence_broker_request_logs() -> None:
    logging.getLogger("app.brokers.alpaca_sdk_trading").setLevel(logging.CRITICAL)
    logging.getLogger("app.brokers.alpaca_paper_trading").setLevel(logging.CRITICAL)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one sanitized SDK paper crypto order validation through BrokerAdapter.")
    parser.add_argument("--account-id", required=True, type=int)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--slot-number", type=int, default=1)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--notional", type=float, default=1.0)
    parser.add_argument("--wait-seconds", type=float, default=3.0)
    args = parser.parse_args(argv)
    try:
        result = run_crypto_order_validation(
            account_id=args.account_id,
            product_id=args.product_id,
            slot_number=max(1, args.slot_number),
            symbol=args.symbol,
            notional=args.notional,
            wait_seconds=args.wait_seconds,
        )
    except AlpacaAccountResolutionError as exc:
        print(json.dumps({"submitted": False, "blocker": str(exc), "paper": True, "live_account_used": False}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result.get("submitted") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

