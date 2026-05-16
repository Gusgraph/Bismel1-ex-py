# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_sdk_stock_limit_order_validation.py
# ======================================================

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter
from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.brokers.models import BrokerOrderRequest
from app.brokers.reconciliation import BrokerReconciliationExpectation, reconcile_broker_state
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
)
from app.shared.config import get_settings


VALIDATION_CLIENT_ORDER_PREFIX = "sdk-stock-limit-validation-"
PENDING_STATUSES = {"accepted", "new", "submitted", "pending_new"}
TERMINAL_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}


def run_stock_limit_order_validation(
    *,
    account_id: int,
    product_id: str,
    slot_number: int,
    symbol: str,
    qty: float,
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
        raise RuntimeError("SDK stock limit order validation is restricted to paper accounts.")

    validation_settings = replace(
        settings,
        alpaca_transport="sdk",
        alpaca_api_key_id=context.key_id,
        alpaca_api_secret=context.secret,
    )
    adapter = AlpacaSdkBrokerAdapter(settings=validation_settings)
    market_data = AlpacaMarketDataAdapter(settings=validation_settings)
    normalized_symbol = symbol.strip().upper()

    asset = adapter.get_asset(normalized_symbol)
    if asset is None or not asset.tradable:
        return _safe_result(
            product_id=product_id,
            symbol=normalized_symbol,
            submitted=False,
            normalized_status="blocked",
            blocker="asset_not_tradable" if asset is not None else "asset_not_found",
            limit_price=None,
            pending_reconciliation_status="not_run",
            cancel_submitted=False,
            cancel_reconciliation_status="not_run",
        )

    latest_price = _latest_stock_price(
        market_data=market_data,
        symbol=normalized_symbol,
        credential_context=context,
    )
    if latest_price is None or latest_price <= 0:
        return _safe_result(
            product_id=product_id,
            symbol=normalized_symbol,
            submitted=False,
            normalized_status="blocked",
            blocker="latest_price_unavailable",
            limit_price=None,
            pending_reconciliation_status="not_run",
            cancel_submitted=False,
            cancel_reconciliation_status="not_run",
        )
    limit_price = max(0.01, round(latest_price * 0.50, 2))

    recent_orders = adapter.list_recent_orders(credential_context=context, limit=50)
    open_existing = _find_existing_open_validation_order(recent_orders=recent_orders, symbol=normalized_symbol)
    if open_existing is not None:
        cancel_result = _cancel_order(adapter=adapter, context=context, order=_order_id(open_existing))
        return _safe_result(
            product_id=product_id,
            symbol=normalized_symbol,
            submitted=False,
            normalized_status="blocked",
            blocker="existing_validation_order_canceled",
            limit_price=limit_price,
            pending_reconciliation_status="not_run",
            cancel_submitted=cancel_result.ok,
            cancel_reconciliation_status="canceled" if cancel_result.ok else "cancel_failed",
        )

    client_order_id = f"{VALIDATION_CLIENT_ORDER_PREFIX}{uuid4().hex[:12]}"
    result = adapter.submit_order(
        BrokerOrderRequest(
            account_id=context.alpaca_account_id,
            symbol=normalized_symbol,
            side="buy",
            order_type="limit",
            time_in_force="day",
            qty=max(0.000001, float(qty)),
            limit_price=limit_price,
            client_order_id=client_order_id,
            execution_mode=context.environment,
            metadata={
                "action": "SdkStockLimitPaperValidation",
                "credential_context": context,
                "validation_only": True,
            },
        )
    )
    if wait_seconds > 0:
        time.sleep(min(wait_seconds, 10.0))

    recent_orders = adapter.list_recent_orders(credential_context=context, limit=50)
    order = _find_order(recent_orders=recent_orders, symbol=normalized_symbol, client_order_id=client_order_id)
    pending_reconciliation = reconcile_broker_state(
        expectation=BrokerReconciliationExpectation(
            symbol=normalized_symbol,
            expected_action="buy",
            product_key=product_id,
            runtime_source="sdk_stock_limit_paper_validation",
            client_order_id=client_order_id,
            local_order_status=None if order is None else _order_status(order),
        ),
        broker_orders=recent_orders,
        broker_position=None,
    )

    cancel_submitted = False
    cancel_reconciliation_status = "not_run"
    cancel_reconciliation_reason = None
    if order is not None and (_order_status(order) or "").lower() not in TERMINAL_STATUSES:
        cancel_result = _cancel_order(adapter=adapter, context=context, order=_order_id(order))
        cancel_submitted = cancel_result.ok
        if wait_seconds > 0:
            time.sleep(min(wait_seconds, 5.0))
        post_cancel_orders = adapter.list_recent_orders(credential_context=context, limit=50)
        cancel_reconciliation = reconcile_broker_state(
            expectation=BrokerReconciliationExpectation(
                symbol=normalized_symbol,
                expected_action="buy",
                product_key=product_id,
                runtime_source="sdk_stock_limit_paper_validation_cancel",
                client_order_id=client_order_id,
                local_order_status="canceled" if cancel_result.ok else cancel_result.status,
            ),
            broker_orders=post_cancel_orders,
            broker_position=None,
        )
        cancel_reconciliation_status = cancel_reconciliation.status
        cancel_reconciliation_reason = cancel_reconciliation.reason_code

    return _safe_result(
        product_id=product_id,
        symbol=normalized_symbol,
        submitted=result.ok,
        normalized_status=_order_status(order) or result.status or "unknown",
        blocker=None if result.ok else result.error_code,
        latest_price=latest_price,
        limit_price=limit_price,
        filled=_is_filled(order),
        pending_reconciliation_status=pending_reconciliation.status,
        pending_reconciliation_reason=pending_reconciliation.reason_code,
        cancel_submitted=cancel_submitted,
        cancel_reconciliation_status=cancel_reconciliation_status,
        cancel_reconciliation_reason=cancel_reconciliation_reason,
    )


def _latest_stock_price(
    *,
    market_data: AlpacaMarketDataAdapter,
    symbol: str,
    credential_context: Any,
) -> float | None:
    bars = market_data.fetch_stock_bars(
        symbol=symbol,
        timeframe="1Day",
        limit=1,
        credential_context=credential_context,
    )
    if not bars:
        return None
    return float(bars[-1].close)


def _cancel_order(*, adapter: AlpacaSdkBrokerAdapter, context: Any, order: str | None) -> Any:
    if not order:
        raise RuntimeError("Validation order cancel requires an internal broker order reference.")
    return adapter.broker_cancel_order(
        account_id=str(context.alpaca_account_id),
        order_id=order,
        metadata={"credential_context": context, "client_order_id": "sdk-stock-limit-validation-cancel"},
    )


def _find_existing_open_validation_order(*, recent_orders: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for order in recent_orders:
        if not isinstance(order, dict):
            continue
        if not str(order.get("client_order_id") or "").startswith(VALIDATION_CLIENT_ORDER_PREFIX):
            continue
        if str(order.get("symbol") or "").strip().upper() != symbol:
            continue
        if (_order_status(order) or "").lower() not in TERMINAL_STATUSES:
            return order
    return None


def _find_order(*, recent_orders: list[dict[str, Any]], symbol: str, client_order_id: str) -> dict[str, Any] | None:
    for order in recent_orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").strip().upper() != symbol:
            continue
        if str(order.get("client_order_id") or "") == client_order_id:
            return order
    return None


def _order_id(order: dict[str, Any] | None) -> str | None:
    if order is None:
        return None
    value = order.get("id") or order.get("order_id") or order.get("broker_order_id")
    return None if value is None else str(value)


def _order_status(order: dict[str, Any] | None) -> str | None:
    if order is None:
        return None
    value = order.get("status")
    return None if value is None else str(value)


def _is_filled(order: dict[str, Any] | None) -> bool:
    return (_order_status(order) or "").strip().lower() == "filled"


def _safe_result(
    *,
    product_id: str,
    symbol: str,
    submitted: bool,
    normalized_status: str,
    limit_price: float | None,
    pending_reconciliation_status: str,
    cancel_submitted: bool,
    cancel_reconciliation_status: str,
    blocker: str | None = None,
    latest_price: float | None = None,
    filled: bool = False,
    pending_reconciliation_reason: str | None = None,
    cancel_reconciliation_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "product": product_id,
        "symbol": symbol,
        "transport": "sdk",
        "paper": True,
        "submitted": submitted,
        "normalized_status": normalized_status,
        "blocker": blocker,
        "latest_price": latest_price,
        "limit_price": limit_price,
        "filled": filled,
        "pending_reconciliation_status": pending_reconciliation_status,
        "pending_reconciliation_reason": pending_reconciliation_reason,
        "cancel_submitted": cancel_submitted,
        "cancel_reconciliation_status": cancel_reconciliation_status,
        "cancel_reconciliation_reason": cancel_reconciliation_reason,
        "live_account_used": False,
        "production_sdk_default_changed": False,
        "completed_at": datetime.now(UTC).isoformat(),
    }


def _silence_broker_request_logs() -> None:
    logging.getLogger("app.brokers.alpaca_sdk_trading").setLevel(logging.CRITICAL)
    logging.getLogger("app.brokers.alpaca_paper_trading").setLevel(logging.CRITICAL)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one sanitized SDK paper stock limit order validation through BrokerAdapter.")
    parser.add_argument("--account-id", required=True, type=int)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--slot-number", type=int, default=1)
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--qty", type=float, default=1.0)
    parser.add_argument("--wait-seconds", type=float, default=3.0)
    args = parser.parse_args(argv)
    try:
        result = run_stock_limit_order_validation(
            account_id=args.account_id,
            product_id=args.product_id,
            slot_number=max(1, args.slot_number),
            symbol=args.symbol,
            qty=args.qty,
            wait_seconds=args.wait_seconds,
        )
    except AlpacaAccountResolutionError as exc:
        print(json.dumps({"submitted": False, "blocker": str(exc), "paper": True, "live_account_used": False}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result.get("submitted") and result.get("cancel_submitted") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
