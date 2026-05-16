# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_sdk_read_compare.py
# ======================================================

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any

from app.brokers.alpaca_paper_trading import AlpacaPaperTradingAdapter
from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.brokers.models import BrokerAccountState, BrokerAssetState, BrokerPositionState
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
)
from app.shared.config import get_settings


@dataclass(frozen=True)
class ReadComparisonRow:
    operation: str
    rest_result: str
    sdk_result: str
    match: bool
    result: str


def run_read_only_compare(
    *,
    account_id: int,
    product_id: str,
    slot_number: int,
    symbols: list[str],
    existing_position_symbol: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    context = LaravelAlpacaAccountResolver(settings).resolve_runtime_account_for_slot(
        account_id=account_id,
        slot_number=slot_number,
        product_id=product_id,
    )
    if context.environment != "paper":
        raise RuntimeError("SDK read-only validation is restricted to paper accounts.")

    rest = AlpacaPaperTradingAdapter(settings=settings)
    sdk = AlpacaSdkBrokerAdapter(settings=settings)
    account_ref = str(context.alpaca_account_id)

    rows: list[ReadComparisonRow] = []
    rest_account = rest.get_account(account_ref)
    sdk_account = sdk.get_account(account_ref)
    rows.append(_compare_row("get_account", _account_summary(rest_account), _account_summary(sdk_account)))

    rest_positions = rest.get_positions(account_ref)
    sdk_positions = sdk.get_positions(account_ref)
    rows.append(_compare_row("get_positions", _positions_summary(rest_positions), _positions_summary(sdk_positions)))

    selected_symbols = _resolve_symbols(symbols=symbols, positions=rest_positions)
    for symbol in selected_symbols:
        rest_asset = rest.get_asset(symbol)
        sdk_asset = sdk.get_asset(symbol)
        rows.append(_compare_row(f"get_asset:{symbol}", _asset_summary(rest_asset), _asset_summary(sdk_asset)))

    position_symbol = existing_position_symbol or (rest_positions[0].symbol if rest_positions else None)
    if position_symbol:
        rest_position = rest.get_position(account_ref, position_symbol)
        sdk_position = sdk.get_position(account_ref, position_symbol)
        rows.append(_compare_row(f"get_position:{position_symbol}", _position_summary(rest_position), _position_summary(sdk_position)))

    missing_symbol = "__BISMEL1_MISSING_POSITION_CHECK__"
    rest_missing = rest.get_position(account_ref, missing_symbol)
    sdk_missing = sdk.get_position(account_ref, missing_symbol)
    rows.append(_compare_row("get_position:missing", _position_summary(rest_missing), _position_summary(sdk_missing)))

    return {
        "context": {
            "product_id": product_id,
            "slot_number": slot_number,
            "environment": context.environment,
            "account_context_resolved": True,
        },
        "summary": {
            "passed": all(row.match for row in rows),
            "row_count": len(rows),
            "orders_submitted": False,
            "live_account_used": False,
        },
        "rows": [asdict(row) for row in rows],
    }


def _compare_row(operation: str, rest_payload: dict[str, Any] | None, sdk_payload: dict[str, Any] | None) -> ReadComparisonRow:
    match = rest_payload == sdk_payload
    return ReadComparisonRow(
        operation=operation,
        rest_result=_compact_result(rest_payload),
        sdk_result=_compact_result(sdk_payload),
        match=match,
        result="pass" if match else "mismatch",
    )


def _account_summary(account: BrokerAccountState) -> dict[str, Any]:
    return {
        "status": account.status,
        "currency": account.currency,
        "equity": _round_money(account.equity),
        "cash": _round_money(account.cash),
        "buying_power": _round_money(account.buying_power),
        "portfolio_value": _round_money(account.portfolio_value),
    }


def _positions_summary(positions: list[BrokerPositionState]) -> dict[str, Any]:
    return {
        "count": len(positions),
        "symbols": sorted(position.symbol for position in positions),
        "positions": {
            position.symbol: _position_summary(position)
            for position in sorted(positions, key=lambda item: item.symbol)
        },
    }


def _position_summary(position: BrokerPositionState | None) -> dict[str, Any] | None:
    if position is None:
        return None
    return {
        "symbol": position.symbol,
        "qty": _round_qty(position.qty),
        "avg_entry_price": _round_money(position.avg_entry_price),
        "current_price": _round_money(position.current_price),
        "market_value": _round_money(position.market_value),
    }


def _asset_summary(asset: BrokerAssetState | None) -> dict[str, Any] | None:
    if asset is None:
        return None
    return {
        "symbol": asset.symbol,
        "asset_class": asset.asset_class,
        "tradable": asset.tradable,
        "shortable": asset.shortable,
        "fractionable": asset.fractionable,
        "status": asset.status,
    }


def _resolve_symbols(*, symbols: list[str], positions: list[BrokerPositionState]) -> list[str]:
    resolved = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    if resolved:
        return resolved[:10]
    return [position.symbol for position in positions[:5]]


def _round_money(value: float | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _round_qty(value: float | None) -> float | None:
    return None if value is None else round(float(value), 9)


def _compact_result(payload: dict[str, Any] | None) -> str:
    if payload is None:
        return "None"
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare REST and SDK Alpaca broker reads through normalized Bismel1 models.")
    parser.add_argument("--account-id", required=True, type=int)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--slot-number", type=int, default=1)
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--position-symbol")
    args = parser.parse_args(argv)
    try:
        result = run_read_only_compare(
            account_id=args.account_id,
            product_id=args.product_id,
            slot_number=max(1, args.slot_number),
            symbols=args.symbol,
            existing_position_symbol=args.position_symbol,
        )
    except AlpacaAccountResolutionError as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result["summary"]["passed"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

