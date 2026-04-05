# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_bismel1_strategy_runner.py
# ======================================================

from __future__ import annotations

from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
)
from app.products.stocks.bismel1.strategy import evaluate_strategy, run_prime_stocks_strategy


def test_runner_rejects_non_stock_assets_without_mutating_state() -> None:
    initial_state = BismillahTrobotStocksV1State(
        add_count=1,
        last_add_price=101.0,
        dollars_used=220.0,
        position_avg_price=100.0,
        position_size=2.0,
    )

    result = run_prime_stocks_strategy(
        execution_bars=[],
        htf_bars=[],
        symbol="BTCUSD",
        asset_type="crypto",
        state=initial_state,
    )

    assert result.status == "invalid_asset_type"
    assert result.validation.is_supported is False
    assert result.validation.normalized_asset_type == "crypto"
    assert "stock/equity symbols only" in result.message
    assert result.latest_bar is None
    assert result.final_state == initial_state


def test_runner_accepts_equity_alias_and_normalizes_to_stock() -> None:
    strategy_input = BismillahTrobotStocksV1Input(
        execution_bars=[],
        htf_bars=[],
        symbol="AAPL",
        asset_type="equity",
    )

    result = evaluate_strategy(strategy_input)

    assert result.status == "ok"
    assert result.asset_type == "stock"
    assert result.validation.is_supported is True
    assert result.validation.requested_asset_type == "equity"
