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

    strategy_input = BismillahTrobotStocksV1Input(
        execution_bars=[],
        htf_bars=[],
        symbol="BTCUSD",
        asset_type="crypto",
    )

    result = run_prime_stocks_strategy(
        strategy_input=strategy_input,
    )

    assert result.status == "no_signal"
    assert result.latest_bar is None
    assert result.final_state == BismillahTrobotStocksV1State()


def test_runner_accepts_equity_alias_and_normalizes_to_stock() -> None:
    strategy_input = BismillahTrobotStocksV1Input(
        execution_bars=[],
        htf_bars=[],
        symbol="AAPL",
        asset_type="equity",
    )

    result = evaluate_strategy(strategy_input)

    assert result.status == "no_signal"

    assert strategy_input.asset_type == "equity"


def test_runner_uses_initial_state_when_supplied() -> None:
    initial_state = BismillahTrobotStocksV1State(
        add_count=2,
        last_add_price=103.0,
        dollars_used=174.0,
        pos_high=107.0,
        trail_stop=101.0,
        position_avg_price=102.0,
        position_size=2.0,
    )

    result = evaluate_strategy(
        BismillahTrobotStocksV1Input(
            execution_bars=[],
            htf_bars=[],
            symbol="AAPL",
            asset_type="stock",
        ),
        initial_state=initial_state,
    )

    assert result.final_state == initial_state
