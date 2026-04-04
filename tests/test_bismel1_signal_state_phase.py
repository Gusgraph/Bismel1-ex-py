# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_bismel1_signal_state_phase.py
# ======================================================

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import isclose

from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PriceBar,
)
from app.products.stocks.bismel1.strategy import evaluate_signal_state_phase


def test_pause_new_basket_and_pause_adds_follow_pine_split() -> None:
    strategy_input = _strategy_input(
        closes=[94.0, 95.0],
        opens=[93.0, 90.0],
        highs=[95.0, 96.0],
        lows=[92.0, 89.0],
    )
    state = BismillahTrobotStocksV1State(
        last_add_price=100.0,
        dollars_used=100.0,
        position_avg_price=100.0,
        position_size=1.0,
    )
    series = PineComputedSeries(
        rsi_val=[30.0, 40.0],
        atr_val=[1.0, 1.0],
        in_pullback_zone=[False, True],
        trend_ok=[False, False],
        regime_fail=[True, True],
        auto_paused=[True, True],
        pause_new_basket=[True, True],
        pause_adds=[False, False],
        is_low_tier=[False, False],
        momentum_confirm=[False, False],
    )

    evaluation = evaluate_signal_state_phase(
        strategy_input,
        BismillahTrobotStocksV1Config(exit_on_regime_fail=False),
        state,
        series,
    )

    assert evaluation.bars[1].pause_new_basket is True
    assert evaluation.bars[1].pause_adds is False
    assert evaluation.bars[1].signal.base_entry_signal is False
    assert evaluation.bars[1].signal.add_signal_raw is True
    assert evaluation.bars[1].signal.add_trigger is True


def test_base_entry_trigger_is_edge_triggered_once_until_signal_resets() -> None:
    strategy_input = _strategy_input(
        closes=[100.0, 102.0, 103.0],
        opens=[101.0, 100.0, 101.0],
        highs=[101.0, 103.0, 104.0],
        lows=[99.0, 99.0, 100.0],
    )
    series = PineComputedSeries(
        in_pullback_zone=[False, True, True],
        trend_ok=[False, True, True],
        pause_new_basket=[True, False, False],
        pause_adds=[True, True, True],
        momentum_confirm=[False, True, True],
        regime_fail=[True, False, False],
        auto_paused=[True, False, False],
        atr_val=[None, None, None],
    )

    evaluation = evaluate_signal_state_phase(strategy_input, BismillahTrobotStocksV1Config(), None, series)

    assert [bar.signal.base_entry_signal for bar in evaluation.bars] == [False, True, True]
    assert [bar.signal.base_entry_trigger for bar in evaluation.bars] == [False, True, False]
    assert evaluation.bars[1].state_after.position_size > 0.0
    assert evaluation.bars[2].state_after.dollars_used == 100.0


def test_add_trigger_fires_only_on_raw_signal_edges_across_bars() -> None:
    strategy_input = _strategy_input(
        closes=[94.0, 95.0, 90.0, 91.0],
        opens=[95.0, 90.0, 92.0, 89.0],
        highs=[96.0, 96.0, 93.0, 92.0],
        lows=[93.0, 89.0, 89.0, 88.0],
    )
    state = BismillahTrobotStocksV1State(
        last_add_price=100.0,
        dollars_used=100.0,
        position_avg_price=100.0,
        position_size=1.0,
    )
    series = PineComputedSeries(
        rsi_val=[30.0, 50.0, 30.0, 50.0],
        atr_val=[1.0, 1.0, 1.0, 1.0],
        in_pullback_zone=[False, True, True, True],
        pause_adds=[False, False, False, False],
        pause_new_basket=[True, True, True, True],
        trend_ok=[False, False, False, False],
        regime_fail=[True, True, True, True],
        auto_paused=[True, True, True, True],
        is_low_tier=[False, False, False, False],
        momentum_confirm=[False, False, False, False],
    )

    evaluation = evaluate_signal_state_phase(
        strategy_input,
        BismillahTrobotStocksV1Config(exit_on_regime_fail=False, atr_trail_mult=20.0),
        state,
        series,
    )

    assert [bar.signal.add_signal_raw for bar in evaluation.bars] == [False, True, False, True]
    assert [bar.signal.add_trigger for bar in evaluation.bars] == [False, True, False, True]
    assert evaluation.bars[1].state_after.add_count == 1
    assert evaluation.bars[3].state_after.add_count == 2
    assert evaluation.final_state.last_add_price == 91.0


def test_atr_trail_exit_trigger_closes_the_mirrored_position_state() -> None:
    strategy_input = _strategy_input(
        closes=[109.0, 111.0, 108.0],
        opens=[108.0, 110.0, 110.0],
        highs=[110.0, 112.0, 112.0],
        lows=[107.0, 109.0, 107.0],
    )
    state = BismillahTrobotStocksV1State(
        last_add_price=100.0,
        dollars_used=100.0,
        position_avg_price=100.0,
        position_size=1.0,
    )
    series = PineComputedSeries(
        atr_val=[1.0, 1.0, 1.0],
        in_pullback_zone=[False, False, False],
        pause_adds=[True, True, True],
        pause_new_basket=[True, True, True],
        trend_ok=[True, True, True],
        regime_fail=[False, False, False],
        auto_paused=[False, False, False],
        is_low_tier=[False, False, False],
        momentum_confirm=[False, False, False],
        rsi_val=[30.0, 30.0, 30.0],
    )

    evaluation = evaluate_signal_state_phase(strategy_input, BismillahTrobotStocksV1Config(), state, series)

    assert [bar.signal.hit_atr_trail for bar in evaluation.bars] == [False, False, True]
    assert evaluation.bars[2].state_before.trail_stop == 109.0
    assert evaluation.bars[2].state_after.position_size == 0.0
    assert evaluation.bars[2].state_after.position_avg_price is None


def test_regime_fail_exit_trigger_respects_optional_exit_flag() -> None:
    strategy_input = _strategy_input(
        closes=[105.0, 106.0],
        opens=[104.0, 105.0],
        highs=[106.0, 107.0],
        lows=[103.0, 104.0],
    )
    state = BismillahTrobotStocksV1State(
        last_add_price=100.0,
        dollars_used=100.0,
        position_avg_price=100.0,
        position_size=1.0,
    )
    series = PineComputedSeries(
        atr_val=[1.0, 1.0],
        in_pullback_zone=[False, False],
        pause_adds=[True, True],
        pause_new_basket=[False, True],
        trend_ok=[True, False],
        regime_fail=[False, True],
        auto_paused=[False, True],
        is_low_tier=[False, False],
        momentum_confirm=[False, False],
        rsi_val=[30.0, 30.0],
    )

    enabled = evaluate_signal_state_phase(strategy_input, BismillahTrobotStocksV1Config(), state, series)
    disabled = evaluate_signal_state_phase(
        strategy_input,
        BismillahTrobotStocksV1Config(exit_on_regime_fail=False),
        state,
        series,
    )

    assert [bar.signal.hit_regime for bar in enabled.bars] == [False, True]
    assert enabled.bars[1].state_after.position_size == 0.0
    assert [bar.signal.hit_regime for bar in disabled.bars] == [False, False]
    assert isclose(disabled.bars[1].state_after.position_size, 1.0, rel_tol=1e-9, abs_tol=1e-9)


def _strategy_input(
    closes: list[float],
    opens: list[float],
    highs: list[float],
    lows: list[float],
) -> BismillahTrobotStocksV1Input:
    start = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    execution_bars = [
        PriceBar(
            starts_at=start + timedelta(hours=4 * index),
            ends_at=start + timedelta(hours=4 * (index + 1)),
            open=opens[index],
            high=highs[index],
            low=lows[index],
            close=closes[index],
        )
        for index in range(len(closes))
    ]
    return BismillahTrobotStocksV1Input(
        execution_bars=execution_bars,
        htf_bars=[],
        symbol="AAPL",
    )
