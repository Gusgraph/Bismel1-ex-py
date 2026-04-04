# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_bismel1_htf_alignment.py
# ======================================================

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.indicators import merge_htf_series
from app.products.stocks.bismel1.models import BismillahTrobotStocksV1Input, PriceBar
from app.products.stocks.bismel1.strategy import compute_pine_series


def test_merge_htf_series_blocks_lookahead_and_carries_last_confirmed_value() -> None:
    execution_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=1),
        closes=[10, 11, 12, 13, 14, 15],
    )
    htf_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=3),
        closes=[100, 200],
    )

    merged = merge_htf_series(execution_bars, htf_bars, [100.0, 200.0])

    assert merged == [None, None, 100.0, 100.0, 100.0, 200.0]


def test_merge_htf_series_keeps_prior_non_none_value_when_current_htf_value_is_none() -> None:
    execution_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=1),
        closes=[10, 11, 12, 13, 14, 15],
    )
    htf_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=2),
        closes=[100, 200, 300],
    )

    merged = merge_htf_series(execution_bars, htf_bars, [1.0, None, 3.0])

    assert merged == [None, 1.0, 1.0, 1.0, 1.0, 3.0]


def test_compute_pine_series_shifts_htf_prev_inside_htf_context_before_merge() -> None:
    execution_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=1),
        closes=[10, 11, 12, 13, 14, 15],
    )
    htf_bars = _build_bars(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        step=timedelta(hours=2),
        closes=[10, 20, 30],
    )
    strategy_input = BismillahTrobotStocksV1Input(
        execution_bars=execution_bars,
        htf_bars=htf_bars,
        symbol="AAPL",
    )
    config = BismillahTrobotStocksV1Config(
        ema_fast_len=1,
        ema_slow_len=1,
        rsi_len=2,
        atr_len=1,
        swing_len=2,
        ema_slow_slope_lookback=1,
    )

    series = compute_pine_series(strategy_input, config)

    assert series.htf_ema_slow == [None, 10.0, 10.0, 20.0, 20.0, 30.0]
    assert series.htf_ema_slow_prev == [None, None, None, 10.0, 10.0, 20.0]
    assert series.htf_ema_slow_slope_up == [False, False, False, True, True, True]


def _build_bars(start: datetime, step: timedelta, closes: list[float]) -> list[PriceBar]:
    bars: list[PriceBar] = []
    for index, close in enumerate(closes):
        starts_at = start + (step * index)
        ends_at = starts_at + step
        bars.append(
            PriceBar(
                starts_at=starts_at,
                ends_at=ends_at,
                open=close - 0.5,
                high=close + 0.5,
                low=close - 1.0,
                close=close,
            )
        )
    return bars
