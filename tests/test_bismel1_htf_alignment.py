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

from math import isclose

from app.products.stocks.bismel1.indicators import merge_htf_series
from app.products.stocks.bismel1.models import BismillahTrobotStocksV1Input
from app.products.stocks.bismel1.strategy import compute_pine_series
from tests.fixtures_bismel1_htf import HTF_PARITY_CONFIG, HTF_PARITY_FIXTURE


def test_merge_htf_close_series_matches_closed_fixture() -> None:
    merged = merge_htf_series(
        HTF_PARITY_FIXTURE.execution_bars_closed,
        HTF_PARITY_FIXTURE.htf_bars_closed,
        HTF_PARITY_FIXTURE.htf_closes,
    )

    _assert_optional_float_series(merged, HTF_PARITY_FIXTURE.expected_htf_close_closed)


def test_merge_htf_close_series_blocks_lookahead_on_open_htf_tail() -> None:
    merged = merge_htf_series(
        HTF_PARITY_FIXTURE.execution_bars_open_tail,
        HTF_PARITY_FIXTURE.htf_bars_open_tail,
        HTF_PARITY_FIXTURE.htf_closes,
    )

    _assert_optional_float_series(merged, HTF_PARITY_FIXTURE.expected_htf_close_open_tail)
    assert merged[-1] == 160.0
    assert merged[-1] != 190.0


def test_merge_htf_series_carries_last_confirmed_non_none_value_across_unavailable_periods() -> None:
    merged = merge_htf_series(
        HTF_PARITY_FIXTURE.execution_bars_closed,
        HTF_PARITY_FIXTURE.htf_bars_closed,
        [None, 130.0, None, 190.0],
    )

    _assert_optional_float_series(merged, HTF_PARITY_FIXTURE.expected_htf_close_with_source_none)


def test_compute_pine_series_matches_fixture_expected_htf_outputs() -> None:
    series = compute_pine_series(
        BismillahTrobotStocksV1Input(
            execution_bars=HTF_PARITY_FIXTURE.execution_bars_closed,
            htf_bars=HTF_PARITY_FIXTURE.htf_bars_closed,
            symbol="AAPL",
        ),
        HTF_PARITY_CONFIG,
    )

    _assert_optional_float_series(series.htf_close, HTF_PARITY_FIXTURE.expected_htf_close_closed)
    _assert_optional_float_series(series.htf_ema_fast, HTF_PARITY_FIXTURE.expected_htf_ema_fast)
    _assert_optional_float_series(series.htf_ema_slow, HTF_PARITY_FIXTURE.expected_htf_ema_slow)
    _assert_optional_float_series(series.htf_ema_slow_prev, HTF_PARITY_FIXTURE.expected_htf_ema_slow_prev)
    assert series.htf_ema_slow_slope_up == HTF_PARITY_FIXTURE.expected_htf_ema_slow_slope_up


def _assert_optional_float_series(
    actual: list[float | None],
    expected: list[float | None],
) -> None:
    assert len(actual) == len(expected)
    for actual_value, expected_value in zip(actual, expected, strict=True):
        if expected_value is None:
            assert actual_value is None
            continue
        assert actual_value is not None
        assert isclose(actual_value, expected_value, rel_tol=1e-9, abs_tol=1e-9)
