from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.models import PriceBar


@dataclass(frozen=True)
class HtfParityFixture:
    execution_bars_closed: list[PriceBar]
    execution_bars_open_tail: list[PriceBar]
    htf_bars_closed: list[PriceBar]
    htf_bars_open_tail: list[PriceBar]
    htf_closes: list[float]
    expected_htf_close_closed: list[float | None]
    expected_htf_close_open_tail: list[float | None]
    expected_htf_close_with_source_none: list[float | None]
    expected_htf_ema_fast: list[float | None]
    expected_htf_ema_slow: list[float | None]
    expected_htf_ema_slow_prev: list[float | None]
    expected_htf_ema_slow_slope_up: list[bool]


def _build_bars(
    start: datetime,
    step: timedelta,
    closes: list[float],
    *,
    final_bar_open: bool = False,
) -> list[PriceBar]:
    bars: list[PriceBar] = []
    for index, close in enumerate(closes):
        starts_at = start + (step * index)
        ends_at = None if final_bar_open and index == len(closes) - 1 else starts_at + step
        bars.append(
            PriceBar(
                starts_at=starts_at,
                ends_at=ends_at,
                open=close - 1.0,
                high=close + 1.0,
                low=close - 2.0,
                close=close,
            )
        )
    return bars


HTF_PARITY_FIXTURE = HtfParityFixture(
    execution_bars_closed=_build_bars(
        start=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        step=timedelta(hours=1),
        closes=[10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
    ),
    execution_bars_open_tail=_build_bars(
        start=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        step=timedelta(hours=1),
        closes=[10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
        final_bar_open=True,
    ),
    htf_bars_closed=_build_bars(
        start=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        step=timedelta(hours=3),
        closes=[100, 130, 160, 190],
    ),
    htf_bars_open_tail=_build_bars(
        start=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        step=timedelta(hours=3),
        closes=[100, 130, 160, 190],
        final_bar_open=True,
    ),
    htf_closes=[100.0, 130.0, 160.0, 190.0],
    expected_htf_close_closed=[
        None,
        None,
        100.0,
        100.0,
        100.0,
        130.0,
        130.0,
        130.0,
        160.0,
        160.0,
        160.0,
        190.0,
    ],
    expected_htf_close_open_tail=[
        None,
        None,
        100.0,
        100.0,
        100.0,
        130.0,
        130.0,
        130.0,
        160.0,
        160.0,
        160.0,
        160.0,
    ],
    expected_htf_close_with_source_none=[
        None,
        None,
        None,
        None,
        None,
        130.0,
        130.0,
        130.0,
        130.0,
        130.0,
        130.0,
        190.0,
    ],
    expected_htf_ema_fast=[
        None,
        None,
        100.0,
        100.0,
        100.0,
        120.0,
        120.0,
        120.0,
        146.66666666666666,
        146.66666666666666,
        146.66666666666666,
        175.55555555555554,
    ],
    expected_htf_ema_slow=[
        None,
        None,
        100.0,
        100.0,
        100.0,
        115.0,
        115.0,
        115.0,
        137.5,
        137.5,
        137.5,
        163.75,
    ],
    expected_htf_ema_slow_prev=[
        None,
        None,
        None,
        None,
        None,
        100.0,
        100.0,
        100.0,
        115.0,
        115.0,
        115.0,
        137.5,
    ],
    expected_htf_ema_slow_slope_up=[
        False,
        False,
        False,
        False,
        False,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
    ],
)


HTF_PARITY_CONFIG = BismillahTrobotStocksV1Config(
    ema_fast_len=2,
    ema_slow_len=3,
    rsi_len=2,
    atr_len=1,
    swing_len=2,
    ema_slow_slope_lookback=1,
)
