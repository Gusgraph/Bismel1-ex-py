# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_bismel1_source_truth_sync.py
# ======================================================

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PriceBar,
)
from app.products.stocks.bismel1.strategy import snapshot_signals


def test_add_signal_is_not_blocked_by_regime_fail_when_add_pause_is_clear() -> None:
    start = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    execution_bars = [
        PriceBar(
            starts_at=start,
            ends_at=start + timedelta(hours=4),
            open=93.0,
            high=95.0,
            low=92.0,
            close=94.0,
        ),
        PriceBar(
            starts_at=start + timedelta(hours=4),
            ends_at=start + timedelta(hours=8),
            open=90.0,
            high=96.0,
            low=89.0,
            close=95.0,
        ),
    ]
    strategy_input = BismillahTrobotStocksV1Input(
        execution_bars=execution_bars,
        htf_bars=[],
        symbol="AAPL",
    )
    config = BismillahTrobotStocksV1Config(exit_on_regime_fail=False)
    state = BismillahTrobotStocksV1State(
        add_count=0,
        last_add_price=100.0,
        dollars_used=0.0,
        position_avg_price=100.0,
        position_size=1.0,
    )
    series = PineComputedSeries(
        rsi_val=[30.0, 40.0],
        atr_val=[1.0, 1.0],
        in_pullback_zone=[False, True],
        trend_ok=[False, False],
        regime_fail=[True, True],
        pause_new_basket=[True, True],
        pause_adds=[False, False],
        is_low_tier=[False, False],
        momentum_confirm=[False, False],
    )

    snapshot = snapshot_signals(
        strategy_input=strategy_input,
        config=config,
        state=state,
        series=series,
    )

    assert snapshot.base_entry_signal is False
    assert snapshot.add_signal_raw is True
    assert snapshot.add_trigger is True
    assert snapshot.hit_regime is False
