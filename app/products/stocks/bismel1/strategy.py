# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/products/stocks/bismel1/strategy.py
# ======================================================

from __future__ import annotations

from app.products.stocks.bismel1.config import (
    Bismel1StrategyConfig,
    BismillahTrobotStocksV1Config,
)
from app.products.stocks.bismel1.indicators import (
    atr,
    ema,
    merge_htf_series,
    rolling_highest,
    rolling_lowest,
    rsi,
    shift_series,
)
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    StrategyInputSet,
)


EXIT_TP = "Bismillah-TP"
EXIT_TP_TRAIL = "Bismillah-TRAIL-PROFIT"


def compute_pine_series(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
) -> PineComputedSeries:
    """Compute Pine section F scaffolding, with HTF merge parity as the current focus."""

    execution_bars = strategy_input.execution_bars
    htf_bars = strategy_input.htf_bars
    execution_closes = [bar.close for bar in execution_bars]
    execution_highs = [bar.high for bar in execution_bars]
    execution_lows = [bar.low for bar in execution_bars]

    ema_fast_exec = ema(execution_closes, config.ema_fast_len)
    ema_slow_exec = ema(execution_closes, config.ema_slow_len)
    rsi_val = rsi(execution_closes, config.rsi_len)
    atr_val = atr(execution_highs, execution_lows, execution_closes, config.atr_len)
    swing_high = rolling_highest(execution_highs, config.swing_len)
    swing_low = rolling_lowest(execution_lows, config.swing_len)

    pullback_depth: list[float | None] = []
    in_pullback_zone: list[bool] = []
    for index in range(len(execution_bars)):
        current_swing_high = swing_high[index]
        current_swing_low = swing_low[index]
        if current_swing_high is None or current_swing_low is None:
            pullback_depth.append(None)
            in_pullback_zone.append(False)
            continue

        swing_range = current_swing_high - current_swing_low
        depth = ((current_swing_high - execution_closes[index]) / swing_range) if swing_range > 0 else 0.0
        pullback_depth.append(depth)
        in_pullback_zone.append(depth >= config.pullback_min)

    htf_closes = [bar.close for bar in htf_bars]
    htf_ema_fast_source = ema(htf_closes, config.ema_fast_len)
    htf_ema_slow_source = ema(htf_closes, config.ema_slow_len)
    htf_ema_slow_prev_source = shift_series(htf_ema_slow_source, config.ema_slow_slope_lookback)

    htf_close = merge_htf_series(execution_bars, htf_bars, htf_closes)
    htf_ema_fast = merge_htf_series(execution_bars, htf_bars, htf_ema_fast_source)
    htf_ema_slow = merge_htf_series(execution_bars, htf_bars, htf_ema_slow_source)
    htf_ema_slow_prev = merge_htf_series(execution_bars, htf_bars, htf_ema_slow_prev_source)

    htf_ema_slow_slope_up: list[bool] = []
    trend_base_htf: list[bool] = []
    trend_ok: list[bool] = []
    for index in range(len(execution_bars)):
        merged_htf_close = htf_close[index]
        merged_htf_ema_fast = htf_ema_fast[index]
        merged_htf_ema_slow = htf_ema_slow[index]
        merged_htf_ema_slow_prev = htf_ema_slow_prev[index]

        slope_up = (
            merged_htf_ema_slow is not None
            and merged_htf_ema_slow_prev is not None
            and merged_htf_ema_slow > merged_htf_ema_slow_prev
        )
        trend_base = (
            merged_htf_close is not None
            and merged_htf_ema_fast is not None
            and merged_htf_ema_slow is not None
            and merged_htf_ema_fast > merged_htf_ema_slow
            and merged_htf_close > merged_htf_ema_slow
            and merged_htf_close > merged_htf_ema_fast
        )
        trend_allowed = trend_base and (slope_up if config.require_ema_slow_slope_up else True)

        htf_ema_slow_slope_up.append(slope_up)
        trend_base_htf.append(trend_base)
        trend_ok.append(trend_allowed)

    # Pine session membership depends on TradingView exchange-time semantics.
    # Until that is modeled, keep the disabled case permissive and the enabled case
    # conservative instead of claiming parity we do not have.
    session_ok = [not config.use_session_filter] * len(execution_bars)
    regime_fail = [not allowed for allowed in trend_ok]
    auto_paused = [
        config.use_auto_pause and config.auto_pause_on_regime_fail and failed for failed in regime_fail
    ]
    entries_paused = [
        config.pause_new_entries_manual or auto_paused[index] or (not session_ok[index])
        for index in range(len(execution_bars))
    ]

    return PineComputedSeries(
        ema_fast_exec=ema_fast_exec,
        ema_slow_exec=ema_slow_exec,
        rsi_val=rsi_val,
        atr_val=atr_val,
        swing_high=swing_high,
        swing_low=swing_low,
        pullback_depth=pullback_depth,
        in_pullback_zone=in_pullback_zone,
        htf_close=htf_close,
        htf_ema_fast=htf_ema_fast,
        htf_ema_slow=htf_ema_slow,
        htf_ema_slow_prev=htf_ema_slow_prev,
        htf_ema_slow_slope_up=htf_ema_slow_slope_up,
        trend_base_htf=trend_base_htf,
        trend_ok=trend_ok,
        session_ok=session_ok,
        entries_paused=entries_paused,
    )


def snapshot_signals(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
    state: BismillahTrobotStocksV1State,
    series: PineComputedSeries,
) -> PineSignalSnapshot:
    """Reserved for Pine sections J, L, and M signal booleans."""

    _ = strategy_input
    _ = config
    _ = state
    _ = series
    return PineSignalSnapshot(
        base_entry_signal=False,
        can_add_more=False,
        spacing_ok=False,
        add_signal_raw=False,
        add_within_cap=False,
        add_signal=False,
        hit_plain_tp=False,
        hit_profit_trail=False,
    )


def evaluate_strategy(
    strategy_input: StrategyInputSet,
    config: Bismel1StrategyConfig | None = None,
) -> dict[str, object]:
    resolved_config = config or Bismel1StrategyConfig()
    series = compute_pine_series(strategy_input, resolved_config)
    signals = snapshot_signals(
        strategy_input=strategy_input,
        config=resolved_config,
        state=BismillahTrobotStocksV1State(),
        series=series,
    )
    return {
        "product_key": resolved_config.product_key,
        "pine_strategy_title": resolved_config.pine_strategy_title,
        "status": "parity_scaffolding_only",
        "message": "Pine components are mapped and documented, but execution parity is not implemented.",
        "series_type": type(series).__name__,
        "signal_type": type(signals).__name__,
    }
