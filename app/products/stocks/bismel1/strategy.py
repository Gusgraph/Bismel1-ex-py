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

from datetime import datetime

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


ENTRY_NAME_FIRST_LOT = "FirstLot"
ENTRY_NAME_MULTI_PREFIX = "MULTI-"
EXIT_COMMENT_ATR = "EXIT_ATR"
EXIT_COMMENT_REGIME = "EXIT_REGIME"
ALERT_ACTION_BUY = "Buy"
ALERT_ACTION_MULTI = "Multi"
ALERT_ACTION_SELL = "Sell"
ALERT_ACTION_PING = "Ping"
ALERT_TAG_FIRST = "First"
ALERT_TAG_ATR_TRAIL = "ATRTrail"
ALERT_TAG_REGIME = "Regime"


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
    atr_pct = [
        None if atr_value is None else ((atr_value / execution_closes[index]) * 100.0 if execution_closes[index] > 0 else 0.0)
        for index, atr_value in enumerate(atr_val)
    ]
    swing_high = rolling_highest(execution_highs, config.swing_len)
    swing_low = rolling_lowest(execution_lows, config.swing_len)
    lowest_low_reclaim = rolling_lowest(execution_lows, config.price_reclaim_bars)

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

    rsi_cross_mode: list[bool] = []
    fast_reclaim_mode: list[bool] = []
    momentum_confirm: list[bool] = []
    for index in range(len(execution_bars)):
        current_rsi = rsi_val[index]
        current_ema_fast = ema_fast_exec[index]
        current_lowest_reclaim = lowest_low_reclaim[index]
        previous_high = execution_highs[index - 1] if index >= 1 else None
        previous_close = execution_closes[index - 1] if index >= 1 else None
        previous_rsi = rsi_val[index - 1] if index >= 1 else None

        crossed_up = (
            previous_rsi is not None
            and current_rsi is not None
            and previous_rsi <= config.rsi_turn_up
            and current_rsi > config.rsi_turn_up
        )
        rsi_cross = crossed_up and previous_high is not None and execution_closes[index] > previous_high
        fast_reclaim = (
            previous_close is not None
            and current_ema_fast is not None
            and current_lowest_reclaim is not None
            and execution_closes[index] > execution_bars[index].open
            and execution_closes[index] > previous_close
            and execution_closes[index] > current_ema_fast
            and current_lowest_reclaim <= current_ema_fast
        )
        momentum_ok = (
            rsi_cross
            if config.entry_mode == "RSI Crossover"
            else (current_rsi is not None and current_rsi > config.rsi_turn_up and fast_reclaim)
        )

        rsi_cross_mode.append(rsi_cross)
        fast_reclaim_mode.append(fast_reclaim)
        momentum_confirm.append(momentum_ok)

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

    in_session = [_session_filter_membership(bar.ends_at or bar.starts_at, config.trade_session) for bar in execution_bars]
    is_weekday = [
        _is_weekday(bar.ends_at or bar.starts_at)
        for bar in execution_bars
    ]
    session_ok = [
        (in_session[index] and (is_weekday[index] if config.trade_weekdays_only else True))
        if config.use_session_filter
        else True
        for index in range(len(execution_bars))
    ]
    regime_fail = [not allowed for allowed in trend_ok]
    auto_paused = [
        config.use_auto_pause and config.auto_pause_on_regime_fail and failed for failed in regime_fail
    ]
    pause_new_basket = [
        config.pause_new_entries_manual
        or (config.pause_on_regime_fail and auto_paused[index])
        or (not session_ok[index])
        for index in range(len(execution_bars))
    ]
    pause_adds = [
        config.pause_new_entries_manual or (not session_ok[index])
        for index in range(len(execution_bars))
    ]
    is_low_tier = [
        atr_pct[index] is not None and atr_pct[index] < 1.2
        for index in range(len(execution_bars))
    ]

    return PineComputedSeries(
        ema_fast_exec=ema_fast_exec,
        ema_slow_exec=ema_slow_exec,
        rsi_val=rsi_val,
        atr_val=atr_val,
        atr_pct=atr_pct,
        swing_high=swing_high,
        swing_low=swing_low,
        pullback_depth=pullback_depth,
        in_pullback_zone=in_pullback_zone,
        lowest_low_reclaim=lowest_low_reclaim,
        rsi_cross_mode=rsi_cross_mode,
        fast_reclaim_mode=fast_reclaim_mode,
        momentum_confirm=momentum_confirm,
        htf_close=htf_close,
        htf_ema_fast=htf_ema_fast,
        htf_ema_slow=htf_ema_slow,
        htf_ema_slow_prev=htf_ema_slow_prev,
        htf_ema_slow_slope_up=htf_ema_slow_slope_up,
        trend_base_htf=trend_base_htf,
        trend_ok=trend_ok,
        in_session=in_session,
        is_weekday=is_weekday,
        session_ok=session_ok,
        regime_fail=regime_fail,
        auto_paused=auto_paused,
        pause_new_basket=pause_new_basket,
        pause_adds=pause_adds,
        is_low_tier=is_low_tier,
    )


def snapshot_signals(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
    state: BismillahTrobotStocksV1State,
    series: PineComputedSeries,
) -> PineSignalSnapshot:
    """Return current-bar booleans that mirror the Pine entry/add/exit gates."""

    if not strategy_input.execution_bars:
        return PineSignalSnapshot(
            base_entry_signal=False,
            base_entry_trigger=False,
            add_bounce_confirm=False,
            gate_atr_ok=False,
            gate_dp_ok=False,
            cap_ok=False,
            add_signal_raw=False,
            add_trigger=False,
            hit_atr_trail=False,
            hit_regime=False,
        )

    index = len(strategy_input.execution_bars) - 1
    current_bar = strategy_input.execution_bars[index]
    current_close = current_bar.close
    previous_close = strategy_input.execution_bars[index - 1].close if index >= 1 else None

    is_stock = strategy_input.asset_type == "stock"
    base_entry_signal = _base_entry_signal_at(
        strategy_input=strategy_input,
        series=series,
        index=index,
    )
    previous_base_entry_signal = _base_entry_signal_at(
        strategy_input=strategy_input,
        series=series,
        index=index - 1,
    )
    base_entry_trigger = base_entry_signal and (not previous_base_entry_signal)

    step = state.add_count + 1
    add_bounce_confirm = (
        previous_close is not None
        and current_close > current_bar.open
        and current_close > previous_close
        and _float_at(series.rsi_val, index) is not None
        and _float_at(series.rsi_val, index) > max(20.0, config.rsi_turn_up - 8.0)
    )
    need_atr = _spacing_atr(series.is_low_tier[index], step)
    need_dp = _min_drop_pct(series.is_low_tier[index], step)
    current_atr = _float_at(series.atr_val, index)
    gate_atr_ok = (
        state.last_add_price is not None
        and current_atr is not None
        and current_close <= (state.last_add_price - current_atr * need_atr)
    )
    gate_dp_ok = (
        state.position_avg_price is not None
        and state.position_avg_price > 0
        and current_close <= (state.position_avg_price * (1.0 - need_dp / 100.0))
    )
    step_dollars = _step_dollars_from_first(step, config.first_lot_dollars, config)
    cap_now = _max_basket_dollars(config.strategy_initial_capital, config.max_basket_pct_equity)
    cap_ok = (state.dollars_used + step_dollars) <= cap_now + 1e-10
    add_signal_raw = _add_signal_raw_at(
        strategy_input=strategy_input,
        config=config,
        state=state,
        series=series,
        index=index,
        add_bounce_confirm=add_bounce_confirm,
        gate_atr_ok=gate_atr_ok,
        gate_dp_ok=gate_dp_ok,
        cap_ok=cap_ok,
    )
    previous_add_signal_raw = _add_signal_raw_at(
        strategy_input=strategy_input,
        config=config,
        state=state,
        series=series,
        index=index - 1,
    )
    add_trigger = add_signal_raw and (not previous_add_signal_raw)
    hit_atr_trail = state.trail_stop is not None and current_close <= state.trail_stop
    hit_regime = config.exit_on_regime_fail and series.regime_fail[index]

    return PineSignalSnapshot(
        base_entry_signal=base_entry_signal,
        base_entry_trigger=base_entry_trigger,
        add_bounce_confirm=add_bounce_confirm,
        gate_atr_ok=gate_atr_ok,
        gate_dp_ok=gate_dp_ok,
        cap_ok=cap_ok,
        add_signal_raw=add_signal_raw,
        add_trigger=add_trigger,
        hit_atr_trail=hit_atr_trail,
        hit_regime=hit_regime,
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
        "message": "Pine source-truth inputs, derived series, signal names, and alert naming are synchronized, but execution parity is not implemented.",
        "series_type": type(series).__name__,
        "signal_type": type(signals).__name__,
    }


def _float_at(values: list[float | None], index: int) -> float | None:
    if index < 0 or index >= len(values):
        return None
    return values[index]


def _bool_at(series_value: list[bool], index: int) -> bool:
    if index < 0 or index >= len(series_value):
        return False
    return series_value[index]


def _is_weekday(timestamp: datetime) -> bool:
    return timestamp.weekday() <= 4


def _session_filter_membership(timestamp: datetime, trade_session: str) -> bool:
    try:
        start_raw, end_raw = trade_session.split("-", maxsplit=1)
        start_minutes = _hhmm_to_minutes(start_raw)
        end_minutes = _hhmm_to_minutes(end_raw)
    except ValueError:
        return False

    current_minutes = timestamp.hour * 60 + timestamp.minute
    return start_minutes <= current_minutes <= end_minutes


def _hhmm_to_minutes(value: str) -> int:
    if len(value) != 4 or not value.isdigit():
        raise ValueError("Expected HHMM session string.")
    return (int(value[:2]) * 60) + int(value[2:])


def _spacing_atr(is_low_tier: bool, step: int) -> float:
    if is_low_tier:
        return 1.5 if step == 1 else 1.8 if step == 2 else 2.2 if step == 3 else 2.8
    return 3.0 if step == 1 else 3.0 if step == 2 else 4.0 if step == 3 else 7.0


def _min_drop_pct(is_low_tier: bool, step: int) -> float:
    if is_low_tier:
        return 0.80 if step == 1 else 1.30 if step == 2 else 1.90 if step == 3 else 2.60
    return 2.80 if step == 1 else 3.30 if step == 2 else 7.90 if step == 3 else 9.60


def _qty_mult(step: int, config: BismillahTrobotStocksV1Config) -> float:
    if step == 0:
        return 1.0
    if step == 1:
        return config.q1
    if step == 2:
        return config.q2
    if step == 3:
        return config.q3
    return config.q4


def _step_dollars_from_first(
    step: int,
    first_dollars: float,
    config: BismillahTrobotStocksV1Config,
) -> float:
    return first_dollars * _qty_mult(step, config)


def _max_basket_dollars(equity_reference: float, configured_cap_pct: float) -> float:
    return equity_reference * (configured_cap_pct / 100.0)


def _base_entry_signal_at(
    strategy_input: BismillahTrobotStocksV1Input,
    series: PineComputedSeries,
    index: int,
) -> bool:
    if index < 0 or index >= len(series.trend_ok):
        return False
    return (
        strategy_input.asset_type == "stock"
        and (not series.pause_new_basket[index])
        and series.trend_ok[index]
        and series.in_pullback_zone[index]
        and series.momentum_confirm[index]
    )


def _add_signal_raw_at(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
    state: BismillahTrobotStocksV1State,
    series: PineComputedSeries,
    index: int,
    add_bounce_confirm: bool | None = None,
    gate_atr_ok: bool | None = None,
    gate_dp_ok: bool | None = None,
    cap_ok: bool | None = None,
) -> bool:
    if index < 0 or index >= len(strategy_input.execution_bars):
        return False

    current_bar = strategy_input.execution_bars[index]
    previous_close = strategy_input.execution_bars[index - 1].close if index >= 1 else None
    step = state.add_count + 1
    resolved_add_bounce_confirm = (
        add_bounce_confirm
        if add_bounce_confirm is not None
        else (
            previous_close is not None
            and current_bar.close > current_bar.open
            and current_bar.close > previous_close
            and _float_at(series.rsi_val, index) is not None
            and _float_at(series.rsi_val, index) > max(20.0, config.rsi_turn_up - 8.0)
        )
    )
    resolved_gate_atr_ok = (
        gate_atr_ok
        if gate_atr_ok is not None
        else (
            state.last_add_price is not None
            and _float_at(series.atr_val, index) is not None
            and current_bar.close
            <= (
                state.last_add_price
                - _float_at(series.atr_val, index) * _spacing_atr(series.is_low_tier[index], step)
            )
        )
    )
    resolved_gate_dp_ok = (
        gate_dp_ok
        if gate_dp_ok is not None
        else (
            state.position_avg_price is not None
            and state.position_avg_price > 0
            and current_bar.close
            <= (state.position_avg_price * (1.0 - _min_drop_pct(series.is_low_tier[index], step) / 100.0))
        )
    )
    resolved_cap_ok = (
        cap_ok
        if cap_ok is not None
        else (
            (
                state.dollars_used
                + _step_dollars_from_first(step, config.first_lot_dollars, config)
            )
            <= _max_basket_dollars(config.strategy_initial_capital, config.max_basket_pct_equity) + 1e-10
        )
    )
    return (
        strategy_input.asset_type == "stock"
        and series.in_pullback_zone[index]
        and (state.add_count < config.max_adds)
        and (not series.pause_adds[index])
        and resolved_add_bounce_confirm
        and resolved_gate_atr_ok
        and resolved_gate_dp_ok
        and resolved_cap_ok
    )
