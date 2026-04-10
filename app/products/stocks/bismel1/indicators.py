# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/products/stocks/bismel1/indicators.py
# ======================================================

from __future__ import annotations

from datetime import datetime

from app.products.stocks.bismel1.models import PriceBar


def ema(values: list[float], length: int) -> list[float | None]:
    """Return a Pine-style EMA series using alpha=2/(length+1)."""

    if length < 1:
        raise ValueError("EMA length must be >= 1.")

    result: list[float | None] = []
    alpha = 2.0 / (length + 1.0)
    previous: float | None = None
    for value in values:
        previous = value if previous is None else (alpha * value) + ((1.0 - alpha) * previous)
        result.append(previous)
    return result


def rsi(values: list[float], length: int) -> list[float | None]:
    """Return a Wilder RSI series for parity scaffolding."""

    if length < 1:
        raise ValueError("RSI length must be >= 1.")
    if not values:
        return []

    result: list[float | None] = [None]
    avg_gain: float | None = None
    avg_loss: float | None = None

    for index in range(1, len(values)):
        change = values[index] - values[index - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if index < length:
            result.append(None)
            avg_gain = gain if avg_gain is None else avg_gain + gain
            avg_loss = loss if avg_loss is None else avg_loss + loss
            continue
        if index == length:
            avg_gain = ((avg_gain or 0.0) + gain) / length
            avg_loss = ((avg_loss or 0.0) + loss) / length
        else:
            avg_gain = ((avg_gain or 0.0) * (length - 1) + gain) / length
            avg_loss = ((avg_loss or 0.0) * (length - 1) + loss) / length
        if avg_loss == 0.0:
            result.append(100.0)
            continue
        rs = (avg_gain or 0.0) / avg_loss
        result.append(100.0 - (100.0 / (1.0 + rs)))
    return result


def true_range(high: list[float], low: list[float], close: list[float]) -> list[float | None]:
    """Return true range values required by ATR."""

    if not (len(high) == len(low) == len(close)):
        raise ValueError("High, low, and close series must have the same length.")
    if not close:
        return []

    result: list[float | None] = [high[0] - low[0]]
    for index in range(1, len(close)):
        result.append(
            max(
                high[index] - low[index],
                abs(high[index] - close[index - 1]),
                abs(low[index] - close[index - 1]),
            )
        )
    return result


def atr(high: list[float], low: list[float], close: list[float], length: int) -> list[float | None]:
    """Return Wilder ATR values for Pine parity scaffolding."""

    if length < 1:
        raise ValueError("ATR length must be >= 1.")

    tr = true_range(high, low, close)
    if not tr:
        return []

    result: list[float | None] = []
    running: float | None = None
    for index, value in enumerate(tr):
        if value is None:
            result.append(None)
            continue
        if index < length:
            running = value if running is None else running + value
            result.append(None)
            continue
        if index == length:
            running = ((running or 0.0) + value) / length
        else:
            running = (((running or 0.0) * (length - 1)) + value) / length
        result.append(running)
    return result


def rolling_highest(values: list[float], length: int) -> list[float | None]:
    """Return the highest value over the inclusive lookback window."""

    if length < 1:
        raise ValueError("Highest length must be >= 1.")
    return [
        None if index + 1 < length else max(values[index - length + 1 : index + 1])
        for index in range(len(values))
    ]


def rolling_lowest(values: list[float], length: int) -> list[float | None]:
    """Return the lowest value over the inclusive lookback window."""

    if length < 1:
        raise ValueError("Lowest length must be >= 1.")
    return [
        None if index + 1 < length else min(values[index - length + 1 : index + 1])
        for index in range(len(values))
    ]


def pct_up_price(base: float, pct: float) -> float:
    """Match Pine helper `f_pct_up_price`."""

    return base * (1.0 + pct / 100.0)


def shift_series(values: list[float | None], bars_back: int) -> list[float | None]:
    """Shift a series backward by `bars_back`, matching Pine `[n]` indexing."""

    if bars_back < 0:
        raise ValueError("bars_back must be >= 0.")
    if bars_back == 0:
        return list(values)
    if not values:
        return []
    if bars_back >= len(values):
        return [None] * len(values)
    return ([None] * bars_back) + list(values[:-bars_back])


def merge_htf_series(
    execution_bars: list[PriceBar],
    htf_bars: list[PriceBar],
    htf_values: list[float | None],
) -> list[float | None]:
    """Approximate Pine `request.security(..., gaps_off, lookahead_off)` on execution bars.

    The current HTF bar only becomes visible once that HTF bar is closed. Earlier
    child bars keep the last confirmed non-`None` HTF value.
    """

    if len(htf_bars) != len(htf_values):
        raise ValueError("htf_bars and htf_values must have the same length.")
    if not execution_bars:
        return []
    if not htf_bars:
        return [None] * len(execution_bars)

    confirmed_value: float | None = None
    merged: list[float | None] = []
    htf_index = 0

    for exec_index, execution_bar in enumerate(execution_bars):
        execution_bar_ends_at = _resolve_bar_close_at(execution_bars, exec_index)
        if execution_bar_ends_at is None:
            merged.append(confirmed_value)
            continue

        while htf_index < len(htf_bars):
            htf_bar_ends_at = _resolve_bar_close_at(htf_bars, htf_index)
            if htf_bar_ends_at is None or htf_bar_ends_at > execution_bar_ends_at:
                break
            htf_value = htf_values[htf_index]
            if htf_value is not None:
                confirmed_value = htf_value
            htf_index += 1

        merged.append(confirmed_value)

    return merged


def _resolve_bar_close_at(bars: list[PriceBar], index: int) -> datetime | None:
    """Return a best-effort close timestamp for a bar without inventing future bars."""

    bar = bars[index]
    if bar.ends_at is not None:
        return bar.ends_at
    if index + 1 < len(bars):
        return bars[index + 1].starts_at
    return None
