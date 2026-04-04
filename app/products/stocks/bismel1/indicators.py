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
