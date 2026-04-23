from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_ADX_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_ADX_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class AdxTrendStrategyConfig:
    adx_length: int
    adx_min_strength: float
    ema_length: int
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "AdxTrendStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        adx_length = _require_positive_int(source.get("adx_length", 14), field_name="adx_length", minimum=2)
        adx_min_strength = _require_positive_float(source.get("adx_min_strength", 20.0), field_name="adx_min_strength")
        ema_length = _require_positive_int(source.get("ema_length", 50), field_name="ema_length")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_ADX_TIMEFRAMES:
            raise ValueError(f"ADX Trend strategy timeframe must be one of {sorted(SUPPORTED_ADX_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_ADX_DIRECTION_FILTERS:
            raise ValueError(f"ADX Trend strategy direction_filter must be one of {sorted(SUPPORTED_ADX_DIRECTION_FILTERS)}.")
        return cls(adx_length, adx_min_strength, ema_length, timeframe, direction_filter)

    @property
    def required_bar_count(self) -> int:
        return max(self.adx_length * 3, self.ema_length + 2)


@dataclass(frozen=True)
class AdxTrendStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    ema_value: float | None = None
    adx_value: float | None = None


def evaluate_adx_trend_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: AdxTrendStrategyConfig,
) -> AdxTrendStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return AdxTrendStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"ADX Trend strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )
    closes = [float(bar.close) for bar in bars]
    ema_series = _ema_series(closes, config.ema_length)
    adx_series = _adx_series(bars, config.adx_length)
    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    ema_value = ema_series[-1]
    prev_ema_value = ema_series[-2]
    adx_value = adx_series[-1]
    prev_adx_value = adx_series[-2]
    if ema_value is None or prev_ema_value is None or adx_value is None or prev_adx_value is None:
        return AdxTrendStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"ADX Trend strategy skipped {symbol} because EMA/ADX values were incomplete.",
        )
    latest_close = float(latest_bar.close)
    previous_close = float(previous_bar.close)
    bullish = latest_close > ema_value and adx_value >= config.adx_min_strength
    trend_failed = latest_close < ema_value or adx_value < max(config.adx_min_strength * 0.7, 10.0)

    if bullish:
        if config.direction_filter in {"short_only", "sell_only"}:
            return AdxTrendStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"ADX Trend strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="adx_trend_buy",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                ema_value=ema_value,
                adx_value=adx_value,
            )
        return AdxTrendStrategyEvaluation(
            status="signal_ready",
            message=f"ADX Trend strategy generated buy for {symbol}.",
            action="buy",
            signal_name="adx_trend_buy",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            ema_value=ema_value,
            adx_value=adx_value,
        )
    if trend_failed:
        return AdxTrendStrategyEvaluation(
            status="signal_ready",
            message=f"ADX Trend strategy generated close for {symbol}.",
            action="close",
            signal_name="adx_trend_close",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            ema_value=ema_value,
            adx_value=adx_value,
        )
    return AdxTrendStrategyEvaluation(
        status="no_signal",
        message=f"ADX Trend strategy found no closed-bar signal for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        ema_value=ema_value,
        adx_value=adx_value,
    )


def _ema_series(values: Sequence[float], length: int) -> list[float | None]:
    multiplier = 2.0 / (length + 1.0)
    series: list[float | None] = [None] * len(values)
    ema_value: float | None = None
    for index, value in enumerate(values):
        if index < length - 1:
            continue
        if index == length - 1:
            ema_value = sum(values[:length]) / length
            series[index] = ema_value
            continue
        assert ema_value is not None
        ema_value = ((value - ema_value) * multiplier) + ema_value
        series[index] = ema_value
    return series


def _adx_series(bars: Sequence[PriceBar], length: int) -> list[float | None]:
    size = len(bars)
    adx: list[float | None] = [None] * size
    if size < (length * 2):
        return adx
    trs: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for index in range(1, size):
        high = float(bars[index].high)
        low = float(bars[index].low)
        prev_high = float(bars[index - 1].high)
        prev_low = float(bars[index - 1].low)
        prev_close = float(bars[index - 1].close)
        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    for index in range((length * 2) - 1, size):
        tr_slice = trs[index - (length * 2) + 1:index - length + 1]
        plus_slice = plus_dm[index - (length * 2) + 1:index - length + 1]
        minus_slice = minus_dm[index - (length * 2) + 1:index - length + 1]
        if not tr_slice or sum(tr_slice) <= 0:
            continue
        atr = sum(tr_slice) / len(tr_slice)
        plus_di = 100.0 * ((sum(plus_slice) / len(plus_slice)) / atr)
        minus_di = 100.0 * ((sum(minus_slice) / len(minus_slice)) / atr)
        denominator = plus_di + minus_di
        if denominator <= 0:
            continue
        adx[index] = 100.0 * abs(plus_di - minus_di) / denominator
    return adx


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"ADX Trend strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"ADX Trend strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_positive_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"ADX Trend strategy requires numeric {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"ADX Trend strategy requires {field_name} to be greater than zero.")
    return resolved
