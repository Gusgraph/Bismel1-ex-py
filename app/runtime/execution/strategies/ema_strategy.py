from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_EMA_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class EmaStrategyConfig:
    fast_ema_length: int
    slow_ema_length: int
    timeframe: str
    direction_filter: str = "both"
    cross_confirmation: bool = False

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "EmaStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        fast = _require_positive_int(source.get("fast_ema_length", 9), field_name="fast_ema_length")
        slow = _require_positive_int(source.get("slow_ema_length", 21), field_name="slow_ema_length")
        if fast >= slow:
            raise ValueError("EMA strategy requires fast_ema_length to be lower than slow_ema_length.")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_EMA_TIMEFRAMES:
            raise ValueError(
                f"EMA strategy timeframe must be one of {sorted(SUPPORTED_EMA_TIMEFRAMES)}."
            )
        direction_filter = str(source.get("direction_filter", "both")).strip().lower() or "both"
        if direction_filter not in SUPPORTED_DIRECTION_FILTERS:
            raise ValueError(
                f"EMA strategy direction_filter must be one of {sorted(SUPPORTED_DIRECTION_FILTERS)}."
            )
        cross_confirmation = bool(source.get("cross_confirmation", False))
        return cls(
            fast_ema_length=fast,
            slow_ema_length=slow,
            timeframe=timeframe,
            direction_filter=direction_filter,
            cross_confirmation=cross_confirmation,
        )

    @property
    def required_bar_count(self) -> int:
        return max(self.fast_ema_length, self.slow_ema_length) + 2


@dataclass(frozen=True)
class EmaStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    fast_ema: float | None = None
    slow_ema: float | None = None


def evaluate_ema_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: EmaStrategyConfig,
) -> EmaStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return EmaStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=(
                f"EMA strategy skipped {symbol} because only {len(bars)} closed bars were available."
            ),
        )

    closes = [float(bar.close) for bar in bars]
    fast_series = _ema_series(closes, config.fast_ema_length)
    slow_series = _ema_series(closes, config.slow_ema_length)
    latest_index = len(closes) - 1
    current_fast = fast_series[latest_index]
    current_slow = slow_series[latest_index]
    if current_fast is None or current_slow is None:
        return EmaStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"EMA strategy skipped {symbol} because EMA values were incomplete.",
        )

    latest_bar = bars[latest_index]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    action: str | None = None
    signal_name: str | None = None

    if config.cross_confirmation:
        if len(closes) < 3:
            return EmaStrategyEvaluation(
                status="skipped_market_data_unavailable",
                message=f"EMA strategy skipped {symbol} because confirmation needs three closed bars.",
            )
        prev_index = latest_index - 1
        prior_index = latest_index - 2
        prev_fast = fast_series[prev_index]
        prev_slow = slow_series[prev_index]
        prior_fast = fast_series[prior_index]
        prior_slow = slow_series[prior_index]
        if None in {prev_fast, prev_slow, prior_fast, prior_slow}:
            return EmaStrategyEvaluation(
                status="skipped_market_data_unavailable",
                message=f"EMA strategy skipped {symbol} because confirmation EMA values were incomplete.",
            )
        cross_up = bool(prior_fast <= prior_slow and prev_fast > prev_slow and current_fast > current_slow)
        cross_down = bool(prior_fast >= prior_slow and prev_fast < prev_slow and current_fast < current_slow)
    else:
        prev_index = latest_index - 1
        prev_fast = fast_series[prev_index]
        prev_slow = slow_series[prev_index]
        if prev_fast is None or prev_slow is None:
            return EmaStrategyEvaluation(
                status="skipped_market_data_unavailable",
                message=f"EMA strategy skipped {symbol} because prior EMA values were incomplete.",
            )
        cross_up = bool(prev_fast <= prev_slow and current_fast > current_slow)
        cross_down = bool(prev_fast >= prev_slow and current_fast < current_slow)

    if cross_up:
        signal_name = "ema_cross_up"
        if config.direction_filter in {"short_only", "sell_only"}:
            return EmaStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"EMA strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name=signal_name,
                latest_close=latest_bar.close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                fast_ema=current_fast,
                slow_ema=current_slow,
            )
        action = "buy"
    elif cross_down:
        signal_name = "ema_cross_down"
        if config.direction_filter == "long_only":
            return EmaStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"EMA strategy skipped {symbol} close because direction_filter=long_only.",
                signal_name=signal_name,
                latest_close=latest_bar.close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                fast_ema=current_fast,
                slow_ema=current_slow,
            )
        action = "close"
    else:
        return EmaStrategyEvaluation(
            status="no_signal",
            message=f"EMA strategy found no closed-bar crossover for {symbol}.",
            latest_close=latest_bar.close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            fast_ema=current_fast,
            slow_ema=current_slow,
        )

    return EmaStrategyEvaluation(
        status="signal_ready",
        message=f"EMA strategy generated {action} for {symbol}.",
        action=action,
        signal_name=signal_name,
        latest_close=latest_bar.close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        fast_ema=current_fast,
        slow_ema=current_slow,
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


def _require_positive_int(value: object, *, field_name: str) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"EMA strategy requires integer {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"EMA strategy requires {field_name} to be greater than zero.")
    return resolved
