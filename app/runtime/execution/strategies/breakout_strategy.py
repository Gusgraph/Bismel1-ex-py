from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_BREAKOUT_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_BREAKOUT_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class BreakoutStrategyConfig:
    lookback_bars: int
    breakout_buffer_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "BreakoutStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        lookback_bars = _require_positive_int(source.get("lookback_bars", 20), field_name="lookback_bars", minimum=2)
        breakout_buffer_percent = _require_non_negative_float(
            source.get("breakout_buffer_percent", 0.2),
            field_name="breakout_buffer_percent",
        )
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_BREAKOUT_TIMEFRAMES:
            raise ValueError(
                f"Breakout strategy timeframe must be one of {sorted(SUPPORTED_BREAKOUT_TIMEFRAMES)}."
            )
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_BREAKOUT_DIRECTION_FILTERS:
            raise ValueError(
                f"Breakout strategy direction_filter must be one of {sorted(SUPPORTED_BREAKOUT_DIRECTION_FILTERS)}."
            )
        return cls(
            lookback_bars=lookback_bars,
            breakout_buffer_percent=breakout_buffer_percent,
            timeframe=timeframe,
            direction_filter=direction_filter,
        )

    @property
    def required_bar_count(self) -> int:
        return self.lookback_bars + 1


@dataclass(frozen=True)
class BreakoutStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    range_high: float | None = None
    range_low: float | None = None
    breakout_threshold: float | None = None
    breakdown_threshold: float | None = None


def evaluate_breakout_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: BreakoutStrategyConfig,
) -> BreakoutStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return BreakoutStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Breakout strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )

    latest_bar = bars[-1]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    history = bars[-(config.lookback_bars + 1):-1]
    prior_range_high = max(float(bar.high) for bar in history)
    prior_range_low = min(float(bar.low) for bar in history)
    latest_close = float(latest_bar.close)
    previous_close = float(bars[-2].close)
    buffer_ratio = config.breakout_buffer_percent / 100.0
    breakout_threshold = prior_range_high * (1.0 + buffer_ratio)
    breakdown_threshold = prior_range_low * (1.0 - buffer_ratio)

    if latest_close > breakout_threshold and previous_close <= breakout_threshold:
        if config.direction_filter in {"short_only", "sell_only"}:
            return BreakoutStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Breakout strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="range_breakout_up",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                range_high=prior_range_high,
                range_low=prior_range_low,
                breakout_threshold=breakout_threshold,
                breakdown_threshold=breakdown_threshold,
            )
        return BreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Breakout strategy generated buy for {symbol}.",
            action="buy",
            signal_name="range_breakout_up",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            range_high=prior_range_high,
            range_low=prior_range_low,
            breakout_threshold=breakout_threshold,
            breakdown_threshold=breakdown_threshold,
        )

    if latest_close < breakdown_threshold and previous_close >= breakdown_threshold:
        return BreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Breakout strategy generated close for {symbol}.",
            action="close",
            signal_name="range_breakdown_close",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            range_high=prior_range_high,
            range_low=prior_range_low,
            breakout_threshold=breakout_threshold,
            breakdown_threshold=breakdown_threshold,
        )

    return BreakoutStrategyEvaluation(
        status="no_signal",
        message=f"Breakout strategy found no closed-bar breakout for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        range_high=prior_range_high,
        range_low=prior_range_low,
        breakout_threshold=breakout_threshold,
        breakdown_threshold=breakdown_threshold,
    )


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Breakout strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Breakout strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_non_negative_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Breakout strategy requires numeric {field_name}.") from exc
    if resolved < 0:
        raise ValueError(f"Breakout strategy requires {field_name} to be zero or greater.")
    return resolved
