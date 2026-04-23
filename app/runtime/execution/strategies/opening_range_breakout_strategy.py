from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_ORB_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_ORB_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class OpeningRangeBreakoutStrategyConfig:
    opening_range_minutes: int
    breakout_buffer_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "OpeningRangeBreakoutStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        opening_range_minutes = _require_positive_int(source.get("opening_range_minutes", 30), field_name="opening_range_minutes", minimum=1)
        breakout_buffer_percent = _require_non_negative_float(source.get("breakout_buffer_percent", 0.1), field_name="breakout_buffer_percent")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_ORB_TIMEFRAMES:
            raise ValueError(f"Opening Range Breakout strategy timeframe must be one of {sorted(SUPPORTED_ORB_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_ORB_DIRECTION_FILTERS:
            raise ValueError(f"Opening Range Breakout strategy direction_filter must be one of {sorted(SUPPORTED_ORB_DIRECTION_FILTERS)}.")
        return cls(opening_range_minutes, breakout_buffer_percent, timeframe, direction_filter)

    @property
    def required_bar_count(self) -> int:
        return 4


@dataclass(frozen=True)
class OpeningRangeBreakoutStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    opening_range_high: float | None = None
    opening_range_low: float | None = None


def evaluate_opening_range_breakout_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: OpeningRangeBreakoutStrategyConfig,
) -> OpeningRangeBreakoutStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return OpeningRangeBreakoutStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Opening Range Breakout strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )
    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    session_bars = _session_bars(bars)
    if len(session_bars) < 2:
        return OpeningRangeBreakoutStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Opening Range Breakout strategy skipped {symbol} because the session range could not be derived.",
        )
    session_start = _bar_time(session_bars[0])
    opening_bars = [
        bar for bar in session_bars
        if ((_bar_time(bar) - session_start).total_seconds() / 60.0) < config.opening_range_minutes
    ]
    if len(opening_bars) < 1:
        return OpeningRangeBreakoutStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Opening Range Breakout strategy skipped {symbol} because the opening range was unavailable.",
        )
    last_opening_bar_time = _bar_time(opening_bars[-1])
    if _bar_time(latest_bar) <= last_opening_bar_time:
        return OpeningRangeBreakoutStrategyEvaluation(
            status="no_signal",
            message=f"Opening Range Breakout strategy is waiting for the opening range to complete for {symbol}.",
            latest_close=float(latest_bar.close),
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        )
    opening_range_high = max(float(bar.high) for bar in opening_bars)
    opening_range_low = min(float(bar.low) for bar in opening_bars)
    buffer_ratio = config.breakout_buffer_percent / 100.0
    breakout_threshold = opening_range_high * (1.0 + buffer_ratio)
    breakdown_threshold = opening_range_low * (1.0 - buffer_ratio)
    latest_close = float(latest_bar.close)
    previous_close = float(previous_bar.close)
    if latest_close > breakout_threshold and previous_close <= breakout_threshold:
        if config.direction_filter in {"short_only", "sell_only"}:
            return OpeningRangeBreakoutStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Opening Range Breakout strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="opening_range_breakout_up",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                opening_range_high=opening_range_high,
                opening_range_low=opening_range_low,
            )
        return OpeningRangeBreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Opening Range Breakout strategy generated buy for {symbol}.",
            action="buy",
            signal_name="opening_range_breakout_up",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            opening_range_high=opening_range_high,
            opening_range_low=opening_range_low,
        )
    if latest_close < breakdown_threshold and previous_close >= breakdown_threshold:
        return OpeningRangeBreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Opening Range Breakout strategy generated close for {symbol}.",
            action="close",
            signal_name="opening_range_breakdown_close",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            opening_range_high=opening_range_high,
            opening_range_low=opening_range_low,
        )
    return OpeningRangeBreakoutStrategyEvaluation(
        status="no_signal",
        message=f"Opening Range Breakout strategy found no closed-bar signal for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
    )


def _session_bars(bars: Sequence[PriceBar]) -> list[PriceBar]:
    latest_day = _bar_time(bars[-1]).date()
    return [bar for bar in bars if _bar_time(bar).date() == latest_day]


def _bar_time(bar: PriceBar) -> datetime:
    return bar.ends_at or bar.starts_at


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Opening Range Breakout strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Opening Range Breakout strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_non_negative_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Opening Range Breakout strategy requires numeric {field_name}.") from exc
    if resolved < 0:
        raise ValueError(f"Opening Range Breakout strategy requires {field_name} to be zero or greater.")
    return resolved
