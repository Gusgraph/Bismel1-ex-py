from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_DONCHIAN_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_DONCHIAN_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class DonchianBreakoutStrategyConfig:
    channel_length: int
    breakout_buffer_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "DonchianBreakoutStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        channel_length = _require_positive_int(source.get("channel_length", 20), field_name="channel_length", minimum=2)
        breakout_buffer_percent = _require_non_negative_float(source.get("breakout_buffer_percent", 0.1), field_name="breakout_buffer_percent")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_DONCHIAN_TIMEFRAMES:
            raise ValueError(f"Donchian Breakout strategy timeframe must be one of {sorted(SUPPORTED_DONCHIAN_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_DONCHIAN_DIRECTION_FILTERS:
            raise ValueError(f"Donchian Breakout strategy direction_filter must be one of {sorted(SUPPORTED_DONCHIAN_DIRECTION_FILTERS)}.")
        return cls(channel_length, breakout_buffer_percent, timeframe, direction_filter)

    @property
    def required_bar_count(self) -> int:
        return self.channel_length + 1


@dataclass(frozen=True)
class DonchianBreakoutStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    channel_high: float | None = None
    channel_low: float | None = None


def evaluate_donchian_breakout_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: DonchianBreakoutStrategyConfig,
) -> DonchianBreakoutStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return DonchianBreakoutStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Donchian Breakout strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )
    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    history = bars[-(config.channel_length + 1):-1]
    channel_high = max(float(bar.high) for bar in history)
    channel_low = min(float(bar.low) for bar in history)
    buffer_ratio = config.breakout_buffer_percent / 100.0
    breakout_threshold = channel_high * (1.0 + buffer_ratio)
    breakdown_threshold = channel_low * (1.0 - buffer_ratio)
    latest_close = float(latest_bar.close)
    previous_close = float(previous_bar.close)

    if latest_close > breakout_threshold and previous_close <= breakout_threshold:
        if config.direction_filter in {"short_only", "sell_only"}:
            return DonchianBreakoutStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Donchian Breakout strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="donchian_breakout_up",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                channel_high=channel_high,
                channel_low=channel_low,
            )
        return DonchianBreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Donchian Breakout strategy generated buy for {symbol}.",
            action="buy",
            signal_name="donchian_breakout_up",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            channel_high=channel_high,
            channel_low=channel_low,
        )

    if latest_close < breakdown_threshold and previous_close >= breakdown_threshold:
        return DonchianBreakoutStrategyEvaluation(
            status="signal_ready",
            message=f"Donchian Breakout strategy generated close for {symbol}.",
            action="close",
            signal_name="donchian_breakdown_close",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            channel_high=channel_high,
            channel_low=channel_low,
        )

    return DonchianBreakoutStrategyEvaluation(
        status="no_signal",
        message=f"Donchian Breakout strategy found no closed-bar signal for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        channel_high=channel_high,
        channel_low=channel_low,
    )


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Donchian Breakout strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Donchian Breakout strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_non_negative_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Donchian Breakout strategy requires numeric {field_name}.") from exc
    if resolved < 0:
        raise ValueError(f"Donchian Breakout strategy requires {field_name} to be zero or greater.")
    return resolved
