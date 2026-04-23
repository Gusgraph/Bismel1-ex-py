from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_MOMENTUM_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_MOMENTUM_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class MomentumStrategyConfig:
    momentum_window: int
    momentum_threshold_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "MomentumStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        momentum_window = _require_positive_int(source.get("momentum_window", 5), field_name="momentum_window", minimum=2)
        momentum_threshold_percent = _require_positive_float(
            source.get("momentum_threshold_percent", 2.0),
            field_name="momentum_threshold_percent",
        )
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_MOMENTUM_TIMEFRAMES:
            raise ValueError(
                f"Momentum strategy timeframe must be one of {sorted(SUPPORTED_MOMENTUM_TIMEFRAMES)}."
            )
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_MOMENTUM_DIRECTION_FILTERS:
            raise ValueError(
                f"Momentum strategy direction_filter must be one of {sorted(SUPPORTED_MOMENTUM_DIRECTION_FILTERS)}."
            )
        return cls(
            momentum_window=momentum_window,
            momentum_threshold_percent=momentum_threshold_percent,
            timeframe=timeframe,
            direction_filter=direction_filter,
        )

    @property
    def required_bar_count(self) -> int:
        return self.momentum_window + 2


@dataclass(frozen=True)
class MomentumStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    current_momentum_percent: float | None = None
    previous_momentum_percent: float | None = None


def evaluate_momentum_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: MomentumStrategyConfig,
) -> MomentumStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return MomentumStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Momentum strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )

    closes = [float(bar.close) for bar in bars]
    current_momentum = _percent_change(closes[-(config.momentum_window + 1)], closes[-1])
    previous_momentum = _percent_change(closes[-(config.momentum_window + 2)], closes[-2])
    latest_bar = bars[-1]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at

    if previous_momentum < config.momentum_threshold_percent <= current_momentum:
        if config.direction_filter in {"short_only", "sell_only"}:
            return MomentumStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Momentum strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="momentum_breakout_up",
                latest_close=closes[-1],
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                current_momentum_percent=current_momentum,
                previous_momentum_percent=previous_momentum,
            )
        return MomentumStrategyEvaluation(
            status="signal_ready",
            message=f"Momentum strategy generated buy for {symbol}.",
            action="buy",
            signal_name="momentum_breakout_up",
            latest_close=closes[-1],
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            current_momentum_percent=current_momentum,
            previous_momentum_percent=previous_momentum,
        )

    fade_threshold = config.momentum_threshold_percent * 0.5
    if previous_momentum >= config.momentum_threshold_percent and current_momentum < fade_threshold:
        return MomentumStrategyEvaluation(
            status="signal_ready",
            message=f"Momentum strategy generated close for {symbol}.",
            action="close",
            signal_name="momentum_fade_close",
            latest_close=closes[-1],
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            current_momentum_percent=current_momentum,
            previous_momentum_percent=previous_momentum,
        )

    return MomentumStrategyEvaluation(
        status="no_signal",
        message=f"Momentum strategy found no closed-bar momentum trigger for {symbol}.",
        latest_close=closes[-1],
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        current_momentum_percent=current_momentum,
        previous_momentum_percent=previous_momentum,
    )


def _percent_change(start: float, end: float) -> float:
    if start == 0:
        return 0.0
    return ((end - start) / start) * 100.0


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Momentum strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Momentum strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_positive_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Momentum strategy requires numeric {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"Momentum strategy requires {field_name} to be greater than zero.")
    return resolved
