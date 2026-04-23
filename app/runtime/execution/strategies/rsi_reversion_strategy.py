from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_RSI_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_RSI_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class RsiReversionStrategyConfig:
    rsi_length: int
    oversold_level: float
    overbought_level: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "RsiReversionStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        rsi_length = _require_positive_int(source.get("rsi_length", 14), field_name="rsi_length", minimum=2)
        oversold_level = _require_rsi_level(source.get("oversold_level", 30), field_name="oversold_level")
        overbought_level = _require_rsi_level(source.get("overbought_level", 70), field_name="overbought_level")
        if oversold_level >= overbought_level:
            raise ValueError("RSI reversion strategy requires oversold_level to be lower than overbought_level.")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_RSI_TIMEFRAMES:
            raise ValueError(
                f"RSI reversion strategy timeframe must be one of {sorted(SUPPORTED_RSI_TIMEFRAMES)}."
            )
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_RSI_DIRECTION_FILTERS:
            raise ValueError(
                f"RSI reversion strategy direction_filter must be one of {sorted(SUPPORTED_RSI_DIRECTION_FILTERS)}."
            )
        return cls(
            rsi_length=rsi_length,
            oversold_level=oversold_level,
            overbought_level=overbought_level,
            timeframe=timeframe,
            direction_filter=direction_filter,
        )

    @property
    def required_bar_count(self) -> int:
        return self.rsi_length + 3


@dataclass(frozen=True)
class RsiReversionStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    current_rsi: float | None = None
    previous_rsi: float | None = None


def evaluate_rsi_reversion_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: RsiReversionStrategyConfig,
) -> RsiReversionStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return RsiReversionStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"RSI reversion strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )

    closes = [float(bar.close) for bar in bars]
    rsi_series = _rsi_series(closes, config.rsi_length)
    current_rsi = rsi_series[-1]
    previous_rsi = rsi_series[-2]
    if current_rsi is None or previous_rsi is None:
        return RsiReversionStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"RSI reversion strategy skipped {symbol} because RSI values were incomplete.",
        )

    latest_bar = bars[-1]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at

    if previous_rsi <= config.oversold_level and current_rsi > config.oversold_level:
        if config.direction_filter in {"short_only", "sell_only"}:
            return RsiReversionStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"RSI reversion strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="rsi_oversold_recovery",
                latest_close=closes[-1],
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                current_rsi=current_rsi,
                previous_rsi=previous_rsi,
            )
        return RsiReversionStrategyEvaluation(
            status="signal_ready",
            message=f"RSI reversion strategy generated buy for {symbol}.",
            action="buy",
            signal_name="rsi_oversold_recovery",
            latest_close=closes[-1],
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            current_rsi=current_rsi,
            previous_rsi=previous_rsi,
        )

    if previous_rsi < config.overbought_level and current_rsi >= config.overbought_level:
        return RsiReversionStrategyEvaluation(
            status="signal_ready",
            message=f"RSI reversion strategy generated close for {symbol}.",
            action="close",
            signal_name="rsi_overbought_exit",
            latest_close=closes[-1],
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            current_rsi=current_rsi,
            previous_rsi=previous_rsi,
        )

    return RsiReversionStrategyEvaluation(
        status="no_signal",
        message=f"RSI reversion strategy found no closed-bar reversion setup for {symbol}.",
        latest_close=closes[-1],
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        current_rsi=current_rsi,
        previous_rsi=previous_rsi,
    )


def _rsi_series(values: Sequence[float], length: int) -> list[float | None]:
    if len(values) <= length:
        return [None] * len(values)
    series: list[float | None] = [None] * len(values)
    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, length + 1):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    average_gain = sum(gains) / length
    average_loss = sum(losses) / length
    series[length] = _rsi_value(average_gain, average_loss)
    for index in range(length + 1, len(values)):
        change = values[index] - values[index - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        average_gain = ((average_gain * (length - 1)) + gain) / length
        average_loss = ((average_loss * (length - 1)) + loss) / length
        series[index] = _rsi_value(average_gain, average_loss)
    return series


def _rsi_value(average_gain: float, average_loss: float) -> float:
    if average_loss == 0:
        return 100.0
    relative_strength = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"RSI reversion strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"RSI reversion strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_rsi_level(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"RSI reversion strategy requires numeric {field_name}.") from exc
    if resolved < 0 or resolved > 100:
        raise ValueError(f"RSI reversion strategy requires {field_name} to be between 0 and 100.")
    return resolved
