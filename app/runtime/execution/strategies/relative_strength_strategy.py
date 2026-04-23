from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_RELATIVE_STRENGTH_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_RELATIVE_STRENGTH_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class RelativeStrengthStrategyConfig:
    benchmark_symbol: str
    strength_window: int
    min_relative_outperformance_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "RelativeStrengthStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        benchmark_symbol = str(source.get("benchmark_symbol", "SPY")).strip().upper() or "SPY"
        strength_window = _require_positive_int(source.get("strength_window", 10), field_name="strength_window", minimum=2)
        min_relative_outperformance_percent = _require_non_negative_float(
            source.get("min_relative_outperformance_percent", 1.0),
            field_name="min_relative_outperformance_percent",
        )
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_RELATIVE_STRENGTH_TIMEFRAMES:
            raise ValueError(f"Relative Strength strategy timeframe must be one of {sorted(SUPPORTED_RELATIVE_STRENGTH_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_RELATIVE_STRENGTH_DIRECTION_FILTERS:
            raise ValueError(
                f"Relative Strength strategy direction_filter must be one of {sorted(SUPPORTED_RELATIVE_STRENGTH_DIRECTION_FILTERS)}."
            )
        return cls(benchmark_symbol, strength_window, min_relative_outperformance_percent, timeframe, direction_filter)

    @property
    def required_bar_count(self) -> int:
        return self.strength_window + 2


@dataclass(frozen=True)
class RelativeStrengthStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    relative_outperformance_percent: float | None = None
    benchmark_symbol: str | None = None


def evaluate_relative_strength_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    benchmark_bars: Sequence[PriceBar],
    config: RelativeStrengthStrategyConfig,
) -> RelativeStrengthStrategyEvaluation:
    if len(bars) < config.required_bar_count or len(benchmark_bars) < config.required_bar_count:
        return RelativeStrengthStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Relative Strength strategy skipped {symbol} because symbol or benchmark bars were unavailable.",
            benchmark_symbol=config.benchmark_symbol,
        )

    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    symbol_start = float(bars[-(config.strength_window + 1)].close)
    symbol_end = float(latest_bar.close)
    benchmark_start = float(benchmark_bars[-(config.strength_window + 1)].close)
    benchmark_end = float(benchmark_bars[-1].close)
    if symbol_start <= 0 or benchmark_start <= 0:
        return RelativeStrengthStrategyEvaluation(
            status="skipped_invalid_config",
            message=f"Relative Strength strategy skipped {symbol} because benchmark inputs were invalid.",
            benchmark_symbol=config.benchmark_symbol,
        )

    symbol_return = ((symbol_end - symbol_start) / symbol_start) * 100.0
    benchmark_return = ((benchmark_end - benchmark_start) / benchmark_start) * 100.0
    relative_outperformance = symbol_return - benchmark_return
    previous_symbol = float(previous_bar.close)
    bullish_structure = symbol_end > previous_symbol and symbol_end > symbol_start
    if relative_outperformance >= config.min_relative_outperformance_percent and bullish_structure:
        if config.direction_filter in {"short_only", "sell_only"}:
            return RelativeStrengthStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Relative Strength strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="relative_strength_buy",
                latest_close=symbol_end,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                relative_outperformance_percent=relative_outperformance,
                benchmark_symbol=config.benchmark_symbol,
            )
        return RelativeStrengthStrategyEvaluation(
            status="signal_ready",
            message=f"Relative Strength strategy generated buy for {symbol}.",
            action="buy",
            signal_name="relative_strength_buy",
            latest_close=symbol_end,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            relative_outperformance_percent=relative_outperformance,
            benchmark_symbol=config.benchmark_symbol,
        )

    if relative_outperformance < max(config.min_relative_outperformance_percent * 0.25, 0.0) or symbol_end < previous_symbol:
        return RelativeStrengthStrategyEvaluation(
            status="signal_ready",
            message=f"Relative Strength strategy generated close for {symbol}.",
            action="close",
            signal_name="relative_strength_close",
            latest_close=symbol_end,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            relative_outperformance_percent=relative_outperformance,
            benchmark_symbol=config.benchmark_symbol,
        )

    return RelativeStrengthStrategyEvaluation(
        status="no_signal",
        message=f"Relative Strength strategy found no closed-bar signal for {symbol}.",
        latest_close=symbol_end,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        relative_outperformance_percent=relative_outperformance,
        benchmark_symbol=config.benchmark_symbol,
    )


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Relative Strength strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Relative Strength strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_non_negative_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Relative Strength strategy requires numeric {field_name}.") from exc
    if resolved < 0:
        raise ValueError(f"Relative Strength strategy requires {field_name} to be zero or greater.")
    return resolved
