from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_BOLLINGER_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_BOLLINGER_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})
SUPPORTED_REENTRY_MODES = frozenset({"inside_band", "close_above_lower"})


@dataclass(frozen=True)
class BollingerReversionStrategyConfig:
    bollinger_length: int
    bollinger_stddev: float
    reentry_mode: str
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "BollingerReversionStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        length = _require_positive_int(source.get("bollinger_length", 20), field_name="bollinger_length", minimum=2)
        stddev = _require_positive_float(source.get("bollinger_stddev", 2.0), field_name="bollinger_stddev")
        reentry_mode = str(source.get("reentry_mode", "inside_band")).strip().lower() or "inside_band"
        if reentry_mode not in SUPPORTED_REENTRY_MODES:
            raise ValueError(f"Bollinger Reversion strategy reentry_mode must be one of {sorted(SUPPORTED_REENTRY_MODES)}.")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_BOLLINGER_TIMEFRAMES:
            raise ValueError(f"Bollinger Reversion strategy timeframe must be one of {sorted(SUPPORTED_BOLLINGER_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_BOLLINGER_DIRECTION_FILTERS:
            raise ValueError(
                f"Bollinger Reversion strategy direction_filter must be one of {sorted(SUPPORTED_BOLLINGER_DIRECTION_FILTERS)}."
            )
        return cls(length, stddev, reentry_mode, timeframe, direction_filter)

    @property
    def required_bar_count(self) -> int:
        return self.bollinger_length + 1


@dataclass(frozen=True)
class BollingerReversionStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    middle_band: float | None = None
    lower_band: float | None = None
    upper_band: float | None = None


def evaluate_bollinger_reversion_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: BollingerReversionStrategyConfig,
) -> BollingerReversionStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return BollingerReversionStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Bollinger Reversion strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )
    closes = [float(bar.close) for bar in bars]
    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    current_window = closes[-config.bollinger_length:]
    previous_window = closes[-(config.bollinger_length + 1):-1]
    middle = sum(current_window) / len(current_window)
    prev_middle = sum(previous_window) / len(previous_window)
    variance = sum((value - middle) ** 2 for value in current_window) / len(current_window)
    prev_variance = sum((value - prev_middle) ** 2 for value in previous_window) / len(previous_window)
    std = sqrt(variance)
    prev_std = sqrt(prev_variance)
    lower = middle - (std * config.bollinger_stddev)
    upper = middle + (std * config.bollinger_stddev)
    prev_lower = prev_middle - (prev_std * config.bollinger_stddev)
    prev_upper = prev_middle + (prev_std * config.bollinger_stddev)
    latest_close = float(latest_bar.close)
    previous_close = float(previous_bar.close)

    if config.reentry_mode == "close_above_lower":
        buy_ready = previous_close < prev_lower and latest_close > lower
    else:
        buy_ready = previous_close < prev_lower and latest_close >= lower
    if buy_ready:
        if config.direction_filter in {"short_only", "sell_only"}:
            return BollingerReversionStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Bollinger Reversion strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="bollinger_reentry_buy",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                middle_band=middle,
                lower_band=lower,
                upper_band=upper,
            )
        return BollingerReversionStrategyEvaluation(
            status="signal_ready",
            message=f"Bollinger Reversion strategy generated buy for {symbol}.",
            action="buy",
            signal_name="bollinger_reentry_buy",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            middle_band=middle,
            lower_band=lower,
            upper_band=upper,
        )

    close_ready = latest_close >= upper or (previous_close >= lower and latest_close < lower)
    if close_ready:
        return BollingerReversionStrategyEvaluation(
            status="signal_ready",
            message=f"Bollinger Reversion strategy generated close for {symbol}.",
            action="close",
            signal_name="bollinger_mean_reversion_close",
            latest_close=latest_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            middle_band=middle,
            lower_band=lower,
            upper_band=upper,
        )

    return BollingerReversionStrategyEvaluation(
        status="no_signal",
        message=f"Bollinger Reversion strategy found no closed-bar signal for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        middle_band=middle,
        lower_band=lower,
        upper_band=upper,
    )


def _require_positive_int(value: object, *, field_name: str, minimum: int = 1) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Bollinger Reversion strategy requires integer {field_name}.") from exc
    if resolved < minimum:
        raise ValueError(f"Bollinger Reversion strategy requires {field_name} to be at least {minimum}.")
    return resolved


def _require_positive_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Bollinger Reversion strategy requires numeric {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"Bollinger Reversion strategy requires {field_name} to be greater than zero.")
    return resolved
