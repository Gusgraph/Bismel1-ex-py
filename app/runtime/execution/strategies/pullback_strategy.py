from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_PULLBACK_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_PULLBACK_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})


@dataclass(frozen=True)
class PullbackStrategyConfig:
    trend_ema_length: int
    pullback_percent: float
    confirmation_bars: int
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "PullbackStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        trend_ema_length = _require_positive_int(source.get("trend_ema_length", 50), field_name="trend_ema_length")
        pullback_percent = _require_positive_float(source.get("pullback_percent", 2.0), field_name="pullback_percent")
        confirmation_bars = _require_positive_int(source.get("confirmation_bars", 1), field_name="confirmation_bars")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_PULLBACK_TIMEFRAMES:
            raise ValueError(
                f"Pullback strategy timeframe must be one of {sorted(SUPPORTED_PULLBACK_TIMEFRAMES)}."
            )
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_PULLBACK_DIRECTION_FILTERS:
            raise ValueError(
                f"Pullback strategy direction_filter must be one of {sorted(SUPPORTED_PULLBACK_DIRECTION_FILTERS)}."
            )
        return cls(
            trend_ema_length=trend_ema_length,
            pullback_percent=pullback_percent,
            confirmation_bars=confirmation_bars,
            timeframe=timeframe,
            direction_filter=direction_filter,
        )

    @property
    def required_bar_count(self) -> int:
        return max(self.trend_ema_length + self.confirmation_bars + 3, self.trend_ema_length + 4)


@dataclass(frozen=True)
class PullbackStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    trend_ema: float | None = None
    pullback_drawdown_percent: float | None = None


def evaluate_pullback_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: PullbackStrategyConfig,
) -> PullbackStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return PullbackStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Pullback strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )

    closes = [float(bar.close) for bar in bars]
    ema_series = _ema_series(closes, config.trend_ema_length)
    latest_index = len(closes) - 1
    latest_ema = ema_series[latest_index]
    if latest_ema is None:
        return PullbackStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Pullback strategy skipped {symbol} because trend EMA values were incomplete.",
        )

    latest_bar = bars[latest_index]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    confirmation_count = config.confirmation_bars
    confirm_start = latest_index - confirmation_count + 1
    if confirm_start <= 0:
        return PullbackStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"Pullback strategy skipped {symbol} because confirmation bars were incomplete.",
        )

    recent_window_start = max(0, latest_index - max(config.trend_ema_length // 2, confirmation_count + 2))
    recent_high = max(closes[recent_window_start:latest_index])
    current_close = closes[latest_index]
    pullback_drawdown_percent = 0.0 if recent_high <= 0 else ((recent_high - current_close) / recent_high) * 100.0
    confirmations = closes[confirm_start: latest_index + 1]
    recovering = all(confirmations[index] > confirmations[index - 1] for index in range(1, len(confirmations)))
    bullish_trend = current_close > latest_ema
    bullish_recovery = current_close >= closes[latest_index - 1]

    if bullish_trend and pullback_drawdown_percent >= config.pullback_percent and recovering and bullish_recovery:
        if config.direction_filter in {"short_only", "sell_only"}:
            return PullbackStrategyEvaluation(
                status="skipped_direction_filter",
                message=f"Pullback strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                signal_name="pullback_recovery",
                latest_close=current_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                trend_ema=latest_ema,
                pullback_drawdown_percent=pullback_drawdown_percent,
            )
        return PullbackStrategyEvaluation(
            status="signal_ready",
            message=f"Pullback strategy generated buy for {symbol}.",
            action="buy",
            signal_name="pullback_recovery",
            latest_close=current_close,
            latest_bar_ended_at=latest_bar_ended_at.isoformat(),
            trend_ema=latest_ema,
            pullback_drawdown_percent=pullback_drawdown_percent,
        )

    trend_break = current_close < latest_ema and closes[latest_index - 1] < ema_series[latest_index - 1]
    if trend_break:
        if config.direction_filter == "long_only":
            return PullbackStrategyEvaluation(
                status="signal_ready",
                message=f"Pullback strategy generated close for {symbol}.",
                action="close",
                signal_name="trend_break_close",
                latest_close=current_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                trend_ema=latest_ema,
                pullback_drawdown_percent=pullback_drawdown_percent,
            )
        if config.direction_filter in {"short_only", "sell_only"}:
            return PullbackStrategyEvaluation(
                status="signal_ready",
                message=f"Pullback strategy generated close for {symbol}.",
                action="close",
                signal_name="trend_break_close",
                latest_close=current_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                trend_ema=latest_ema,
                pullback_drawdown_percent=pullback_drawdown_percent,
            )

    return PullbackStrategyEvaluation(
        status="no_signal",
        message=f"Pullback strategy found no confirmed pullback entry for {symbol}.",
        latest_close=current_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        trend_ema=latest_ema,
        pullback_drawdown_percent=pullback_drawdown_percent,
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
        raise ValueError(f"Pullback strategy requires integer {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"Pullback strategy requires {field_name} to be greater than zero.")
    return resolved


def _require_positive_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Pullback strategy requires numeric {field_name}.") from exc
    if resolved <= 0:
        raise ValueError(f"Pullback strategy requires {field_name} to be greater than zero.")
    return resolved
