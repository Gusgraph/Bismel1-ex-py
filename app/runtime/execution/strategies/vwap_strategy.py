from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from app.products.stocks.bismel1.models import PriceBar


SUPPORTED_VWAP_TIMEFRAMES = frozenset({"15m", "1h"})
SUPPORTED_VWAP_DIRECTION_FILTERS = frozenset({"both", "long_only", "short_only", "sell_only"})
SUPPORTED_VWAP_MODES = frozenset({"trend", "reversion"})


@dataclass(frozen=True)
class VwapStrategyConfig:
    vwap_mode: str
    deviation_percent: float
    timeframe: str
    direction_filter: str = "long_only"

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "VwapStrategyConfig":
        source = payload if isinstance(payload, dict) else {}
        vwap_mode = str(source.get("vwap_mode", "trend")).strip().lower() or "trend"
        if vwap_mode not in SUPPORTED_VWAP_MODES:
            raise ValueError(f"VWAP strategy vwap_mode must be one of {sorted(SUPPORTED_VWAP_MODES)}.")
        deviation_percent = _require_non_negative_float(source.get("deviation_percent", 0.5), field_name="deviation_percent")
        timeframe = str(source.get("timeframe", "15m")).strip().lower() or "15m"
        if timeframe not in SUPPORTED_VWAP_TIMEFRAMES:
            raise ValueError(f"VWAP strategy timeframe must be one of {sorted(SUPPORTED_VWAP_TIMEFRAMES)}.")
        direction_filter = str(source.get("direction_filter", "long_only")).strip().lower() or "long_only"
        if direction_filter not in SUPPORTED_VWAP_DIRECTION_FILTERS:
            raise ValueError(
                f"VWAP strategy direction_filter must be one of {sorted(SUPPORTED_VWAP_DIRECTION_FILTERS)}."
            )
        return cls(
            vwap_mode=vwap_mode,
            deviation_percent=deviation_percent,
            timeframe=timeframe,
            direction_filter=direction_filter,
        )

    @property
    def required_bar_count(self) -> int:
        return 4


@dataclass(frozen=True)
class VwapStrategyEvaluation:
    status: str
    message: str
    action: str | None = None
    signal_name: str | None = None
    latest_close: float | None = None
    latest_bar_ended_at: str | None = None
    session_vwap: float | None = None
    threshold_price: float | None = None


def evaluate_vwap_strategy(
    *,
    symbol: str,
    bars: Sequence[PriceBar],
    config: VwapStrategyConfig,
) -> VwapStrategyEvaluation:
    if len(bars) < config.required_bar_count:
        return VwapStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"VWAP strategy skipped {symbol} because only {len(bars)} closed bars were available.",
        )

    latest_bar = bars[-1]
    previous_bar = bars[-2]
    latest_bar_ended_at = latest_bar.ends_at or latest_bar.starts_at
    session_bars = _session_bars(bars)
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for bar in session_bars:
        typical_price = (float(bar.high) + float(bar.low) + float(bar.close)) / 3.0
        volume = float(bar.volume or 0.0)
        cumulative_pv += typical_price * (volume if volume > 0 else 1.0)
        cumulative_volume += volume if volume > 0 else 1.0
    if cumulative_volume <= 0:
        return VwapStrategyEvaluation(
            status="skipped_market_data_unavailable",
            message=f"VWAP strategy skipped {symbol} because usable VWAP volume was unavailable.",
        )

    session_vwap = cumulative_pv / cumulative_volume
    threshold_price = session_vwap * (1.0 + (config.deviation_percent / 100.0))
    support_price = session_vwap * (1.0 - (config.deviation_percent / 100.0))
    latest_close = float(latest_bar.close)
    previous_close = float(previous_bar.close)

    if config.vwap_mode == "trend":
        reclaimed_vwap = previous_close <= session_vwap and latest_close > threshold_price
        lost_vwap = previous_close >= session_vwap and latest_close < session_vwap
        if reclaimed_vwap:
            if config.direction_filter in {"short_only", "sell_only"}:
                return VwapStrategyEvaluation(
                    status="skipped_direction_filter",
                    message=f"VWAP strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                    signal_name="vwap_reclaim",
                    latest_close=latest_close,
                    latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                    session_vwap=session_vwap,
                    threshold_price=threshold_price,
                )
            return VwapStrategyEvaluation(
                status="signal_ready",
                message=f"VWAP strategy generated buy for {symbol}.",
                action="buy",
                signal_name="vwap_reclaim",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                session_vwap=session_vwap,
                threshold_price=threshold_price,
            )
        if lost_vwap:
            return VwapStrategyEvaluation(
                status="signal_ready",
                message=f"VWAP strategy generated close for {symbol}.",
                action="close",
                signal_name="vwap_loss",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                session_vwap=session_vwap,
                threshold_price=session_vwap,
            )
    else:
        reentered_band = previous_close < support_price and latest_close >= session_vwap
        failed_reversion = previous_close >= session_vwap and latest_close < support_price
        if reentered_band:
            if config.direction_filter in {"short_only", "sell_only"}:
                return VwapStrategyEvaluation(
                    status="skipped_direction_filter",
                    message=f"VWAP strategy skipped {symbol} buy because direction_filter={config.direction_filter}.",
                    signal_name="vwap_reversion_reclaim",
                    latest_close=latest_close,
                    latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                    session_vwap=session_vwap,
                    threshold_price=support_price,
                )
            return VwapStrategyEvaluation(
                status="signal_ready",
                message=f"VWAP strategy generated buy for {symbol}.",
                action="buy",
                signal_name="vwap_reversion_reclaim",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                session_vwap=session_vwap,
                threshold_price=support_price,
            )
        if failed_reversion:
            return VwapStrategyEvaluation(
                status="signal_ready",
                message=f"VWAP strategy generated close for {symbol}.",
                action="close",
                signal_name="vwap_reversion_fail",
                latest_close=latest_close,
                latest_bar_ended_at=latest_bar_ended_at.isoformat(),
                session_vwap=session_vwap,
                threshold_price=support_price,
            )

    return VwapStrategyEvaluation(
        status="no_signal",
        message=f"VWAP strategy found no closed-bar signal for {symbol}.",
        latest_close=latest_close,
        latest_bar_ended_at=latest_bar_ended_at.isoformat(),
        session_vwap=session_vwap,
        threshold_price=threshold_price if config.vwap_mode == "trend" else support_price,
    )


def _session_bars(bars: Sequence[PriceBar]) -> list[PriceBar]:
    latest = bars[-1]
    latest_day = latest.ends_at.date() if latest.ends_at is not None else latest.starts_at.date()
    return [
        bar for bar in bars
        if ((bar.ends_at.date() if bar.ends_at is not None else bar.starts_at.date()) == latest_day)
    ]


def _require_non_negative_float(value: object, *, field_name: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"VWAP strategy requires numeric {field_name}.") from exc
    if resolved < 0:
        raise ValueError(f"VWAP strategy requires {field_name} to be zero or greater.")
    return resolved
