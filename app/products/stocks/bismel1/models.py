# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/products/stocks/bismel1/models.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class PriceBar:
    """Execution or HTF OHLCV bar aligned to Pine series indexing."""

    starts_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    ends_at: datetime | None = None


@dataclass(frozen=True)
class BismillahTrobotStocksV1Input:
    """Inputs required to evaluate Pine-equivalent components later."""

    execution_bars: list[PriceBar]
    htf_bars: list[PriceBar]
    symbol: str
    asset_type: str = "stock"
    exchange_timezone: str | None = None
    session_name: str | None = None


@dataclass
class BismillahTrobotStocksV1State:
    """Persistent Pine `var` state plus minimal runtime mirrors used by scaffolding."""

    add_count: int = 0
    last_add_price: float | None = None
    dollars_used: float = 0.0
    pos_high: float | None = None
    trail_stop: float | None = None
    dash_table_initialized: bool = False
    position_avg_price: float | None = None
    position_size: float = 0.0


@dataclass(frozen=True)
class PineComputedSeries:
    """Named Pine calculations documented in the parity map."""

    ema_fast_exec: list[float | None] = field(default_factory=list)
    ema_slow_exec: list[float | None] = field(default_factory=list)
    rsi_val: list[float | None] = field(default_factory=list)
    atr_val: list[float | None] = field(default_factory=list)
    atr_pct: list[float | None] = field(default_factory=list)
    swing_high: list[float | None] = field(default_factory=list)
    swing_low: list[float | None] = field(default_factory=list)
    pullback_depth: list[float | None] = field(default_factory=list)
    in_pullback_zone: list[bool] = field(default_factory=list)
    lowest_low_reclaim: list[float | None] = field(default_factory=list)
    rsi_cross_mode: list[bool] = field(default_factory=list)
    fast_reclaim_mode: list[bool] = field(default_factory=list)
    momentum_confirm: list[bool] = field(default_factory=list)
    htf_close: list[float | None] = field(default_factory=list)
    htf_ema_fast: list[float | None] = field(default_factory=list)
    htf_ema_slow: list[float | None] = field(default_factory=list)
    htf_ema_slow_prev: list[float | None] = field(default_factory=list)
    htf_ema_slow_slope_up: list[bool] = field(default_factory=list)
    trend_base_htf: list[bool] = field(default_factory=list)
    trend_ok: list[bool] = field(default_factory=list)
    regime_exit_confirmed: list[bool] = field(default_factory=list)
    in_session: list[bool] = field(default_factory=list)
    is_weekday: list[bool] = field(default_factory=list)
    session_ok: list[bool] = field(default_factory=list)
    regime_fail: list[bool] = field(default_factory=list)
    auto_paused: list[bool] = field(default_factory=list)
    pause_new_basket: list[bool] = field(default_factory=list)
    pause_adds: list[bool] = field(default_factory=list)
    is_low_tier: list[bool] = field(default_factory=list)


@dataclass(frozen=True)
class PineSignalSnapshot:
    """Current-bar entry, add, and exit booleans named after the Pine file."""

    base_entry_signal: bool
    base_entry_trigger: bool
    add_bounce_confirm: bool
    gate_atr_ok: bool
    gate_dp_ok: bool
    cap_ok: bool
    add_signal_raw: bool
    add_trigger: bool
    hit_atr_trail: bool
    hit_regime: bool


@dataclass(frozen=True)
class PineSignalStateBar:
    """Bar-by-bar Pine parity view for pause, signal, and minimal state transitions."""

    bar_index: int
    regime_fail: bool
    auto_paused: bool
    pause_new_basket: bool
    pause_adds: bool
    in_position_before: bool
    signal: PineSignalSnapshot
    state_before: BismillahTrobotStocksV1State
    state_after: BismillahTrobotStocksV1State


@dataclass(frozen=True)
class PineSignalStateEvaluation:
    """Sequential parity output for the signal/state phase."""

    series: PineComputedSeries
    bars: list[PineSignalStateBar] = field(default_factory=list)
    final_state: BismillahTrobotStocksV1State = field(default_factory=BismillahTrobotStocksV1State)


@dataclass(frozen=True)
class AiCacheRecord:
    scope: str
    symbol: str | None
    Ai_regime_label: str
    Ai_sentiment_label: str
    Ai_safety_label: str
    Ai_confidence: float
    Ai_reason: str
    Ai_updated_at: str | None
    Ai_source: str
    Ai_execution_allowed: bool
    Ai_block_new_entries: bool
    Ai_block_adds: bool
    Ai_blocked_reason: str | None
    is_stale: bool = False
    is_available: bool = True


@dataclass(frozen=True)
class PrimeStocksAiDecision:
    Ai_regime_label: str
    Ai_sentiment_label: str
    Ai_safety_label: str
    Ai_confidence: float
    Ai_reason: str
    Ai_updated_at: str | None
    Ai_source: str
    Ai_execution_allowed: bool
    Ai_block_new_entries: bool
    Ai_block_adds: bool
    Ai_blocked_reason: str | None
    market_record: AiCacheRecord | None = None
    symbol_record: AiCacheRecord | None = None
    is_stale: bool = False
    is_available: bool = True


@dataclass(frozen=True)
class PrimeStocksStrategyResult:
    """Consolidated strategy output."""

    product_key: str
    pine_strategy_title: str
    status: str
    message: str
    series: PineComputedSeries
    latest_signal: PineSignalSnapshot
    latest_bar: PineSignalStateBar | None
    final_state: BismillahTrobotStocksV1State
    ai_decision: PrimeStocksAiDecision | None = None
    execution_allowed: bool = False
    execution_timeframe: str = "1H"
    trend_timeframe: str = "1D"



StrategyInputSet = BismillahTrobotStocksV1Input
