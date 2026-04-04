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
    """Persistent `var` state declared by the Pine strategy."""

    in_basket: bool = False
    add_count: int = 0
    entry_bar_index: int | None = None
    last_entry_price: float | None = None

    basket_qty_units: float = 0.0
    basket_cost: float = 0.0
    basket_avg: float | None = None
    basket_dollars_used: float = 0.0
    basket_b1_dollars: float | None = None

    tp_price: float | None = None
    profit_trail_armed: bool = False
    profit_trail_active: bool = False
    profit_trail_high: float | None = None
    profit_trail_stop: float | None = None
    profit_trail_arm_price: float | None = None
    last_exit_reason: str = ""


@dataclass(frozen=True)
class PineComputedSeries:
    """Named Pine calculations documented in the parity map."""

    ema_fast_exec: list[float | None] = field(default_factory=list)
    ema_slow_exec: list[float | None] = field(default_factory=list)
    rsi_val: list[float | None] = field(default_factory=list)
    atr_val: list[float | None] = field(default_factory=list)
    swing_high: list[float | None] = field(default_factory=list)
    swing_low: list[float | None] = field(default_factory=list)
    pullback_depth: list[float | None] = field(default_factory=list)
    in_pullback_zone: list[bool] = field(default_factory=list)
    htf_close: list[float | None] = field(default_factory=list)
    htf_ema_fast: list[float | None] = field(default_factory=list)
    htf_ema_slow: list[float | None] = field(default_factory=list)
    htf_ema_slow_prev: list[float | None] = field(default_factory=list)
    htf_ema_slow_slope_up: list[bool] = field(default_factory=list)
    trend_base_htf: list[bool] = field(default_factory=list)
    trend_ok: list[bool] = field(default_factory=list)
    session_ok: list[bool] = field(default_factory=list)
    entries_paused: list[bool] = field(default_factory=list)


@dataclass(frozen=True)
class PineSignalSnapshot:
    """Entry, add, and exit booleans named after Pine sections J-M."""

    base_entry_signal: bool
    can_add_more: bool
    spacing_ok: bool
    add_signal_raw: bool
    add_within_cap: bool
    add_signal: bool
    hit_plain_tp: bool
    hit_profit_trail: bool


StrategyInputSet = BismillahTrobotStocksV1Input
