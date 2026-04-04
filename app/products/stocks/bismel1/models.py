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

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PriceBar:
    starts_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    ends_at: datetime | None = None


@dataclass(frozen=True)
class StrategyInputSet:
    execution_bars: list[PriceBar]
    trend_bars: list[PriceBar]
    symbol: str
