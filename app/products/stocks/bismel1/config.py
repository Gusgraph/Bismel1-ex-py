# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/products/stocks/bismel1/config.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Bismel1StrategyConfig:
    product_key: str = "stocks.bismel1"
    execution_timeframe: str = "4H"
    trend_timeframe: str = "1D"
    pine_reference_filename: str = "Stocks-pine.pine"

