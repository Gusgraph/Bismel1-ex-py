# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_pine_parity_map_smoke.py
# ======================================================

from __future__ import annotations

from pathlib import Path

from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
)
from app.products.stocks.bismel1.strategy import evaluate_strategy


def test_parity_map_exists() -> None:
    parity_map = Path("app/products/stocks/bismel1/parity_map.md")
    assert parity_map.exists()


def test_pine_aligned_structures_import() -> None:
    config = BismillahTrobotStocksV1Config()
    state = BismillahTrobotStocksV1State()
    series = PineComputedSeries()
    snapshot = PineSignalSnapshot(False, False, False, False, False, False, False, False)

    assert config.pine_strategy_title == "Bismillah-Trobot Stocks v1"
    assert config.trend_tf == "60"
    assert state.in_basket is False
    assert series.htf_close == []
    assert series.entries_paused == []
    assert snapshot.base_entry_signal is False


def test_strategy_reports_scaffolding_only() -> None:
    strategy_input = BismillahTrobotStocksV1Input(execution_bars=[], htf_bars=[], symbol="AAPL")

    result = evaluate_strategy(strategy_input)

    assert result["status"] == "parity_scaffolding_only"
