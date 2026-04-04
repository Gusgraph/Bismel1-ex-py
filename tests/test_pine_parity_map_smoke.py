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


def test_parity_map_documents_fixture_proof_scope() -> None:
    parity_map = Path("app/products/stocks/bismel1/parity_map.md").read_text(encoding="utf-8")

    assert "## HTF fixture proof status" in parity_map
    assert "### Proven by deterministic fixture tests" in parity_map
    assert "### Still assumed, not yet proven against live TradingView output" in parity_map


def test_parity_map_documents_missing_real_series_status() -> None:
    parity_map = Path("app/products/stocks/bismel1/parity_map.md").read_text(encoding="utf-8")

    assert "## Real exported sample status" in parity_map
    assert "this repo does not contain a TradingView-exported HTF sample file" in parity_map


def test_pine_readme_documents_tradingview_export_contract() -> None:
    pine_readme = Path("reference/pine/README.md").read_text(encoding="utf-8")

    assert "## TradingView HTF export contract" in pine_readme
    assert "reference/pine/Stocks-pine-tv-export-sample.csv" in pine_readme
    assert "htf_close_tv" in pine_readme
    assert "htf_ema_fast_tv" in pine_readme
    assert "htf_ema_slow_tv" in pine_readme
    assert "htf_ema_slow_prev_tv" in pine_readme


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
