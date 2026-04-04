# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/products/stocks/bismel1/strategy.py
# ======================================================

from __future__ import annotations

from app.products.stocks.bismel1.config import (
    Bismel1StrategyConfig,
    BismillahTrobotStocksV1Config,
)
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    StrategyInputSet,
)


EXIT_TP = "Bismillah-TP"
EXIT_TP_TRAIL = "Bismillah-TRAIL-PROFIT"


def compute_pine_series(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
) -> PineComputedSeries:
    """Reserved for Pine section F/J series parity.

    This phase only locks the interface to the exact Pine components. It does not
    claim `request.security`, session, or bar-confirmation parity yet.
    """

    _ = strategy_input
    _ = config
    return PineComputedSeries()


def snapshot_signals(
    strategy_input: BismillahTrobotStocksV1Input,
    config: BismillahTrobotStocksV1Config,
    state: BismillahTrobotStocksV1State,
    series: PineComputedSeries,
) -> PineSignalSnapshot:
    """Reserved for Pine sections J, L, and M signal booleans."""

    _ = strategy_input
    _ = config
    _ = state
    _ = series
    return PineSignalSnapshot(
        base_entry_signal=False,
        can_add_more=False,
        spacing_ok=False,
        add_signal_raw=False,
        add_within_cap=False,
        add_signal=False,
        hit_plain_tp=False,
        hit_profit_trail=False,
    )


def evaluate_strategy(
    strategy_input: StrategyInputSet,
    config: Bismel1StrategyConfig | None = None,
) -> dict[str, object]:
    resolved_config = config or Bismel1StrategyConfig()
    series = compute_pine_series(strategy_input, resolved_config)
    signals = snapshot_signals(
        strategy_input=strategy_input,
        config=resolved_config,
        state=BismillahTrobotStocksV1State(),
        series=series,
    )
    return {
        "product_key": resolved_config.product_key,
        "pine_strategy_title": resolved_config.pine_strategy_title,
        "status": "parity_scaffolding_only",
        "message": "Pine components are mapped and documented, but execution parity is not implemented.",
        "series_type": type(series).__name__,
        "signal_type": type(signals).__name__,
    }
