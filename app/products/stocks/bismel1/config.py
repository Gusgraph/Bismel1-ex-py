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
class BismillahTrobotStocksV1Config:
    """Static Pine input defaults for strict parity-mapping work."""

    product_key: str = "stocks.bismel1"
    pine_strategy_title: str = "Bismillah-Trobot Stocks v1"
    pine_reference_filename: str = "Stocks-pine.pine"

    strategy_overlay: bool = True
    strategy_pyramiding: int = 5
    strategy_initial_capital: float = 10000.0
    strategy_commission_type: str = "percent"
    strategy_commission_value: float = 0.1

    exec_tf_note: str = "Run Bismillah on 1m chart"
    trend_tf: str = "60"

    ema_fast_len: int = 50
    ema_slow_len: int = 200
    swing_len: int = 20
    pullback_min: float = 0.40
    rsi_len: int = 14
    atr_len: int = 14

    entry_mode: str = "Fast Reclaim"
    price_reclaim_bars: int = 2

    rsi_turn_up: float = 46.0
    add_spacing_atr: float = 2.4
    tp_pct: float = 1.0

    trail_profit_on: bool = True
    trail_start_pct: float = 0.20
    trail_dist_pct: float = 0.35

    pause_new_entries_manual: bool = False
    use_auto_pause: bool = True
    auto_pause_on_regime_fail: bool = True
    require_ema_slow_slope_up: bool = True
    ema_slow_slope_lookback: int = 10
    show_pause_labels: bool = False

    use_session_filter: bool = False
    trade_session: str = "0930-1600"
    trade_weekdays_only: bool = True

    max_adds: int = 4
    b1_pct_equity: float = 1.0
    q0: float = 1.0
    q1: float = 1.1
    q2: float = 1.5
    q3: float = 1.7
    q4: float = 1.9
    max_basket_pct_equity: float = 8.0

    show_emas_exec: bool = True
    show_htf_status: bool = False
    show_buy_add_labels: bool = True
    show_exit_labels: bool = True
    show_pullback_highlight: bool = False
    show_debug_label: bool = False
    show_mode_labels: bool = False
    show_top_right_dashboard: bool = True
    dashboard_text_size: str = "small"

    use_webhook_alerts: bool = True
    app_secret: str = "F7k3Qp9aL"
    ping_on_every_bar: bool = False
    tif_input: str = "day"


Bismel1StrategyConfig = BismillahTrobotStocksV1Config
