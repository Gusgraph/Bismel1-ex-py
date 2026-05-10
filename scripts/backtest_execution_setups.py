# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: x
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/backtest_execution_setups.py
# ======================================================

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Callable, Iterable

from app.products.stocks.bismel1.models import PriceBar
from app.runtime.execution.strategies import (
    AdxTrendStrategyConfig,
    BollingerReversionStrategyConfig,
    BreakoutStrategyConfig,
    DonchianBreakoutStrategyConfig,
    EmaStrategyConfig,
    MomentumStrategyConfig,
    OpeningRangeBreakoutStrategyConfig,
    PullbackStrategyConfig,
    RelativeStrengthStrategyConfig,
    RsiReversionStrategyConfig,
    VwapStrategyConfig,
    evaluate_adx_trend_strategy,
    evaluate_bollinger_reversion_strategy,
    evaluate_breakout_strategy,
    evaluate_donchian_breakout_strategy,
    evaluate_ema_strategy,
    evaluate_momentum_strategy,
    evaluate_opening_range_breakout_strategy,
    evaluate_pullback_strategy,
    evaluate_relative_strength_strategy,
    evaluate_rsi_reversion_strategy,
    evaluate_vwap_strategy,
)


SYMBOL_GROUPS: dict[str, list[str]] = {
    "Broad ETFs": ["SPY", "QQQ", "DIA", "IWM", "VOO", "IVV"],
    "Sector ETFs": ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "SMH", "VGT"],
    "Commodity / International ETFs": ["GLD", "SLV", "EFA", "FXI", "HYG"],
    "Large-cap stocks": ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "TSLA", "AMD", "AMZN"],
}
SYMBOLS = [symbol for symbols in SYMBOL_GROUPS.values() for symbol in symbols]
STOP_LOSS_VARIANTS: dict[str, float | None] = {
    "none": None,
    "2pct": 2.0,
    "3pct": 3.0,
    "5pct": 5.0,
}
SLIPPAGE_PER_FILL_PCT = 0.05
STARTING_CAPITAL = 10_000.0
TRADE_NOTIONAL = 1_000.0


@dataclass(frozen=True)
class StrategySpec:
    key: str
    label: str
    make_config: Callable[[], object]
    evaluate: Callable[..., object]
    needs_benchmark: bool = False


@dataclass
class Trade:
    entry_at: datetime
    exit_at: datetime
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    hold_bars: int
    exit_reason: str


@dataclass
class BacktestResult:
    symbol: str
    group: str
    strategy: str
    stop_loss: str
    timeframe: str
    start_at: datetime | None
    end_at: datetime | None
    bars: int
    trades: list[Trade]
    no_signal_periods: int
    data_issues: int
    max_drawdown: float

    @property
    def total_pnl(self) -> float:
        return sum(trade.pnl for trade in self.trades)

    @property
    def average_trade_pnl(self) -> float:
        return mean([trade.pnl for trade in self.trades]) if self.trades else 0.0

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        return (len([trade for trade in self.trades if trade.pnl > 0]) / len(self.trades)) * 100.0

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(trade.pnl for trade in self.trades if trade.pnl > 0)
        gross_loss = abs(sum(trade.pnl for trade in self.trades if trade.pnl < 0))
        if gross_loss == 0:
            return gross_profit if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    @property
    def average_hold_bars(self) -> float:
        return mean([trade.hold_bars for trade in self.trades]) if self.trades else 0.0

    @property
    def best_trade(self) -> float:
        return max([trade.pnl for trade in self.trades], default=0.0)

    @property
    def worst_trade(self) -> float:
        return min([trade.pnl for trade in self.trades], default=0.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline B1 Execution strategy backtest report generator.")
    parser.add_argument("--bars-csv", required=True, help="CSV with symbol,timeframe,starts_at,ends_at,open,high,low,close,volume")
    parser.add_argument("--output-dir", default="reports", help="Directory for generated reports.")
    args = parser.parse_args()

    bars_by_symbol = load_bars(Path(args.bars_csv))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    strategies = strategy_specs()
    results: list[BacktestResult] = []
    unavailable = sorted(set(SYMBOLS) - set(bars_by_symbol))
    benchmark = bars_by_symbol.get("SPY", [])

    for symbol in SYMBOLS:
        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < 25:
            continue
        group = group_for_symbol(symbol)
        for strategy in strategies:
            for stop_key, stop_pct in STOP_LOSS_VARIANTS.items():
                results.append(
                    run_backtest(
                        symbol=symbol,
                        group=group,
                        strategy=strategy,
                        bars=bars,
                        benchmark_bars=benchmark,
                        stop_loss_pct=stop_pct,
                        stop_loss_key=stop_key,
                    )
                )

    write_matrix(output_dir / "execution_symbol_strategy_matrix.csv", results, unavailable)
    write_summary(output_dir / "execution_backtest_summary.md", results, unavailable)
    write_recommended_setups(output_dir / "execution_recommended_setups.md", results, unavailable)

    print(f"Generated reports in {output_dir}")
    print(f"Backtested symbols: {len(set(result.symbol for result in results))}")
    print(f"Data unavailable symbols: {len(unavailable)}")


def strategy_specs() -> list[StrategySpec]:
    return [
        StrategySpec("ema", "EMA Strategy", lambda: EmaStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_ema_strategy),
        StrategySpec("pullback", "Pullback Strategy", lambda: PullbackStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_pullback_strategy),
        StrategySpec("breakout", "Breakout Strategy", lambda: BreakoutStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_breakout_strategy),
        StrategySpec("rsi_reversion", "RSI Reversion Strategy", lambda: RsiReversionStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_rsi_reversion_strategy),
        StrategySpec("momentum", "Momentum Strategy", lambda: MomentumStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_momentum_strategy),
        StrategySpec("vwap", "VWAP Strategy", lambda: VwapStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_vwap_strategy),
        StrategySpec("bollinger_reversion", "Bollinger Reversion Strategy", lambda: BollingerReversionStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_bollinger_reversion_strategy),
        StrategySpec("adx_trend", "ADX Trend Strategy", lambda: AdxTrendStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_adx_trend_strategy),
        StrategySpec("donchian_breakout", "Donchian Breakout Strategy", lambda: DonchianBreakoutStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_donchian_breakout_strategy),
        StrategySpec("relative_strength", "Relative Strength Strategy", lambda: RelativeStrengthStrategyConfig.from_payload({"direction_filter": "long_only", "benchmark_symbol": "SPY"}), evaluate_relative_strength_strategy, True),
        StrategySpec("opening_range_breakout", "Opening Range Breakout Strategy", lambda: OpeningRangeBreakoutStrategyConfig.from_payload({"direction_filter": "long_only"}), evaluate_opening_range_breakout_strategy),
    ]


def load_bars(path: Path) -> dict[str, list[PriceBar]]:
    bars_by_symbol: dict[str, list[PriceBar]] = defaultdict(list)
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            timeframe = str(row.get("timeframe", "")).lower()
            if timeframe != "15m":
                continue
            symbol = str(row["symbol"]).upper()
            bars_by_symbol[symbol].append(
                PriceBar(
                    starts_at=parse_datetime(row["starts_at"]),
                    ends_at=parse_datetime(row["ends_at"]) if row.get("ends_at") else None,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]) if row.get("volume") not in {None, ""} else None,
                )
            )
    return {symbol: sorted(bars, key=lambda bar: bar.starts_at) for symbol, bars in bars_by_symbol.items()}


def run_backtest(
    *,
    symbol: str,
    group: str,
    strategy: StrategySpec,
    bars: list[PriceBar],
    benchmark_bars: list[PriceBar],
    stop_loss_pct: float | None,
    stop_loss_key: str,
) -> BacktestResult:
    config = strategy.make_config()
    required = int(getattr(config, "required_bar_count", 25))
    trades: list[Trade] = []
    no_signal_periods = 0
    data_issues = 0
    position_qty = 0.0
    entry_price = 0.0
    entry_at: datetime | None = None
    entry_index = 0
    equity = STARTING_CAPITAL
    peak = STARTING_CAPITAL
    max_drawdown = 0.0

    for index in range(required, len(bars) - 1):
        current_window = bars[: index + 1]
        next_bar = bars[index + 1]

        if position_qty > 0 and stop_loss_pct is not None:
            stop_price = entry_price * (1.0 - (stop_loss_pct / 100.0))
            if next_bar.low <= stop_price:
                trade = close_trade(
                    entry_at=entry_at or bars[index].starts_at,
                    exit_at=next_bar.starts_at,
                    entry_price=entry_price,
                    exit_price=stop_price,
                    quantity=position_qty,
                    entry_index=entry_index,
                    exit_index=index + 1,
                    reason=f"stop_loss_{stop_loss_key}",
                )
                trades.append(trade)
                equity += trade.pnl
                peak = max(peak, equity)
                max_drawdown = min(max_drawdown, equity - peak)
                position_qty = 0.0
                entry_price = 0.0
                entry_at = None
                continue

        try:
            if strategy.needs_benchmark:
                evaluation = strategy.evaluate(
                    symbol=symbol,
                    bars=current_window,
                    benchmark_bars=aligned_benchmark(benchmark_bars, current_window),
                    config=config,
                )
            else:
                evaluation = strategy.evaluate(symbol=symbol, bars=current_window, config=config)
        except Exception:
            data_issues += 1
            continue

        action = str(getattr(evaluation, "action", "") or "").lower()
        status = str(getattr(evaluation, "status", "") or "")
        if status.startswith("skipped_market_data"):
            data_issues += 1
        if action == "":
            no_signal_periods += 1
            continue

        if action == "buy" and position_qty <= 0:
            entry_price = apply_entry_slippage(float(next_bar.open))
            position_qty = TRADE_NOTIONAL / entry_price
            entry_at = next_bar.starts_at
            entry_index = index + 1
        elif action in {"sell", "close"} and position_qty > 0:
            trade = close_trade(
                entry_at=entry_at or bars[index].starts_at,
                exit_at=next_bar.starts_at,
                entry_price=entry_price,
                exit_price=apply_exit_slippage(float(next_bar.open)),
                quantity=position_qty,
                entry_index=entry_index,
                exit_index=index + 1,
                reason="strategy_close",
            )
            trades.append(trade)
            equity += trade.pnl
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity - peak)
            position_qty = 0.0
            entry_price = 0.0
            entry_at = None

    if position_qty > 0:
        last_bar = bars[-1]
        trade = close_trade(
            entry_at=entry_at or last_bar.starts_at,
            exit_at=last_bar.starts_at,
            entry_price=entry_price,
            exit_price=apply_exit_slippage(float(last_bar.close)),
            quantity=position_qty,
            entry_index=entry_index,
            exit_index=len(bars) - 1,
            reason="end_of_data",
        )
        trades.append(trade)
        equity += trade.pnl
        max_drawdown = min(max_drawdown, equity - peak)

    return BacktestResult(
        symbol=symbol,
        group=group,
        strategy=strategy.label,
        stop_loss=stop_loss_key,
        timeframe="15m",
        start_at=bars[0].starts_at if bars else None,
        end_at=bars[-1].starts_at if bars else None,
        bars=len(bars),
        trades=trades,
        no_signal_periods=no_signal_periods,
        data_issues=data_issues,
        max_drawdown=max_drawdown,
    )


def close_trade(
    *,
    entry_at: datetime,
    exit_at: datetime,
    entry_price: float,
    exit_price: float,
    quantity: float,
    entry_index: int,
    exit_index: int,
    reason: str,
) -> Trade:
    pnl = (exit_price - entry_price) * quantity
    pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0
    return Trade(entry_at, exit_at, entry_price, exit_price, pnl, pnl_pct, max(1, exit_index - entry_index), reason)


def write_matrix(path: Path, results: list[BacktestResult], unavailable: list[str]) -> None:
    fields = [
        "symbol", "group", "strategy", "stop_loss", "fit_rating", "bars", "start_at", "end_at",
        "trades", "win_rate", "profit_factor", "total_pnl", "average_trade_pnl", "max_drawdown",
        "average_hold_bars", "best_trade", "worst_trade", "no_signal_periods", "data_issues", "notes",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in sorted(results, key=lambda item: (item.symbol, fit_rank(fit_rating(item)), -item.profit_factor)):
            writer.writerow(result_row(result))
        for symbol in unavailable:
            writer.writerow({
                "symbol": symbol,
                "group": group_for_symbol(symbol),
                "strategy": "data_unavailable",
                "stop_loss": "",
                "fit_rating": "Watch / Needs More Data",
                "notes": "No local 15M historical bars available in this initial study.",
            })


def write_summary(path: Path, results: list[BacktestResult], unavailable: list[str]) -> None:
    best_by_symbol = select_best_by_symbol(results)
    lines = [
        "# Bismel1 Execution Backtest Summary",
        "",
        "Initial report-only study using locally available 15M historical bars and production B1 Execution strategy evaluators.",
        "No broker adapters, order submission, live runtime, Prime logic, or customer settings were changed.",
        "",
        "## Data Limitations",
        f"- Requested symbols: {len(SYMBOLS)}.",
        f"- Symbols with local 15M bars: {len(best_by_symbol)}.",
        f"- Symbols marked data_unavailable: {len(unavailable)}.",
        "- The local dataset did not contain enough bars for a true 90/180/252 trading-day study across the full universe.",
        "- This should be treated as an initial available-data study, not a production guarantee.",
        "",
        "## Best Symbol/Strategy Candidates",
        "",
        "| Symbol | Best Strategy | Fit Rating | Stop Loss | Trades | Win Rate | Profit Factor | P/L | Drawdown | Notes |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for result in best_by_symbol:
        row = result_row(result)
        lines.append(
            f"| {row['symbol']} | {row['strategy']} | {row['fit_rating']} | {row['stop_loss']} | "
            f"{row['trades']} | {row['win_rate']} | {row['profit_factor']} | {row['total_pnl']} | {row['max_drawdown']} | {row['notes']} |"
        )
    lines.extend([
        "",
        "## Strategy Summary",
        "",
        "| Strategy | Best Symbol Types | Weak Symbol Types | Notes |",
        "|---|---|---|---|",
    ])
    for strategy in sorted({result.strategy for result in results}):
        strategy_results = [result for result in results if result.strategy == strategy and result.stop_loss == "none"]
        strong = [result for result in strategy_results if fit_rating(result) == "Strong Fit"]
        weak = [result for result in strategy_results if fit_rating(result) == "Not Recommended"]
        lines.append(
            f"| {strategy} | {summarize_groups(strong)} | {summarize_groups(weak)} | "
            f"{strategy_note(strategy)} |"
        )
    lines.extend([
        "",
        "## Stop-Loss Impact",
        "",
        "| Group | No SL Avg P/L | 2% SL Avg P/L | 3% SL Avg P/L | 5% SL Avg P/L | Recommendation |",
        "|---|---:|---:|---:|---:|---|",
    ])
    for group in SYMBOL_GROUPS:
        lines.append(stop_loss_group_line(group, results))
    lines.extend([
        "",
        "## Symbols To Watch Or Avoid",
        "",
        "- Watch / Needs More Data: any symbol without local 15M bars in this run, plus candidates with fewer than 3 trades.",
        "- Not Recommended: combinations with negative P/L, weak profit factor, or large drawdown in the available sample.",
        "- Stop loss should remain optional user risk preference. This run does not justify making it mandatory by default.",
        "",
        "## Unavailable Symbols",
        "",
        ", ".join(unavailable) if unavailable else "None.",
    ])
    path.write_text("\n".join(lines) + "\n")


def write_recommended_setups(path: Path, results: list[BacktestResult], unavailable: list[str]) -> None:
    best = {result.symbol: result for result in select_best_by_symbol(results)}
    setups = [
        ("Conservative ETF Execution", ["SPY", "QQQ", "VOO", "IVV", "XLK", "XLF", "XLI", "VGT", "SMH"], "Broad, liquid ETF symbols"),
        ("Sector Rotation Execution", ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "SMH"], "Sector ETF exposure"),
        ("Large-Cap Momentum Execution", ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "AMD", "TSLA"], "Liquid large-cap stocks"),
        ("Mixed ETF + Stock Starter", ["SPY", "QQQ", "XLK", "SMH", "AAPL", "MSFT", "NVDA", "GOOGL"], "Balanced beginner watchlist"),
    ]
    lines = [
        "# Bismel1 Execution Recommended Starter Setups",
        "",
        "These are historically tested starting points from available local data. They are not profit guarantees, investment advice, or risk-free configurations.",
        "Users remain responsible for product settings, broker account risk, and ongoing monitoring.",
        "",
    ]
    for name, symbols, goal in setups:
        lines.extend([f"## {name}", "", f"Goal: {goal}.", "", "| Symbol | Suggested Strategy | Fit | Stop Loss Suggestion | Risk Note |", "|---|---|---|---|---|"])
        for symbol in symbols:
            result = best.get(symbol)
            if result is None:
                lines.append(f"| {symbol} | Needs more data | Watch / Needs More Data | Optional only | Local 15M bars were unavailable in this run. |")
                continue
            rating = fit_rating(result)
            stop_note = stop_loss_note(symbol, results)
            lines.append(f"| {symbol} | {result.strategy} | {rating} | {stop_note} | {risk_note(result)} |")
        lines.append("")
    lines.extend([
        "## User-Facing Wording",
        "",
        "- Use: historically tested, may fit, suggested starting point, needs monitoring, not a guarantee.",
        "- Avoid: guaranteed profit, safe profit, no loss, sure win rate, beats the market.",
    ])
    path.write_text("\n".join(lines) + "\n")


def result_row(result: BacktestResult) -> dict[str, object]:
    return {
        "symbol": result.symbol,
        "group": result.group,
        "strategy": result.strategy,
        "stop_loss": result.stop_loss,
        "fit_rating": fit_rating(result),
        "bars": result.bars,
        "start_at": result.start_at.isoformat() if result.start_at else "",
        "end_at": result.end_at.isoformat() if result.end_at else "",
        "trades": len(result.trades),
        "win_rate": round(result.win_rate, 2),
        "profit_factor": round(result.profit_factor, 2),
        "total_pnl": round(result.total_pnl, 2),
        "average_trade_pnl": round(result.average_trade_pnl, 2),
        "max_drawdown": round(result.max_drawdown, 2),
        "average_hold_bars": round(result.average_hold_bars, 2),
        "best_trade": round(result.best_trade, 2),
        "worst_trade": round(result.worst_trade, 2),
        "no_signal_periods": result.no_signal_periods,
        "data_issues": result.data_issues,
        "notes": risk_note(result),
    }


def select_best_by_symbol(results: list[BacktestResult]) -> list[BacktestResult]:
    selected = []
    for symbol in sorted({result.symbol for result in results}):
        candidates = [result for result in results if result.symbol == symbol]
        candidates.sort(key=lambda item: (fit_rank(fit_rating(item)), -item.total_pnl, item.max_drawdown))
        selected.append(candidates[0])
    return selected


def fit_rating(result: BacktestResult) -> str:
    trades = len(result.trades)
    if trades < 3:
        return "Watch / Needs More Data"
    if result.total_pnl > 0 and result.profit_factor >= 1.5 and result.win_rate >= 45 and result.max_drawdown > -120:
        return "Strong Fit"
    if result.total_pnl > 0 and result.profit_factor >= 1.05 and result.max_drawdown > -180:
        return "Acceptable Fit"
    return "Not Recommended"


def fit_rank(rating: str) -> int:
    return {"Strong Fit": 0, "Acceptable Fit": 1, "Watch / Needs More Data": 2, "Not Recommended": 3}.get(rating, 9)


def stop_loss_group_line(group: str, results: list[BacktestResult]) -> str:
    group_results = [result for result in results if result.group == group]
    values = {key: average_pnl([result for result in group_results if result.stop_loss == key]) for key in STOP_LOSS_VARIANTS}
    best_key = max(values, key=lambda key: values[key])
    recommendation = "Keep stop loss optional; no default SL justified." if best_key == "none" else f"{best_key} improved this sample, but keep optional."
    return f"| {group} | {values['none']:.2f} | {values['2pct']:.2f} | {values['3pct']:.2f} | {values['5pct']:.2f} | {recommendation} |"


def stop_loss_note(symbol: str, results: list[BacktestResult]) -> str:
    symbol_results = [result for result in results if result.symbol == symbol]
    if not symbol_results:
        return "Optional only"
    values = {key: average_pnl([result for result in symbol_results if result.stop_loss == key]) for key in STOP_LOSS_VARIANTS}
    best_key = max(values, key=lambda key: values[key])
    return "No SL by default; optional 2-3% user risk preference" if best_key == "none" else f"Optional {best_key}; validate before enabling"


def average_pnl(results: list[BacktestResult]) -> float:
    return mean([result.total_pnl for result in results]) if results else 0.0


def summarize_groups(results: Iterable[BacktestResult]) -> str:
    groups = sorted({result.group for result in results})
    return ", ".join(groups) if groups else "None in available sample"


def strategy_note(strategy: str) -> str:
    if "Reversion" in strategy:
        return "Best treated as tactical; monitor drawdown and signal frequency."
    if "Breakout" in strategy or "Momentum" in strategy or "ADX" in strategy:
        return "Fits stronger directional movement; can underperform in chop."
    if "VWAP" in strategy:
        return "Intraday reference strategy; depends heavily on session behavior."
    if "Relative" in strategy:
        return "Needs reliable benchmark bars; useful when symbols diverge from broad market."
    return "Use as a baseline trend or structure strategy."


def risk_note(result: BacktestResult) -> str:
    if len(result.trades) < 3:
        return "Needs more closed trades before relying on this pairing."
    if result.max_drawdown <= -180:
        return "Drawdown was high in the available sample."
    if result.profit_factor < 1.05:
        return "Weak profit factor in the available sample."
    return "Historically tested starting point; not a guarantee."


def group_for_symbol(symbol: str) -> str:
    for group, symbols in SYMBOL_GROUPS.items():
        if symbol in symbols:
            return group
    return "Other"


def aligned_benchmark(benchmark_bars: list[PriceBar], current_window: list[PriceBar]) -> list[PriceBar]:
    if not current_window:
        return []
    cutoff = current_window[-1].starts_at
    return [bar for bar in benchmark_bars if bar.starts_at <= cutoff]


def apply_entry_slippage(price: float) -> float:
    return price * (1.0 + (SLIPPAGE_PER_FILL_PCT / 100.0))


def apply_exit_slippage(price: float) -> float:
    return price * (1.0 - (SLIPPAGE_PER_FILL_PCT / 100.0))


def parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


if __name__ == "__main__":
    main()
