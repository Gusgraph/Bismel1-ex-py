# Bismel1 Execution Backtest Summary

Initial report-only study using locally available 15M historical bars and production B1 Execution strategy evaluators.
No broker adapters, order submission, live runtime, Prime logic, or customer settings were changed.

## Data Limitations
- Requested symbols: 29.
- Symbols with local 15M bars: 7.
- Symbols marked data_unavailable: 22.
- The local dataset did not contain enough bars for a true 90/180/252 trading-day study across the full universe.
- This should be treated as an initial available-data study, not a production guarantee.

## Best Symbol/Strategy Candidates

| Symbol | Best Strategy | Fit Rating | Stop Loss | Trades | Win Rate | Profit Factor | P/L | Drawdown | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---|
| AAPL | Donchian Breakout Strategy | Strong Fit | none | 10 | 80.0 | 49.26 | 90.55 | -1.78 | Historically tested starting point; not a guarantee. |
| GOOGL | Donchian Breakout Strategy | Strong Fit | 2pct | 7 | 57.14 | 4.65 | 197.04 | -54.0 | Historically tested starting point; not a guarantee. |
| META | RSI Reversion Strategy | Strong Fit | none | 3 | 100.0 | 46.25 | 46.25 | 0.0 | Historically tested starting point; not a guarantee. |
| MSFT | RSI Reversion Strategy | Strong Fit | 3pct | 3 | 66.67 | 2.14 | 34.34 | -30.0 | Historically tested starting point; not a guarantee. |
| NVDA | Opening Range Breakout Strategy | Strong Fit | none | 18 | 61.11 | 9.94 | 273.33 | -17.39 | Historically tested starting point; not a guarantee. |
| SPY | EMA Strategy | Watch / Needs More Data | none | 0 | 0.0 | 0.0 | 0 | 0.0 | Needs more closed trades before relying on this pairing. |
| TSLA | RSI Reversion Strategy | Strong Fit | 2pct | 3 | 66.67 | 3.66 | 53.16 | -20.0 | Historically tested starting point; not a guarantee. |

## Strategy Summary

| Strategy | Best Symbol Types | Weak Symbol Types | Notes |
|---|---|---|---|
| ADX Trend Strategy | Large-cap stocks | Large-cap stocks | Fits stronger directional movement; can underperform in chop. |
| Bollinger Reversion Strategy | Large-cap stocks | Large-cap stocks | Best treated as tactical; monitor drawdown and signal frequency. |
| Breakout Strategy | Large-cap stocks | Large-cap stocks | Fits stronger directional movement; can underperform in chop. |
| Donchian Breakout Strategy | Large-cap stocks | Large-cap stocks | Fits stronger directional movement; can underperform in chop. |
| EMA Strategy | None in available sample | None in available sample | Use as a baseline trend or structure strategy. |
| Momentum Strategy | Large-cap stocks | None in available sample | Fits stronger directional movement; can underperform in chop. |
| Opening Range Breakout Strategy | Large-cap stocks | Large-cap stocks | Fits stronger directional movement; can underperform in chop. |
| Pullback Strategy | None in available sample | None in available sample | Use as a baseline trend or structure strategy. |
| RSI Reversion Strategy | Large-cap stocks | None in available sample | Best treated as tactical; monitor drawdown and signal frequency. |
| Relative Strength Strategy | None in available sample | Large-cap stocks | Needs reliable benchmark bars; useful when symbols diverge from broad market. |
| VWAP Strategy | Large-cap stocks | Large-cap stocks | Intraday reference strategy; depends heavily on session behavior. |

## Stop-Loss Impact

| Group | No SL Avg P/L | 2% SL Avg P/L | 3% SL Avg P/L | 5% SL Avg P/L | Recommendation |
|---|---:|---:|---:|---:|---|
| Broad ETFs | -0.15 | -0.15 | -0.15 | -0.15 | Keep stop loss optional; no default SL justified. |
| Sector ETFs | 0.00 | 0.00 | 0.00 | 0.00 | Keep stop loss optional; no default SL justified. |
| Commodity / International ETFs | 0.00 | 0.00 | 0.00 | 0.00 | Keep stop loss optional; no default SL justified. |
| Large-cap stocks | 30.49 | 35.72 | 35.27 | 31.08 | 2pct improved this sample, but keep optional. |

## Symbols To Watch Or Avoid

- Watch / Needs More Data: any symbol without local 15M bars in this run, plus candidates with fewer than 3 trades.
- Not Recommended: combinations with negative P/L, weak profit factor, or large drawdown in the available sample.
- Stop loss should remain optional user risk preference. This run does not justify making it mandatory by default.

## Unavailable Symbols

AMD, AMZN, DIA, EFA, FXI, GLD, HYG, IVV, IWM, QQQ, SLV, SMH, VGT, VOO, XLE, XLF, XLI, XLK, XLP, XLU, XLV, XLY
