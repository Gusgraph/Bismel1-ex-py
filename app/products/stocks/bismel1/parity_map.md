<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: app/products/stocks/bismel1/parity_map.md
======================================================
-->

# Bismillah-Trobot Stocks v1 parity map

This file documents the Pine source of truth in `reference/pine/Stocks-pine.pine`.

## Scope and declared strategy settings

- Pine version: `//@version=6`
- Strategy title: `Bismillah-Trobot Stocks v1`
- `overlay=true`
- `pyramiding=5`
- `initial_capital=10000`
- `commission_type=strategy.commission.percent`
- `commission_value=0.1`
- Script comments declare:
  - stocks only
  - 1m execution / 1h trend
  - long only
  - no time exit
  - no Ghuphran exit
  - no loss trail exit
  - TP by price %
  - optional profit-trail only after TP extension
  - webhook via `alert()`

## Pine inputs and defaults

### A) Core inputs

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `execTfNote` | `Execution Timeframe Note` | `input.string` | `Run Bismillah on 1m chart` | Note-only input; not used in logic. |
| `trendTf` | `Trend Timeframe (HTF)` | `input.timeframe` | `60` | HTF for `request.security`. |
| `emaFastLen` | `EMA Fast` | `input.int` | `50` | `minval=1`. |
| `emaSlowLen` | `EMA Slow` | `input.int` | `200` | `minval=1`. |
| `swingLen` | `Swing Length (Execution TF)` | `input.int` | `20` | `minval=2`. |
| `pullbackMin` | `Min Pullback Depth (0.40 = 40%)` | `input.float` | `0.40` | `minval=0.05`, `maxval=0.95`, `step=0.05`. |
| `rsiLen` | `RSI Length (Execution TF)` | `input.int` | `14` | `minval=2`. |
| `atrLen` | `ATR Length (Execution TF)` | `input.int` | `14` | `minval=1`. |
| `entryMode` | `Entry Repeat Behavior` | `input.string` | `Fast Reclaim` | Options: `Fast Reclaim`, `RSI Crossover`. |
| `priceReclaimBars` | `1m Reclaim Confirm Bars` | `input.int` | `2` | `minval=1`, `maxval=10`. |

### B) Stocks tuning

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `rsiTurnUp` | `Stocks RSI Turn-Up` | `input.float` | `46` | `minval=1`, `maxval=99`. |
| `addSpacingATR` | `Stocks Add Spacing ATR` | `input.float` | `2.4` | `minval=0.1`, `step=0.1`. |
| `tpPct` | `Stocks Take Profit %` | `input.float` | `1.0` | `minval=0.05`, `step=0.05`. |
| `trailProfitOn` | `Stocks Trail Profit After TP` | `input.bool` | `true` | Enables profit-only trail. |
| `trailStartPct` | `Stocks Trail Profit Start Extra %` | `input.float` | `0.20` | `minval=0.0`, `step=0.05`. |
| `trailDistPct` | `Stocks Trail Profit Distance %` | `input.float` | `0.35` | `minval=0.05`, `step=0.05`. |

### C) Regime / pause

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `pauseNewEntriesManual` | `Pause New Entries (Manual)` | `input.bool` | `false` | Manual gate only affects new entries/adds. |
| `useAutoPause` | `Use Auto Pause by Regime` | `input.bool` | `true` | |
| `autoPauseOnRegimeFail` | `Kafa-Allah Trend Regime Fails` | `input.bool` | `true` | |
| `requireEmaSlowSlopeUp` | `Require HTF EMA200 AlRafi-a` | `input.bool` | `true` | |
| `emaSlowSlopeLookback` | `HTF EMA200 Slope YaAllah Bars` | `input.int` | `10` | `minval=1`. |
| `showPauseLabels` | `Alhamdulilah / REGIME Labels` | `input.bool` | `false` | Visual only. |
| `useSessionFilter` | `Use Market Session Filter` | `input.bool` | `false` | |
| `tradeSession` | `Stocks Session (Exchange Time)` | `input.session` | `0930-1600` | Uses exchange time via Pine `time()`. |
| `tradeWeekdaysOnly` | `Weekdays Only` | `input.bool` | `true` | Only applied if session filter enabled. |

### D) Position ladder sizing

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `maxAdds` | `Allah Al-Muatee` | `input.int` | `4` | `minval=0`, `maxval=10`. |
| `b1PctEquity` | `Stocks B1 % Equity` | `input.float` | `1.0` | `minval=0.001`, `step=0.01`. |
| `q0` | `Allah Recovers B1` | `input.float` | `1.0` | Step multiplier. |
| `q1` | `Allah Recovers A1` | `input.float` | `1.1` | Step multiplier. |
| `q2` | `Allah Recovers A2` | `input.float` | `1.5` | Step multiplier. |
| `q3` | `Allah Recovers A3` | `input.float` | `1.7` | Step multiplier. |
| `q4` | `Allah Recovers A4` | `input.float` | `1.9` | Step multiplier. |
| `maxBasketPctEquity` | `Stocks Max Basket % Equity` | `input.float` | `8.0` | `minval=0.05`, `step=0.05`. |

### E) Visual controls

| Pine name | Default |
| --- | --- |
| `showEMAsExec` | `true` |
| `showHTFStatus` | `false` |
| `showBuyAddLabels` | `true` |
| `showExitLabels` | `true` |
| `showPullbackHighlight` | `false` |
| `showDebugLabel` | `false` |
| `showModeLabels` | `false` |
| `showTopRightDashboard` | `true` |
| `dashboardTextSize` | `small` with options `tiny`, `small`, `normal` |

### W) Webhook controls

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `useWebhookAlerts` | `Enable Webhook Alerts` | `input.bool` | `true` | Guard for `alert()`. |
| `appSecret` | `Executor Secret` | `input.string` | `F7k3Qp9aL` | Passed through alert JSON. |
| `pingOnEveryBar` | `PING on every bar close (testing)` | `input.bool` | `false` | Testing-only alert action. |
| `tifInput` | `Time In Force` | `input.string` | `day` | Options: `day`, `gtc`, `opg`, `cls`, `ioc`, `fok`. |

## Persistent vars / state

- `inBasket`
- `addCount`
- `entryBarIndex`
- `lastEntryPrice`
- `basketQtyUnits`
- `basketCost`
- `basketAvg`
- `basketDollarsUsed`
- `basketB1Dollars`
- `tpPrice`
- `profitTrailArmed`
- `profitTrailActive`
- `profitTrailHigh`
- `profitTrailStop`
- `profitTrailArmPrice`
- `lastExitReason`
- `debugLbl`
- `htfLbl`
- `modeLbl`
- `pauseLbl`
- `dashTbl`

## Indicators and derived calculations used

- Execution TF:
  - `ta.ema(close, emaFastLen)`
  - `ta.ema(close, emaSlowLen)`
  - `ta.rsi(close, rsiLen)`
  - `ta.atr(atrLen)`
  - `ta.highest(high, swingLen)`
  - `ta.lowest(low, swingLen)`
- Pullback:
  - `swingRange = swingHigh - swingLow`
  - `pullbackDepth = swingRange > 0 ? (swingHigh - close) / swingRange : 0.0`
  - `inPullbackZone = pullbackDepth >= pullbackMin`
- Entry momentum:
  - `rsiCrossMode = ta.crossover(rsiVal, rsiTurnUp) and close > high[1]`
  - `fastReclaimMode = close > open and close > close[1] and close > emaFastExec and ta.lowest(low, priceReclaimBars) <= emaFastExec`
  - `momentumConfirm = entryMode == "RSI Crossover" ? rsiCrossMode : (rsiVal > rsiTurnUp and fastReclaimMode)`
- HTF via `request.security(..., barmerge.gaps_off, barmerge.lookahead_off)`:
  - `htfClose`
  - `htfEmaFast`
  - `htfEmaSlow`
  - `htfEmaSlowPrev`
  - `htfEmaSlowSlopeUp = htfEmaSlow > htfEmaSlowPrev`
  - `trendBaseHTF = htfEmaFast > htfEmaSlow and htfClose > htfEmaSlow and htfClose > htfEmaFast`
  - `trendOk = trendBaseHTF and (requireEmaSlowSlopeUp ? htfEmaSlowSlopeUp : true)`
- Session and pause:
  - `inSession = not na(time(timeframe.period, tradeSession))`
  - `isWeekday = dayofweek >= dayofweek.monday and dayofweek <= dayofweek.friday`
  - `sessionOk = useSessionFilter ? (inSession and (tradeWeekdaysOnly ? isWeekday : true)) : true`
  - `regimeFail = not trendOk`
  - `autoPaused = useAutoPause and autoPauseOnRegimeFail and regimeFail`
  - `entriesPaused = pauseNewEntriesManual or autoPaused or (not sessionOk)`
- Helper math:
  - `f_qty_mult`
  - `f_order_id`
  - `f_step_dollars_from_b1`
  - `f_equity_b1_dollars`
  - `f_max_basket_dollars`
  - `f_to_qty`
  - `f_pct_up_price`

## Timeframe assumptions

- Source comments declare `1m execution / 1h trend`.
- Pine logic runs execution calculations on current chart timeframe.
- HTF calculations use `trendTf`, default `60`.
- `execTfNote` is informational only and does not enforce timeframe.
- Session membership uses `time(timeframe.period, tradeSession)`.
- Alerts only fire on `barstate.isconfirmed`.

## Entry conditions

`baseEntrySignal` requires all of:

- `isStock`
- `not entriesPaused`
- `trendOk`
- `inPullbackZone`
- `momentumConfirm`

Initial B1 placement occurs on `not inBasket and baseEntrySignal`, then additionally requires:

- `b1Qty > 0`
- `b1StepDollars <= maxBasketDollarsNow`

Side effects on B1:

- `strategy.entry(f_order_id(0), strategy.long, qty=b1Qty)`
- initialize basket state
- recompute basket average and TP/trail fields
- optional open label
- BUY webhook with `notional=b1StepDollars`

## Add conditions

Add flow requires all of:

- `inBasket`
- `addCount < maxAdds`
- `not entriesPaused`
- `trendOk`
- `inPullbackZone`
- `close <= lastEntryPrice - atrVal * addSpacingATR`
- `close > open`
- `close > close[1]`
- `rsiVal > max(20, rsiTurnUp - 8)`
- next planned add dollars remain within basket cap using `<= maxBasketDollarsNow + 1e-10`

Side effects on add:

- `strategy.entry(f_order_id(step), strategy.long, qty=stepQty)`
- increment `addCount`
- update last entry, basket dollars used, basket qty, basket cost, basket avg
- recompute TP/trail fields from new basket average
- optional add label
- BUY ADD webhook with `notional=stepDollars`

## Exit conditions

While `inBasket`:

- recompute `tpPrice`
- recompute `profitTrailArmPrice` if trailing is enabled

Trail activation requires:

- `trailProfitOn`
- `not profitTrailActive`
- `not na(profitTrailArmPrice)`
- `high >= profitTrailArmPrice`

Plain TP exit:

- `(not trailProfitOn) and close >= tpPrice`

Profit trail exit:

- `trailProfitOn and profitTrailActive and close <= profitTrailStop`

Exit side effects:

- SELL CLOSE webhook without explicit qty
- `strategy.close_all(comment=exitReason)`
- set `lastExitReason`
- optional exit label
- reset basket/trail state to flat

Safety sync:

- If `strategy.position_size == 0 and inBasket`, reset flat-state without sending an alert.

## Pause / regime / session rules

- `entriesPaused = pauseNewEntriesManual or autoPaused or (not sessionOk)`
- `autoPaused = useAutoPause and autoPauseOnRegimeFail and regimeFail`
- `regimeFail = not trendOk`
- Session rules only block new entries/adds.
- The script does not force an exit on pause, regime fail, or session fail.

## Alert / output actions implied by Pine

`f_send(_json)` only calls `alert(_json, alert.freq_once_per_bar_close)` when:

- `useWebhookAlerts`
- `isStock`
- `barstate.isconfirmed`

Alert families:

- BUY B1
- BUY ADD
- SELL CLOSE
- PING

Visual outputs:

- execution EMA plots
- optional pullback background
- optional labels
- top-right dashboard table

## Ambiguities and Pine-specific behavior to match carefully

- `ta.ema`, `ta.rsi`, and `ta.atr` warmup behavior must match Pine exactly before parity can be claimed.
- `request.security` with `barmerge.gaps_off` and `barmerge.lookahead_off` must be matched exactly.
- `htfEmaSlowPrev` shifts EMA inside HTF context, not after merge.
- Session and weekday logic depends on TradingView exchange-time semantics.
- `barstate.isconfirmed` is mandatory for webhook timing.
- `syminfo.type == "stock"` must be preserved; Python should not silently assume stock symbols.
- `strategy.equity` is dynamic in Pine and is not yet reproduced in Python.
- TradingView fill semantics for `strategy.entry`, `strategy.close_all`, and `pyramiding=5` are not implemented.
- `profitTrailArmed` is assigned but not used in later conditions; preserve that field.
- Order id naming is inconsistent by source of truth: `Bismillahi-1` vs `Bismillah-A*`.
- The `1e-10` add-cap tolerance is deliberate and should not be normalized away.

## Mapped in Python during this phase

- Pine input names and defaults
- Persistent state names
- Named series and signal scaffolding
- Thin indicator/helper signatures required by Pine logic
- Non-parity strategy scaffold

## Still not fully implemented

- Exact indicator parity
- Exact HTF merge behavior
- Exact session/calendar behavior
- TradingView equity/order semantics
- Actual entry/add/exit execution parity
- Webhook payload emission/runtime behavior
