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

This file documents the exact source of truth in `reference/pine/Bismel1-Pine-Final.pine`.

## Scope and strategy metadata

- Pine version: `//@version=6`
- Strategy title: `Bismillah-Trobot Stocks v1`
- `overlay=true`
- `pyramiding=5`
- `initial_capital=10000`
- `commission_type=strategy.commission.percent`
- `commission_value=0.1`
- Source comments explicitly describe:
  - stocks only
  - swing mode
  - run on `1H` execution chart with `1D` trend timeframe
  - pullback window `20` bars
  - exit by ATR trailing stop plus optional regime-fail exit
  - hidden auto tier threshold `ATR% < 1.2`
  - split pause behavior: regime fail pauses new baskets only, not recovery adds
  - alerts are emitted via `alert()` JSON payloads
  - alert JSON uses `"license":"tvk_..."`

## Pine sections synchronized

- Strategy declaration and header comments
- `A) Core inputs (SWING)`
- `B) Swing tuning`
- `C) Regime / pause (SPLIT)`
- `D) Visual controls`
- `E) Webhook controls (ALERT() MODE)`
- `Core calculations`
- `AUTO TIER (hidden): ATR% threshold fixed 1.2`
- `Helpers`
- `Position state`
- `ENTRY (edge-trigger) — gated by pauseNewBasket`
- `MULTI adds (tiered + edge-trigger) — NOT gated by regimeFail`
- `EXIT: ATR TRAIL (and optional regime fail)`
- `Dashboard`
- `Optional PING (testing only)`

## Timeframe assumptions

- `execTfNote` default is `Run Bismillah on 4H chart`.
- `trendTf` default is `D`.
- Execution calculations run on the current chart timeframe; the note does not enforce timeframe.
- HTF trend calculations use `request.security(..., trendTf, ..., barmerge.gaps_off, barmerge.lookahead_off)`.
- Session gating uses `time(timeframe.period, tradeSession)`.
- Alerts only send when `barstate.isconfirmed`.

## Inputs and defaults

### A) Core inputs

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `execTfNote` | `Execution Timeframe Note` | `input.string` | `Run Bismillah on 4H chart` | Informational only. |
| `trendTf` | `Trend Timeframe (HTF)` | `input.timeframe` | `D` | Daily HTF trend. |
| `emaFastLen` | `EMA Fast` | `input.int` | `50` | `minval=1` |
| `emaSlowLen` | `EMA Slow` | `input.int` | `200` | `minval=1` |
| `swingLen` | `Pullback Window (bars)` | `input.int` | `20` | `minval=5` |
| `pullbackMin` | `Min Pullback Depth (0.40 = 40%)` | `input.float` | `0.40` | `minval=0.05`, `maxval=0.95`, `step=0.05` |
| `rsiLen` | `RSI Length (Exec TF)` | `input.int` | `14` | `minval=2` |
| `atrLen` | `ATR Length (Exec TF)` | `input.int` | `14` | `minval=1` |
| `entryMode` | `Entry Repeat Behavior` | `input.string` | `Fast Reclaim` | Options: `Fast Reclaim`, `RSI Crossover` |
| `priceReclaimBars` | `Reclaim Confirm Bars` | `input.int` | `2` | `minval=1`, `maxval=10` |

### B) Swing tuning

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `rsiTurnUp` | `RSI Turn-Up` | `input.float` | `46` | `minval=1`, `maxval=99` |
| `maxAdds` | `Recovery Lot Multiblier` | `input.int` | `2` | `minval=0`, `maxval=4` |
| `firstLotDollars` | `First Lot-$` | `input.float` | `100.0` | `minval=1.0`, `step=1.0` |
| `q1` | `MULTI 1 x` | `input.float` | `1.2` | `step=0.1` |
| `q2` | `MULTI 2 x` | `input.float` | `1.6` | `step=0.1` |
| `q3` | `MULTI 3 x` | `input.float` | `2.0` | `step=0.1` |
| `q4` | `MULTI 4 x` | `input.float` | `2.5` | `step=0.1` |
| `maxBasketPctEquity` | `Max Basket % Equity` | `input.float` | `10.0` | `minval=0.05`, `step=0.05` |
| `atrTrailMult` | `ATR Trail Mult` | `input.float` | `3.0` | `minval=0.5`, `step=0.1` |
| `exitOnRegimeFail` | `Exit on Regime Fail (HTF)` | `input.bool` | `true` | Optional exit gate. |

### C) Regime / pause (SPLIT)

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `pauseNewEntriesManual` | `Pause New Entries (Manual)` | `input.bool` | `false` | Pauses new baskets and adds. |
| `pauseOnRegimeFail` | `Pause NEW Basket on Regime Fail` | `input.bool` | `true` | Applies only to new baskets. |
| `useAutoPause` | `Use Auto Pause by Regime` | `input.bool` | `true` | |
| `autoPauseOnRegimeFail` | `Auto Pause when Regime Fails` | `input.bool` | `true` | |
| `requireEmaSlowSlopeUp` | `Require HTF EMA200 Slope Up` | `input.bool` | `true` | |
| `emaSlowSlopeLookback` | `HTF EMA200 Slope Lookback` | `input.int` | `10` | `minval=1` |
| `useSessionFilter` | `Use Market Session Filter` | `input.bool` | `false` | |
| `tradeSession` | `Stocks Session (Exchange Time)` | `input.session` | `0930-1600` | TradingView exchange-time semantics. |
| `tradeWeekdaysOnly` | `Weekdays Only` | `input.bool` | `true` | |

### D) Visual controls

| Pine name | Default |
| --- | --- |
| `showEMAsExec` | `true` |
| `showBuyAddLabels` | `true` |
| `showExitLabels` | `true` |
| `showPullbackHighlight` | `false` |
| `showTopRightDashboard` | `true` |
| `dashboardTextSize` | `small` with options `tiny`, `small`, `normal` |

### E) Webhook controls

| Pine name | Label | Type | Default | Notes |
| --- | --- | --- | --- | --- |
| `useWebhookAlerts` | `Enable Webhook Alerts` | `input.bool` | `true` | |
| `licenseKey` | `License (tvk_...)` | `input.string` | `tvk_REPLACE_ME` | Sent as JSON field `"license"`. |
| `tifInput` | `Time In Force` | `input.string` | `day` | Options: `day`, `gtc`, `opg`, `cls`, `ioc`, `fok` |
| `pingOnEveryBar` | `PING on every bar close (testing)` | `input.bool` | `false` | Testing-only path. |

## Derived calculations and helpers

- Execution indicators:
  - `emaFastExec = ta.ema(close, emaFastLen)`
  - `emaSlowExec = ta.ema(close, emaSlowLen)`
  - `rsiVal = ta.rsi(close, rsiLen)`
  - `atrVal = ta.atr(atrLen)`
  - `atrPct = close > 0 ? (atrVal / close) * 100.0 : 0.0`
- Pullback window:
  - `swingHigh = ta.highest(high, swingLen)`
  - `swingLow = ta.lowest(low, swingLen)`
  - `pullbackDepth = swingRange > 0 ? (swingHigh - close) / swingRange : 0.0`
  - `inPullbackZone = pullbackDepth >= pullbackMin`
- Entry momentum:
  - `lowestLowReclaim = ta.lowest(low, priceReclaimBars)`
  - `rsiCrossMode = ta.crossover(rsiVal, rsiTurnUp) and close > high[1]`
  - `fastReclaimMode = close > open and close > close[1] and close > emaFastExec and lowestLowReclaim <= emaFastExec`
  - `momentumConfirm = entryMode == "RSI Crossover" ? rsiCrossMode : (rsiVal > rsiTurnUp and fastReclaimMode)`
- HTF trend:
  - `htfClose`
  - `htfEmaFast`
  - `htfEmaSlow`
  - `htfEmaSlowPrev`
  - `htfEmaSlowSlopeUp`
  - `trendBaseHTF`
  - `trendOk`
- Session and pause split:
  - `regimeFail = not trendOk`
  - `autoPaused = useAutoPause and autoPauseOnRegimeFail and regimeFail`
  - `pauseNewBasket = pauseNewEntriesManual or (pauseOnRegimeFail and autoPaused) or (not sessionOk)`
  - `pauseAdds = pauseNewEntriesManual or (not sessionOk)`
- Hidden tier helpers:
  - `tierThresh = 1.2`
  - `isLowTier = atrPct < tierThresh`
  - `f_spacing_atr(step)` uses low-tier or high-tier fixed spacing tables
  - `f_min_drop_pct(step)` uses low-tier or high-tier fixed drop-percent tables
- Sizing helpers:
  - `f_qty_mult(step)`
  - `f_step_dollars_from_first(step, firstLotDollars)`
  - `f_max_basket_dollars()`
  - `f_to_qty(dollars)`
  - `f_txt_size(size_key)`

## Persistent vars and state

### Pine `var` declarations

- `var int addCount = 0`
- `var float lastAddPrice = na`
- `var float dollarsUsed = 0.0`
- `var float posHigh = na`
- `var float trailStop = na`
- `var table dashTbl = table.new(position.top_right, 2, 11, border_width=1)`

### Python scaffolding mirror

- `BismillahTrobotStocksV1State.add_count`
- `BismillahTrobotStocksV1State.last_add_price`
- `BismillahTrobotStocksV1State.dollars_used`
- `BismillahTrobotStocksV1State.pos_high`
- `BismillahTrobotStocksV1State.trail_stop`
- `BismillahTrobotStocksV1State.dash_table_initialized`
- `BismillahTrobotStocksV1State.position_avg_price`

`position_avg_price` is a Python-only runtime mirror used to evaluate add gating without claiming broker/runtime parity.

## Entry, add, exit, and alert naming

### Strategy order / close names

- Base entry order name: `FirstLot`
- Add order names: `MULTI-1`, `MULTI-2`, `MULTI-3`, `MULTI-4`
- Close comments: `EXIT_REGIME`, `EXIT_ATR`

### Alert id and payload naming

- Alert id builder: `f_id(_action, _tag)`
- Format: `<Action>-<SYMBOL>-<TIMEFRAME>-<time_close_ms>-<Tag>`
- Documented examples:
  - `Buy-AAPL-4h-<time_ms>-First`
  - `Multi-AAPL-4h-<time_ms>-L1`
  - `Sell-AAPL-4h-<time_ms>-ATRTrail`
- Base entry webhook:
  - `intent:"open"`
  - `alert_id = f_id("Buy", "First")`
- Add webhook:
  - `intent:"add"`
  - `alert_id = f_id("Multi", "L" + str.tostring(step))`
- Exit webhook:
  - `alert_id = f_id("Sell", hitRegime ? "Regime" : "ATRTrail")`
- Ping webhook:
  - `action:"ping"`
  - `alert_id = f_id("Ping", "")`

## Current Python scaffolding scope

- `config.py` matches the exact Pine metadata and input defaults from `Bismel1-Pine-Final.pine`.
- `models.py` reflects the Pine-derived series names, signal names, and persistent vars relevant to source-truth synchronization.
- `strategy.py` computes Pine-aligned derived series for:
  - EMA, RSI, ATR, ATR%
  - pullback and reclaim signals
  - HTF trend merge
  - split pause flags
  - hidden low-tier detection
- `strategy.py` now evaluates bar-by-bar signal/state parity for:
  - `regimeFail`
  - `autoPaused`
  - `pauseNewBasket`
  - `pauseAdds`
  - `baseEntrySignal`
  - `baseEntryTrigger`
  - `addBounceConfirm`
  - `gateAtrOk`
  - `gateDpOk`
  - `capOk`
  - `addSignalRaw`
  - `addTrigger`
  - `hitAtrTrail`
  - `hitRegime`
- The sequential evaluator resets Pine `var` mirrors when flat, updates `posHigh` and `trailStop` only while a mirrored position is open, and advances the minimum position mirror required to preserve trigger edges:
  - base entry opens the mirrored position using close-price notional-to-qty conversion
  - adds update `addCount`, `lastAddPrice`, `dollarsUsed`, `position_size`, and weighted `position_avg_price`
  - ATR-trail or regime-fail exit resets the mirrored position state back to flat
- This is signal/state parity only. The mirrored position exists solely so later bars can evaluate Pine-equivalent gates and edge triggers.

## This phase proves

- New-basket pause and add pause are split exactly as Pine documents: regime fail can pause `pauseNewBasket` while leaving `pauseAdds` clear.
- Base entry uses Pine naming and edge-trigger behavior: `baseEntryTrigger = baseEntrySignal and not baseEntrySignal[1]`.
- Add logic uses Pine naming and edge-trigger behavior: `addTrigger = addSignalRaw and not addSignalRaw[1]`, evaluated against the prior bar's raw signal from the sequential state path rather than recomputing the prior bar with current state.
- ATR-trail exit is evaluated as a parity trigger only: `close <= trailStop` while the mirrored position is open.
- Regime-fail exit is evaluated as a parity trigger only: `exitOnRegimeFail and regimeFail` while the mirrored position is open.
- Focused tests now cover:
  - `pauseNewBasket` vs `pauseAdds`
  - base entry edge triggering
  - multi-add raw signal and trigger edges
  - ATR trail exit trigger
  - regime-fail exit trigger

## Documented ambiguities and non-goals

- Execution parity is not complete. Python scaffolding does not place orders, send alerts, or implement broker/runtime orchestration.
- `strategy.equity` is still not modeled. Basket-cap checks still use configured initial capital rather than TradingView's live `strategy.equity`.
- `strategy.position_size` and TradingView `strategy.position_avg_price` are now mirrored only to the minimum extent needed for signal/state parity. This is not broker execution parity.
- Session filtering is only approximated in Python using bar timestamps plus `HHMM-HHMM` parsing. TradingView exchange-time semantics, holidays, and DST handling remain unresolved.
- `request.security(..., gaps_off, lookahead_off)` merge intent is preserved, but real exported TradingView series are still required for proof.
- The Pine PING path uses `"asset_class":"crypto"` while the rest of the script is stock-only. This is preserved as a source-truth quirk, not normalized.
- Dashboard rendering is documented but not implemented as Python UI output.
- Same-bar interactions between add fills and exit triggers are not proven against TradingView exports yet. The current implementation preserves the trigger booleans as a parity layer, but exact broker-emulator side effects remain unproven.

## Remaining parity gaps

- No broker/runtime position lifecycle parity beyond the minimum mirror required for signal/state gating
- No live `strategy.equity` parity for basket-cap evolution
- No TradingView-export comparison for the new signal/state fields yet
- No proof yet for same-bar order/execution side effects in TradingView's broker emulator
- No execution-side alert payload emission yet
