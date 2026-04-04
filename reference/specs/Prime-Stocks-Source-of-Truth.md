<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: reference/specs/Prime-Stocks-Source-of-Truth.md
======================================================
-->

# Prime Stocks Strategy — Source of Truth

## Authority

This document is the strategy concept source of truth for Prime Stocks.

- User intent is the final authority.
- Python implementation is the real strategy home.
- Pine is reference material only.
- Any concept tweak comes from the user.

## Current Default Timeframe Choice

**Current chosen default: 1H execution / 1D trend**

In plain terms:

- **1H decides when**
- **1D helps decide whether**

This replaces older wording that centered the base working structure around 4H / 1D.

## Strategy Type

Long-only stocks swing/pullback automation.

Designed to trade strong stocks in bullish higher-timeframe conditions, enter on pullback resolution, manage controlled recovery adds, and exit through ATR-aware trailing logic with optional regime invalidation.

## Core Market Logic

The strategy trades only when the stock is in a valid bullish higher-timeframe regime.

Trend / regime filter uses:

- HTF EMA50
- HTF EMA200
- HTF close relative to both moving averages
- optional HTF EMA200 slope-up requirement using a lookback window

Trend is valid when:

- EMA50 > EMA200
- HTF close is above both EMA50 and EMA200
- optional slope filter is satisfied when enabled

## Entry Concept

This is a pullback-and-reclaim strategy.

The strategy does **not** enter just because price dropped.
It waits for:

1. a valid bullish HTF regime
2. a controlled pullback on execution timeframe
3. reclaim / momentum confirmation showing the pullback is resolving

The intended behavior is:

- strength
- then reset
- then renewed strength

## Pullback Zone

Execution timeframe pullback logic uses a recent lookback window, commonly around 20 bars.

Core idea:

- compute swing high / swing low over the pullback window
- measure pullback depth as distance below swing high relative to the range
- require pullback depth to meet or exceed the configured minimum threshold

## Entry Confirmation

Two conceptual confirmation modes exist:

### Fast Reclaim
Default mode.

Typical reclaim behavior includes:

- close > open
- close > prior close
- close above EMA50
- reclaim window confirmation using the recent lowest-low check over the configured reclaim bars

### RSI Crossover
Optional mode.

Typical crossover behavior includes:

- RSI crossing above threshold
- reclaim confirmation still required

## Base Entry

The first entry begins a new basket.

Naming:

- **FirstLot**
- executor intent: **open**

Base entry is only allowed when:

- symbol is stock
- HTF trend is valid
- pullback zone is valid
- momentum / reclaim confirmation is valid
- new basket is not paused

Sizing concept:

- first lot uses configured dollars
- automation routing should send notional dollars cleanly

## Basket Logic and Recovery Adds

The strategy is basket-aware.

It separates:

- base entry
- recovery adds

This separation is operationally important because the executor must block a new base basket if a position already exists, while still allowing explicitly signaled adds.

Recovery add naming:

- **MULTI**
- executor intent: **add**

Adds are:

- controlled
- limited
- volatility-gated
- not unlimited averaging down
- not random martingale behavior

## Volatility Tiering

Adds use ATR percent gating.

Concept:

- compute ATR% on execution timeframe
- low tier below about **1.2**
- otherwise high tier

Tier affects spacing thresholds such as:

- ATR spacing gate
- minimum drop from average price gate

## Add Trigger Requirements

A valid add requires all relevant gates to pass, including:

- bounce confirmation
- spacing gate
- drop-from-average gate
- basket cap gate

Adds are edge-triggered:

- one add trigger per bar-close transition from false to true

## Basket Risk Cap

Basket exposure is capped.

Concept:

- basket dollars used across FirstLot + MULTI adds
- capped by configured max basket % equity
- adds beyond cap must be blocked

## Exit Logic

Primary exit framework:

### ATR Trailing Exit
Core exit.

Concept:

- track highest price since entry
- trail stop derived from posHigh minus ATR times configured multiplier
- exit when close falls through trail

### Optional Regime-Fail Exit
Secondary invalidation behavior.

If enabled, regime failure can close the basket.

## Pause Logic Split

This is a critical strategy rule.

- **Regime fail blocks new baskets**
- **Regime fail does not automatically block recovery adds**

Recovery adds may still continue unless blocked by:

- manual pause
- session filter
- other explicit gating rules

This split behavior must be preserved.

## Session Rules

For stocks, optional session filtering can apply.

Typical session intent:

- US market session
- weekdays only
- used to avoid out-of-session stock triggers when enabled

## Signal Intent Structure

Executor signal intent must remain explicit:

- base entry = `open`
- recovery add = `add`

This is not optional wording.
It is a core operational rule.

Typical payload concepts may include:

- action
- symbol
- timeframe
- license
- alert_id
- bar_time_ms
- asset_class
- intent

## Operational Rules

The executor/backend must respect:

- open vs add intent separation
- duplicate alert protection
- clean alert identity structure
- safe basket behavior

## What This Strategy Is Trying To Do

Capture bullish continuation swings in strong stocks by:

- aligning with HTF trend
- waiting for controlled pullbacks
- confirming reclaim behavior
- entering selectively
- using controlled adds only when allowed
- exiting with volatility-aware trailing and regime protection

## What This Strategy Is Not

- not a random alert generator
- not always in market
- not unlimited averaging down
- not short-selling
- not allowed to open a new basket on top of an already open position unless the signal is an allowed add

## Canonical Rules Summary

- Strategy type: long-only stocks swing/pullback automation
- Current chosen default: **1H execution / 1D trend**
- Trend filter: HTF bullish regime requires EMA50 > EMA200 and HTF close above both, with optional EMA200 slope-up lookback
- Entry model: pullback zone plus reclaim confirmation, not breakout chasing
- Base entry: FirstLot, new basket only, intent open
- Adds: controlled MULTI recovery ladder, volatility-gated, limited count, intent add
- ATR% tier rule: low tier below about 1.2 ATR%
- Risk cap: basket dollars capped by max basket % equity
- Exits: ATR trailing stop first, optional regime-fail exit
- Pause split: regime fail blocks new baskets, but recovery adds can still continue unless other pause/session rules block them
- Operational rule: backend must respect open vs add separation and duplicate protection

This document is the source of truth unless explicitly replaced by a newer strategy spec approved by the user.
