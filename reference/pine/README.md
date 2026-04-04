<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: reference/pine/README.md
======================================================
-->

# Pine References

`reference/pine/Bismel1-Pine-Final.pine` is the current exact source-of-truth Pine file for the Bismel1 stock strategy.

The older `reference/pine/Stocks-pine.pine` file remains in the repo as historical material only. Source-truth synchronization work must follow `Bismel1-Pine-Final.pine` unless the repository owner explicitly changes that contract.

## TradingView export contract

Real-series parity is still blocked until a small TradingView export is committed for the active source file.

Expected sample file path:

- `reference/pine/Bismel1-Pine-Final-tv-export-sample.csv`

Expected file format:

- UTF-8 CSV
- one row per execution-timeframe bar
- timestamps in ISO-8601 with timezone offset, or UTC-normalized ISO-8601
- rows sorted ascending by execution-bar start time
- no blank separator rows

Required execution-bar columns:

- `exec_starts_at`
- `exec_ends_at`
- `exec_open`
- `exec_high`
- `exec_low`
- `exec_close`

Required HTF / derived columns:

- `htf_starts_at`
- `htf_ends_at`
- `htf_close_tv`
- `htf_ema_fast_tv`
- `htf_ema_slow_tv`
- `htf_ema_slow_prev_tv`
- `trend_ok_tv`
- `regime_fail_tv`
- `pause_new_basket_tv`
- `pause_adds_tv`

Recommended alert / state columns:

- `atr_pct_tv`
- `is_low_tier_tv`
- `trail_stop_tv`
- `base_entry_signal_tv`
- `base_entry_trigger_tv`
- `add_signal_raw_tv`
- `add_trigger_tv`
- `hit_atr_trail_tv`
- `hit_regime_tv`

Expected export semantics:

- The sample must come from `reference/pine/Bismel1-Pine-Final.pine`.
- The export must use the Pine defaults unless deviations are documented alongside the CSV.
- The HTF columns must be direct Pine outputs of:
  - `htfClose`
  - `htfEmaFast`
  - `htfEmaSlow`
  - `htfEmaSlowPrev`
- The pause columns must reflect the exact split-pause behavior:
  - `pauseNewBasket`
  - `pauseAdds`
- The sample should be small and readable, ideally 20-80 execution bars, and include:
  - warmup bars
  - at least one HTF confirmation boundary
  - at least one new-basket pause case
  - at least one add-eligible case if available

