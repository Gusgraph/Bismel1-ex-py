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

This directory is reserved for Pine source files that are treated as exact source-of-truth references for Python conversion.

Expected first source file:

- `reference/pine/Stocks-pine.pine`

If the file is not yet present in this repo, copy the original Pine source here in a later step without altering the logic during placement.

## TradingView HTF export contract

Real-series HTF parity is blocked until one small TradingView export is committed to this directory.

Expected file path:

- `reference/pine/Stocks-pine-tv-export-sample.csv`

Expected file format:

- UTF-8 CSV
- one row per execution-timeframe bar
- timestamps exported in ISO-8601 with timezone offset, or in UTC-normalized `YYYY-MM-DDTHH:MM:SS+00:00`
- rows sorted ascending by execution bar start time
- no blank separator rows

Required columns:

- `exec_starts_at`
- `exec_ends_at`
- `exec_open`
- `exec_high`
- `exec_low`
- `exec_close`
- `htf_starts_at`
- `htf_ends_at`
- `htf_close_tv`
- `htf_ema_fast_tv`
- `htf_ema_slow_tv`
- `htf_ema_slow_prev_tv`

Expected Pine export semantics:

- The sample must come from `reference/pine/Stocks-pine.pine`.
- The export must use the same Pine inputs as the comparison config, or list any deviations next to the sample file.
- The exported HTF columns must be the direct Pine outputs of:
  - `htfClose`
  - `htfEmaFast`
  - `htfEmaSlow`
  - `htfEmaSlowPrev`
- The sample should be small and readable, ideally 20-80 execution bars covering at least:
  - pre-first-confirmed HTF bars
  - one HTF confirmation boundary
  - one carry-forward span across multiple execution bars
  - one non-initial `htfEmaSlowPrev` value
