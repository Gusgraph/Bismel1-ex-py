<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: project-notes/VM-DUPLICATE-FILES-REVIEW.md
======================================================
-->

# VM Duplicate Files Review

This is an inspection-only cleanup review. No files were removed in this phase.

| Absolute path | Why it looks duplicate or obsolete | Recommendation |
| --- | --- | --- |
| `/var/www/html/bismel1.com/python/bismel1_engine/models.py` | Existing Python strategy model file for Bismel1 inside the web repo. The new executor repo now owns the dedicated Python executor structure for this concern. | Keep for now, remove later after parity migration is complete and callers are moved. |
| `/var/www/html/bismel1.com/python/bismel1_engine/indicators.py` | Existing indicator implementation file in the web repo overlaps with the new executor repo responsibility. | Keep for now, remove later after exact Pine parity work is re-established in this repo. |
| `/var/www/html/bismel1.com/python/bismel1_engine/strategy.py` | Existing strategy evaluation logic for Bismel1 in the web repo appears to be an earlier Python executor attempt. | Keep for now, remove later after the dedicated executor repo becomes authoritative. |
| `/var/www/html/bismel1.com/python/bismel1_engine/scanner_cli.py` | CLI-oriented executor helper inside the web repo suggests mixed concerns and overlaps with the new executor direction. | Keep for now, remove later after replacement tooling exists here if still needed. |
| `/var/www/html/bismel1.com/python/tests/test_bismel1_strategy.py` | Test file tied to the older in-web-repo Python strategy implementation. | Keep for now, remove later after equivalent tests are recreated in this repo. |
| `/var/www/html/bismel1.com/Trobot - Stocks-Swing-4.pine` | Pine source-of-truth file currently lives in the web repo instead of the dedicated executor repo reference area. | Keep and copy later into `reference/pine/Trobot - Stocks-Swing-4.pine`; remove later only if the web repo should no longer host strategy source. |
