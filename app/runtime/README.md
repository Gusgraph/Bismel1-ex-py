<!--
اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
version: 1
======================================================
- App Name: Bismel1-ex-py
- Gusgraph LLC -
- Author: Gus Kazem
- https://Gusgraph.com
- File Path: app/runtime/README.md
======================================================
-->

# Runtime

Prime Stocks dry-run runtime code lives here for Cloud Run server-side execution.

Current phase:

- reads one Prime Stocks runtime config
- fetches Alpaca market data
- runs the canonical strategy
- writes runtime state, snapshot, signal, execution, action, and logs to Firestore
- can submit guarded Alpaca paper orders
- does not place live orders
- expects Cloud Scheduler to invoke `POST /runtime/prime-stocks/scheduled`
- evaluates only on newly closed `1H` bars
- keeps browser state out of runtime continuity
