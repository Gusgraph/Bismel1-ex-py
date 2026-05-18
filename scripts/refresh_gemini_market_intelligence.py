# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/refresh_gemini_market_intelligence.py
# ======================================================

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services.gemini_market_intelligence import GeminiMarketIntelligenceRefreshService
from app.shared.config import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh shared Gemini market and symbol intelligence cache.")
    parser.add_argument("--symbol", action="append", default=[], help="Optional symbol to include. May be repeated.")
    parser.add_argument("--force", action="store_true", help="Refresh even when cache is fresh.")
    args = parser.parse_args()

    service = GeminiMarketIntelligenceRefreshService(settings=get_settings())
    result = service.refresh(symbols=args.symbol, force=args.force)
    print(
        json.dumps(
            {
                "ok": result.ok,
                "market_refreshed": result.market_refreshed,
                "symbols_discovered": result.symbols_discovered,
                "symbols_refreshed": result.symbols_refreshed,
                "symbols_skipped_fresh": result.symbols_skipped_fresh,
                "errors": result.errors,
                "tokens_used": result.tokens_used,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
