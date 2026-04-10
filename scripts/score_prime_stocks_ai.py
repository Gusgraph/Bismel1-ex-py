# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/score_prime_stocks_ai.py
# ======================================================

from __future__ import annotations

import argparse
import json
import os

from app.services.firestore_runtime_store import PrimeStocksFirestoreRuntimeStore
from app.services.gemini_ai_scoring import DEFAULT_GEMINI_MODEL, GeminiAiScoringService
from app.shared.config import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Score and store Prime Stocks AI cache records.")
    parser.add_argument("--scope", choices=["market", "symbol"], required=True)
    parser.add_argument("--headline", required=True)
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--context", default=None)
    args = parser.parse_args()

    settings = get_settings()
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if api_key == "":
        raise RuntimeError("GEMINI_API_KEY is required.")
    model = (os.getenv("GEMINI_MODEL") or settings.gemini_model or DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL

    scorer = GeminiAiScoringService(api_key=api_key, model=model)
    record = scorer.score_headline(
        scope=args.scope,
        symbol=args.symbol,
        headline=args.headline,
        context=args.context,
    )

    store = PrimeStocksFirestoreRuntimeStore(settings=settings)
    if args.scope == "market":
        store.write_ai_market_record(record)
    else:
        if record.symbol is None:
            raise RuntimeError("--symbol is required for symbol scope.")
        store.write_ai_symbol_record(record)

    print(json.dumps(_serialize_result(record), ensure_ascii=True, indent=2))
    return 0


def _serialize_result(record) -> dict[str, object]:
    return {
        "scope": record.scope,
        "symbol": record.symbol,
        "Ai_regime_label": record.Ai_regime_label,
        "Ai_sentiment_label": record.Ai_sentiment_label,
        "Ai_safety_label": record.Ai_safety_label,
        "Ai_confidence": record.Ai_confidence,
        "Ai_reason": record.Ai_reason,
        "Ai_updated_at": record.Ai_updated_at,
        "Ai_source": record.Ai_source,
        "Ai_execution_allowed": record.Ai_execution_allowed,
        "Ai_block_new_entries": record.Ai_block_new_entries,
        "Ai_block_adds": record.Ai_block_adds,
        "Ai_blocked_reason": record.Ai_blocked_reason,
    }


if __name__ == "__main__":
    raise SystemExit(main())
