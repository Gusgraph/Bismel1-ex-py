# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/test_gemini_sentiment.py
# ======================================================

from __future__ import annotations

import json
import os
import sys

from app.services.gemini_ai_scoring import DEFAULT_GEMINI_MODEL, GeminiAiScoringService


DEFAULT_HEADLINE = "Nvidia jumps after reporting stronger-than-expected revenue and raising AI demand guidance."
def main() -> int:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if api_key == "":
        raise RuntimeError("GEMINI_API_KEY is required.")

    model = (os.getenv("GEMINI_MODEL") or DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    headline = " ".join(sys.argv[1:]).strip() or DEFAULT_HEADLINE
    scorer = GeminiAiScoringService(api_key=api_key, model=model)
    record = scorer.score_headline(scope="symbol", symbol="AAPL", headline=headline)
    result = {
        "Ai_regime_label": record.Ai_regime_label,
        "Ai_sentiment_label": record.Ai_sentiment_label,
        "Ai_safety_label": record.Ai_safety_label,
        "Ai_confidence": record.Ai_confidence,
        "Ai_reason": record.Ai_reason,
    }
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
