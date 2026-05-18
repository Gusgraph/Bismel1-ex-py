# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: scripts/gemini_smoke_test.py
# ======================================================

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services.gemini_runtime_diagnostics import run_gemini_smoke_test
from app.shared.config import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a safe Gemini runtime smoke test.")
    parser.add_argument("--no-write", action="store_true", help="Do not write the sanitized AI runtime event to Firestore.")
    args = parser.parse_args()

    settings = get_settings()
    result = run_gemini_smoke_test(settings=settings, write_event=not args.no_write)
    safe_output = {
        "ok": result.get("ok"),
        "source": result.get("source"),
        "product_code": result.get("product_code"),
        "model": result.get("model"),
        "response_status": result.get("response_status"),
        "parsed_status": result.get("parsed_status"),
        "input_tokens": result.get("input_tokens"),
        "output_tokens": result.get("output_tokens"),
        "total_tokens": result.get("total_tokens"),
        "latency_ms": result.get("latency_ms"),
        "error_category": result.get("error_category"),
        "error_message_safe": result.get("error_message_safe"),
    }
    print(json.dumps(safe_output, ensure_ascii=True, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
