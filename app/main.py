# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/main.py
# ======================================================

from __future__ import annotations

from fastapi import FastAPI

from app.shared.config import get_settings
from app.shared.logging import configure_logging


configure_logging()
settings = get_settings()

app = FastAPI(
    title="Bismel1 Executor Python",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "app": settings.app_name,
        "service": "executor-bootstrap",
        "status": "ok",
        "phase": "phase-1-bootstrap",
        "runtime_target": "cloud-run",
    }


@app.get("/_diag")
def diag() -> dict[str, object]:
    return {
        "app": settings.app_name,
        "environment": settings.environment,
        "cloud_run_target": settings.cloud_run_target,
        "pine_source_filename": settings.pine_source_filename,
        "live_execution_implemented": False,
    }
