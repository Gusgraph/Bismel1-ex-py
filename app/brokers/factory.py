# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/factory.py
# ======================================================

from __future__ import annotations

from app.brokers.alpaca_paper_trading import AlpacaPaperTradingAdapter
from app.brokers.alpaca_sdk_trading import AlpacaSdkBrokerAdapter
from app.shared.config import AppConfig


def resolve_alpaca_transport(settings: AppConfig) -> str:
    transport = (settings.alpaca_transport or "rest").strip().lower()
    if transport == "sdk":
        return "sdk"
    return "rest"


def build_alpaca_broker_adapter(settings: AppConfig) -> AlpacaPaperTradingAdapter | AlpacaSdkBrokerAdapter:
    if resolve_alpaca_transport(settings) == "sdk":
        return AlpacaSdkBrokerAdapter(settings=settings)
    return AlpacaPaperTradingAdapter(settings=settings)

