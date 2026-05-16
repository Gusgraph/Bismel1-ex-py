# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/transport_policy.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass

from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


ADMIN_RUNTIME_MONITOR_UIDS = {
    "admin-runtime-monitor-prime",
    "admin-runtime-monitor-execution",
}
ALLOWED_TRANSPORTS = {"auto", "rest", "sdk"}
ALLOWED_ROLLOUTS = {"admin_monitor", "paper", "all_paper", "all"}


@dataclass(frozen=True)
class AlpacaTransportDecision:
    primary: str
    fallback: str
    selected: str
    reason: str
    rollout: str


def normalize_transport(value: str | None, *, default: str = "auto") -> str:
    normalized = (value or default).strip().lower()
    return normalized if normalized in ALLOWED_TRANSPORTS else default


def normalize_rollout(value: str | None, *, default: str = "admin_monitor") -> str:
    normalized = (value or default).strip().lower()
    return normalized if normalized in ALLOWED_ROLLOUTS else default


def is_admin_runtime_monitor_context(context: object | None) -> bool:
    if not isinstance(context, ResolvedAlpacaAccountContext):
        return False
    return context.uid in ADMIN_RUNTIME_MONITOR_UIDS and context.environment.strip().lower() == "paper"


def sdk_allowed_for_context(*, settings: AppConfig, context: object | None) -> bool:
    rollout = normalize_rollout(settings.alpaca_transport_rollout)
    if rollout == "all":
        return True
    if not isinstance(context, ResolvedAlpacaAccountContext):
        return False
    environment = context.environment.strip().lower()
    if rollout in {"paper", "all_paper"}:
        return environment == "paper"
    return is_admin_runtime_monitor_context(context)


def resolve_alpaca_transport_decision(
    *,
    settings: AppConfig,
    context: object | None = None,
    sdk_error: bool = False,
) -> AlpacaTransportDecision:
    legacy_transport = normalize_transport(settings.alpaca_transport)
    primary = normalize_transport(settings.alpaca_transport_primary, default="sdk")
    fallback = normalize_transport(settings.alpaca_transport_fallback, default="rest")
    rollout = normalize_rollout(settings.alpaca_transport_rollout)

    if legacy_transport == "sdk":
        return AlpacaTransportDecision(
            primary="sdk",
            fallback=fallback,
            selected="sdk",
            reason="sdk_primary",
            rollout="all",
        )
    if legacy_transport == "rest":
        return AlpacaTransportDecision(
            primary=primary,
            fallback=fallback,
            selected="rest",
            reason="rest_legacy_override",
            rollout=rollout,
        )
    if sdk_error:
        return AlpacaTransportDecision(
            primary=primary,
            fallback=fallback,
            selected=fallback,
            reason="sdk_error_fallback_rest",
            rollout=rollout,
        )
    if settings.admin_runtime_monitor_alpaca_transport.strip().lower() == "sdk" and is_admin_runtime_monitor_context(context):
        return AlpacaTransportDecision(
            primary="sdk",
            fallback=fallback,
            selected="sdk",
            reason="admin_monitor_sdk_primary",
            rollout="admin_monitor",
        )
    if primary == "sdk" and sdk_allowed_for_context(settings=settings, context=context):
        return AlpacaTransportDecision(
            primary=primary,
            fallback=fallback,
            selected="sdk",
            reason="sdk_primary",
            rollout=rollout,
        )
    return AlpacaTransportDecision(
        primary=primary,
        fallback=fallback,
        selected=fallback,
        reason="sdk_disabled_for_context",
        rollout=rollout,
    )
