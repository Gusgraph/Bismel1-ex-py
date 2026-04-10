# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/services/gemini_ai_scoring.py
# ======================================================

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.products.stocks.bismel1.models import AiCacheRecord, PrimeStocksAiDecision


DEFAULT_GEMINI_MODEL = "gemini-2.5-flash-lite"
_RISK_ORDER = {"risk_off": 0, "neutral": 1, "risk_on": 2}
_SENTIMENT_ORDER = {"bearish": 0, "neutral": 1, "bullish": 2}
_SAFETY_ORDER = {"unsafe": 0, "caution": 1, "safe": 2}


class GeminiAiScoringError(RuntimeError):
    """Raised when Gemini scoring cannot produce a normalized AI result."""


class HttpJsonGenerateProtocol(Protocol):
    def generate(self, *, url: str, payload: dict[str, object]) -> dict[str, object]:
        raise NotImplementedError


class UrllibGeminiGenerateClient(HttpJsonGenerateProtocol):
    def generate(self, *, url: str, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload).encode("utf-8")
        request = Request(
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=27) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise GeminiAiScoringError(f"Gemini API HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise GeminiAiScoringError(f"Gemini API request failed: {exc.reason}") from exc


class GeminiAiScoringService:
    def __init__(
        self,
        *,
        api_key: str,
        model: str = DEFAULT_GEMINI_MODEL,
        http_client: HttpJsonGenerateProtocol | None = None,
    ) -> None:
        self._api_key = api_key.strip()
        self._model = model.strip() or DEFAULT_GEMINI_MODEL
        self._http_client = http_client or UrllibGeminiGenerateClient()
        if self._api_key == "":
            raise GeminiAiScoringError("GEMINI_API_KEY is required for Gemini scoring.")

    def score_headline(
        self,
        *,
        scope: str,
        headline: str,
        symbol: str | None = None,
        context: str | None = None,
        updated_at: datetime | None = None,
    ) -> AiCacheRecord:
        resolved_scope = scope.strip().lower()
        resolved_symbol = None if symbol is None else symbol.strip().upper() or None
        prompt = self._build_prompt(scope=resolved_scope, symbol=resolved_symbol, headline=headline, context=context)
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent?key={self._api_key}"
        response = self._http_client.generate(url=url, payload=payload)
        text = _extract_response_text(response)
        normalized = normalize_ai_classification_payload(json.loads(text))
        return build_ai_cache_record(
            scope=resolved_scope,
            symbol=resolved_symbol,
            ai_payload=normalized,
            updated_at=updated_at or datetime.now(tz=UTC),
            source=f"gemini:{self._model}",
        )

    def _build_prompt(self, *, scope: str, symbol: str | None, headline: str, context: str | None) -> str:
        scope_label = "market-wide regime" if scope == "market" else f"symbol-specific stock view for {symbol or 'UNKNOWN'}"
        context_line = "" if context is None or context.strip() == "" else f"\nContext: {context.strip()}"
        return (
            "You are classifying one stock-market headline for Bismel1 Prime Stocks.\n"
            f"Scope: {scope_label}.\n"
            "Return JSON only with exactly these keys:\n"
            "Ai_regime_label, Ai_sentiment_label, Ai_safety_label, Ai_confidence, Ai_reason.\n"
            "Rules:\n"
            "- Ai_regime_label: risk_on, neutral, or risk_off\n"
            "- Ai_sentiment_label: bullish, neutral, or bearish\n"
            "- Ai_safety_label: safe, caution, or unsafe\n"
            "- Ai_confidence: decimal from 0.0 to 1.0\n"
            "- Ai_reason: one short sentence under 27 words\n"
            f"Headline: {headline.strip()}{context_line}"
        )


def normalize_ai_classification_payload(payload: dict[str, object]) -> dict[str, object]:
    regime = _normalize_label(str(payload.get("Ai_regime_label", "neutral")), allowed=_RISK_ORDER, default="neutral")
    sentiment = _normalize_label(
        str(payload.get("Ai_sentiment_label", "neutral")),
        allowed=_SENTIMENT_ORDER,
        default="neutral",
    )
    safety = str(payload.get("Ai_safety_label", "caution")).strip().lower()
    if safety == "avoid":
        safety = "unsafe"
    safety = _normalize_label(safety, allowed=_SAFETY_ORDER, default="caution")
    confidence = _clamp_confidence(payload.get("Ai_confidence"))
    reason = str(payload.get("Ai_reason", "")).strip() or "Gemini classification returned no explicit reason."
    return {
        "Ai_regime_label": regime,
        "Ai_sentiment_label": sentiment,
        "Ai_safety_label": safety,
        "Ai_confidence": confidence,
        "Ai_reason": reason,
    }


def build_ai_cache_record(
    *,
    scope: str,
    symbol: str | None,
    ai_payload: dict[str, object],
    updated_at: datetime,
    source: str,
) -> AiCacheRecord:
    blocked_reason = _resolve_ai_blocked_reason(ai_payload)
    block_new_entries = blocked_reason in {"ai_safety_unsafe", "ai_regime_risk_off", "ai_sentiment_bearish"}
    block_adds = blocked_reason in {"ai_safety_unsafe", "ai_regime_risk_off"}
    return AiCacheRecord(
        scope=scope,
        symbol=symbol,
        Ai_regime_label=str(ai_payload["Ai_regime_label"]),
        Ai_sentiment_label=str(ai_payload["Ai_sentiment_label"]),
        Ai_safety_label=str(ai_payload["Ai_safety_label"]),
        Ai_confidence=float(ai_payload["Ai_confidence"]),
        Ai_reason=str(ai_payload["Ai_reason"]),
        Ai_updated_at=updated_at.astimezone(UTC).isoformat(),
        Ai_source=source,
        Ai_execution_allowed=not (block_new_entries or block_adds),
        Ai_block_new_entries=block_new_entries,
        Ai_block_adds=block_adds,
        Ai_blocked_reason=blocked_reason,
        is_stale=False,
        is_available=True,
    )


def merge_ai_cache_records(
    *,
    market_record: AiCacheRecord | None,
    symbol_record: AiCacheRecord | None,
    max_age_minutes: int,
    now: datetime,
) -> PrimeStocksAiDecision:
    resolved_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    checked_market = _mark_record_staleness(market_record, max_age_minutes=max_age_minutes, now=resolved_now)
    checked_symbol = _mark_record_staleness(symbol_record, max_age_minutes=max_age_minutes, now=resolved_now)

    for checked in (checked_market, checked_symbol):
        if checked is None:
            return _blocked_ai_decision(
                reason="ai_cache_unavailable",
                message="AI cache is unavailable for the required Prime Stocks scope.",
                market_record=checked_market,
                symbol_record=checked_symbol,
            )
        if checked.is_stale:
            return _blocked_ai_decision(
                reason="ai_cache_stale",
                message="AI cache is stale for Prime Stocks runtime evaluation.",
                market_record=checked_market,
                symbol_record=checked_symbol,
            )

    assert checked_market is not None and checked_symbol is not None
    blocked_reason = _first_non_empty([checked_symbol.Ai_blocked_reason, checked_market.Ai_blocked_reason])
    return PrimeStocksAiDecision(
        Ai_regime_label=_select_label(
            [checked_market.Ai_regime_label, checked_symbol.Ai_regime_label],
            ordering=_RISK_ORDER,
            default="neutral",
        ),
        Ai_sentiment_label=_select_label(
            [checked_market.Ai_sentiment_label, checked_symbol.Ai_sentiment_label],
            ordering=_SENTIMENT_ORDER,
            default="neutral",
        ),
        Ai_safety_label=_select_label(
            [checked_market.Ai_safety_label, checked_symbol.Ai_safety_label],
            ordering=_SAFETY_ORDER,
            default="caution",
        ),
        Ai_confidence=min(checked_market.Ai_confidence, checked_symbol.Ai_confidence),
        Ai_reason=" | ".join(
            [reason for reason in [checked_market.Ai_reason.strip(), checked_symbol.Ai_reason.strip()] if reason]
        ),
        Ai_updated_at=_oldest_timestamp(
            [checked_market.Ai_updated_at, checked_symbol.Ai_updated_at],
        ),
        Ai_source="cached_gemini",
        Ai_execution_allowed=blocked_reason is None,
        Ai_block_new_entries=checked_market.Ai_block_new_entries or checked_symbol.Ai_block_new_entries,
        Ai_block_adds=checked_market.Ai_block_adds or checked_symbol.Ai_block_adds,
        Ai_blocked_reason=blocked_reason,
        market_record=checked_market,
        symbol_record=checked_symbol,
        is_stale=False,
        is_available=True,
    )


def serialize_ai_decision(ai_decision: PrimeStocksAiDecision | None) -> dict[str, object] | None:
    if ai_decision is None:
        return None
    payload = asdict(ai_decision)
    return payload


def _blocked_ai_decision(
    *,
    reason: str,
    message: str,
    market_record: AiCacheRecord | None,
    symbol_record: AiCacheRecord | None,
) -> PrimeStocksAiDecision:
    return PrimeStocksAiDecision(
        Ai_regime_label="neutral",
        Ai_sentiment_label="neutral",
        Ai_safety_label="caution",
        Ai_confidence=0.0,
        Ai_reason=message,
        Ai_updated_at=_oldest_timestamp(
            [
                None if market_record is None else market_record.Ai_updated_at,
                None if symbol_record is None else symbol_record.Ai_updated_at,
            ]
        ),
        Ai_source="cached_gemini",
        Ai_execution_allowed=False,
        Ai_block_new_entries=True,
        Ai_block_adds=True,
        Ai_blocked_reason=reason,
        market_record=market_record,
        symbol_record=symbol_record,
        is_stale=reason == "ai_cache_stale",
        is_available=reason != "ai_cache_unavailable",
    )


def _mark_record_staleness(
    record: AiCacheRecord | None,
    *,
    max_age_minutes: int,
    now: datetime,
) -> AiCacheRecord | None:
    if record is None:
        return None
    if record.Ai_updated_at is None:
        return AiCacheRecord(**{**asdict(record), "is_stale": True})
    parsed = datetime.fromisoformat(record.Ai_updated_at.replace("Z", "+00:00"))
    resolved_parsed = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    is_stale = now.astimezone(UTC) - resolved_parsed.astimezone(UTC) > timedelta(minutes=max_age_minutes)
    return AiCacheRecord(**{**asdict(record), "is_stale": is_stale})


def _extract_response_text(payload: dict[str, object]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or len(candidates) == 0:
        raise GeminiAiScoringError(f"Gemini API returned no candidates: {payload}")

    first_candidate = candidates[0]
    if not isinstance(first_candidate, dict):
        raise GeminiAiScoringError(f"Gemini API candidate was not an object: {payload}")

    content = first_candidate.get("content")
    if not isinstance(content, dict):
        raise GeminiAiScoringError(f"Gemini API candidate content missing: {payload}")

    parts = content.get("parts")
    if not isinstance(parts, list):
        raise GeminiAiScoringError(f"Gemini API candidate parts missing: {payload}")

    combined = "".join(
        [part["text"] for part in parts if isinstance(part, dict) and isinstance(part.get("text"), str)]
    ).strip()
    if combined == "":
        raise GeminiAiScoringError(f"Gemini API returned empty text: {payload}")
    return combined


def _resolve_ai_blocked_reason(ai_payload: dict[str, object]) -> str | None:
    if str(ai_payload["Ai_safety_label"]) == "unsafe":
        return "ai_safety_unsafe"
    if str(ai_payload["Ai_regime_label"]) == "risk_off":
        return "ai_regime_risk_off"
    if str(ai_payload["Ai_sentiment_label"]) == "bearish":
        return "ai_sentiment_bearish"
    return None


def _normalize_label(value: str, *, allowed: dict[str, int], default: str) -> str:
    normalized = value.strip().lower()
    return normalized if normalized in allowed else default


def _clamp_confidence(value: object) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, resolved))


def _select_label(values: list[str], *, ordering: dict[str, int], default: str) -> str:
    normalized_values = [value for value in values if value in ordering]
    if not normalized_values:
        return default
    return sorted(normalized_values, key=lambda value: ordering[value])[0]


def _oldest_timestamp(values: list[str | None]) -> str | None:
    parsed_values: list[datetime] = []
    for value in values:
        if value is None:
            continue
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        parsed_values.append(parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC))
    if not parsed_values:
        return None
    return min(parsed_values).astimezone(UTC).isoformat()


def _first_non_empty(values: list[str | None]) -> str | None:
    for value in values:
        if value is not None and value.strip() != "":
            return value
    return None
