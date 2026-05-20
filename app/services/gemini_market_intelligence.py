# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/services/gemini_market_intelligence.py
# ======================================================

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Protocol

from app.products.stocks.bismel1.models import AiCacheRecord
from app.runtime.prime_stocks_dry_run import build_prime_stocks_runtime_service
from app.services.firestore_runtime_store import PrimeStocksFirestoreRuntimeStore
from app.services.gemini_ai_scoring import DEFAULT_GEMINI_MODEL, GeminiAiScoringService, GeminiAiScoringResult
from app.services.gemini_runtime_diagnostics import build_gemini_runtime_event
from app.shared.config import AppConfig


class SymbolScorerProtocol(Protocol):
    def score_headline_with_metadata(
        self,
        *,
        scope: str,
        headline: str,
        symbol: str | None = None,
        context: str | None = None,
        updated_at: datetime | None = None,
    ) -> GeminiAiScoringResult:
        raise NotImplementedError


@dataclass(frozen=True)
class GeminiRefreshResult:
    ok: bool
    market_refreshed: bool
    symbols_discovered: int
    symbols_refreshed: int
    symbols_skipped_fresh: int
    symbols_remaining_stale: int = 0
    batch_limit: int = 0
    errors: list[dict[str, object]] = field(default_factory=list)
    tokens_used: int = 0


class GeminiMarketIntelligenceRefreshService:
    def __init__(
        self,
        *,
        settings: AppConfig,
        store: PrimeStocksFirestoreRuntimeStore | None = None,
        scorer: SymbolScorerProtocol | None = None,
        now_provider=None,
    ) -> None:
        self._settings = settings
        self._store = store or PrimeStocksFirestoreRuntimeStore(settings=settings)
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))
        if scorer is not None:
            self._scorer = scorer
        else:
            api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
            model = (os.getenv("GEMINI_MODEL") or settings.gemini_model or DEFAULT_GEMINI_MODEL).strip()
            self._scorer = GeminiAiScoringService(api_key=api_key, model=model)

    def collect_unique_symbols(self, *, extra_symbols: list[str] | None = None) -> list[str]:
        symbols: set[str] = set()
        for symbol in extra_symbols or []:
            normalized = _normalize_symbol(symbol)
            if normalized:
                symbols.add(normalized)
        for symbol in _env_symbols():
            symbols.add(symbol)

        try:
            prime_service = build_prime_stocks_runtime_service(settings=self._settings)
            for target in prime_service.list_scheduler_targets():
                for symbol in prime_service.list_target_symbols(
                    uid=target.uid,
                    account_id=target.account_id,
                    alpaca_account_id=target.alpaca_account_id,
                    slot_number=target.slot_number,
                ):
                    normalized = _normalize_symbol(symbol)
                    if normalized:
                        symbols.add(normalized)
        except Exception:
            fallback = _normalize_symbol(self._settings.prime_stocks_default_symbol)
            if fallback:
                symbols.add(fallback)

        return sorted(symbols)[: self._settings.ai_refresh_max_symbols]

    def refresh(self, *, symbols: list[str] | None = None, force: bool = False) -> GeminiRefreshResult:
        now = self._now_provider()
        errors: list[dict[str, object]] = []
        tokens_used = 0
        market_refreshed = False
        symbols_refreshed = 0
        symbols_skipped_fresh = 0

        if force or _is_record_stale(
            self._store.load_ai_market_record(),
            ttl_minutes=self._settings.ai_market_cache_ttl_minutes,
            now=now,
        ):
            try:
                started = perf_counter()
                result = self._scorer.score_headline_with_metadata(
                    scope="market",
                    headline="Broad U.S. market regime and risk posture refresh.",
                    context="Classify only broad market posture. Do not use broker, account, or customer data.",
                    updated_at=now,
                )
                self._store.write_ai_market_record(result.record)
                tokens_used += int(result.usage_metadata.get("total_tokens") or 0)
                self._store.write_ai_runtime_event(
                    build_gemini_runtime_event(
                        source="ai_scanner",
                        product_code="shared_market",
                        model=result.record.Ai_source.replace("gemini:", ""),
                        prompt="market-regime-refresh",
                        response_status="success",
                        parsed_status="ok",
                        usage_metadata=result.usage_metadata,
                        latency_ms=int(round((perf_counter() - started) * 1000)),
                        sanitized_summary={"scope": "market", "status": "refreshed"},
                    )
                )
                market_refreshed = True
            except Exception as exc:
                errors.append({"scope": "market", "error_category": "gemini_refresh_failed", "message": str(exc)})

        unique_symbols = self.collect_unique_symbols(extra_symbols=symbols)
        stale_symbols: list[str] = []
        for symbol in unique_symbols:
            existing = self._store.load_ai_symbol_record(symbol)
            if not force and not _is_record_stale(
                existing,
                ttl_minutes=self._settings.ai_symbol_cache_ttl_minutes,
                now=now,
            ):
                symbols_skipped_fresh += 1
                continue
            stale_symbols.append(symbol)

        batch_limit = max(1, int(self._settings.ai_refresh_batch_size))
        symbols_to_refresh = stale_symbols[:batch_limit]
        symbols_remaining_stale = max(0, len(stale_symbols) - len(symbols_to_refresh))

        for symbol in symbols_to_refresh:
            try:
                started = perf_counter()
                result = self._scorer.score_headline_with_metadata(
                    scope="symbol",
                    symbol=symbol,
                    headline=f"{symbol} symbol-level market intelligence refresh.",
                    context=(
                        "Classify sentiment, setup support, safety, and confidence from public market context only. "
                        "Do not use user, broker, order, or account data."
                    ),
                    updated_at=now,
                )
                self._store.write_ai_symbol_record(result.record)
                tokens_used += int(result.usage_metadata.get("total_tokens") or 0)
                self._store.write_ai_runtime_event(
                    build_gemini_runtime_event(
                        source="ai_scanner",
                        product_code="shared_symbol_cache",
                        symbol=symbol,
                        model=result.record.Ai_source.replace("gemini:", ""),
                        prompt=f"symbol-refresh:{symbol}",
                        response_status="success",
                        parsed_status="ok",
                        usage_metadata=result.usage_metadata,
                        latency_ms=int(round((perf_counter() - started) * 1000)),
                        sanitized_summary={
                            "scope": "symbol",
                            "symbol": symbol,
                            "sentiment": result.record.Ai_sentiment_label,
                            "setup_support": result.record.setup_support_label,
                            "safety": result.record.Ai_safety_label,
                        },
                    )
                )
                symbols_refreshed += 1
            except Exception as exc:
                errors.append({"scope": "symbol", "symbol": symbol, "error_category": "gemini_refresh_failed", "message": str(exc)})

        self._write_batch_summary_event(
            symbols_discovered=len(unique_symbols),
            symbols_refreshed=symbols_refreshed,
            symbols_skipped_fresh=symbols_skipped_fresh,
            symbols_remaining_stale=symbols_remaining_stale,
            batch_limit=batch_limit,
            errors=errors,
            tokens_used=tokens_used,
        )
        return GeminiRefreshResult(
            ok=len(errors) == 0,
            market_refreshed=market_refreshed,
            symbols_discovered=len(unique_symbols),
            symbols_refreshed=symbols_refreshed,
            symbols_skipped_fresh=symbols_skipped_fresh,
            symbols_remaining_stale=symbols_remaining_stale,
            batch_limit=batch_limit,
            errors=errors,
            tokens_used=tokens_used,
        )

    def _write_batch_summary_event(
        self,
        *,
        symbols_discovered: int,
        symbols_refreshed: int,
        symbols_skipped_fresh: int,
        symbols_remaining_stale: int,
        batch_limit: int,
        errors: list[dict[str, object]],
        tokens_used: int,
    ) -> None:
        try:
            self._store.write_ai_runtime_event(
                build_gemini_runtime_event(
                    source="ai_scanner",
                    product_code="shared_symbol_cache",
                    model=getattr(self._scorer, "model", None) or self._settings.gemini_model,
                    prompt="ai-refresh-batch-summary",
                    response_status="success" if not errors else "partial",
                    parsed_status="ok" if not errors else "partial",
                    usage_metadata={"total_tokens": tokens_used},
                    sanitized_summary={
                        "scope": "refresh_batch",
                        "symbols_discovered": symbols_discovered,
                        "symbols_refreshed": symbols_refreshed,
                        "symbols_skipped_fresh": symbols_skipped_fresh,
                        "symbols_remaining_stale": symbols_remaining_stale,
                        "batch_limit": batch_limit,
                        "error_count": len(errors),
                    },
                    error_category=None if not errors else "gemini_refresh_partial_errors",
                    error_message_safe=None if not errors else "One or more symbols failed during the bounded AI refresh batch.",
                )
            )
        except Exception:
            # Batch summary writeback is diagnostic only; symbol cache writes above remain authoritative.
            return


def _is_record_stale(record: AiCacheRecord | None, *, ttl_minutes: int, now: datetime) -> bool:
    if record is None or record.Ai_updated_at is None:
        return True
    try:
        parsed = datetime.fromisoformat(record.Ai_updated_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    resolved = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return now.astimezone(UTC) - resolved.astimezone(UTC) > timedelta(minutes=max(1, ttl_minutes))


def _normalize_symbol(symbol: str) -> str | None:
    normalized = symbol.strip().upper()
    return normalized or None


def _env_symbols() -> list[str]:
    raw = os.getenv("AI_REFRESH_SYMBOLS", "")
    return [symbol for item in raw.split(",") if (symbol := _normalize_symbol(item))]
