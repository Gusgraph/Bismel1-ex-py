# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/runtime/prime_stocks_dry_run.py
# ======================================================

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import logging
from time import perf_counter
from typing import Any, Callable
from uuid import uuid4

from app.brokers.factory import build_alpaca_broker_adapter
from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter
from app.brokers.alpaca_paper_trading import (
    AlpacaPaperExecutionResult,
    AlpacaPaperPositionState,
    AlpacaPaperSubmissionState,
    AlpacaPaperTradingAdapter,
    AlpacaPaperTradingError,
)
from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.models import (
    AiCacheRecord,
    BismillahTrobotStocksV1Input,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    PrimeStocksStrategyResult,
)
from app.products.stocks.bismel1.strategy import _bool_at, run_prime_stocks_strategy
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
    RuntimeAccountTarget,
)
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksLatestExecutionRecord,
    PrimeStocksRuntimeConfigRecord,
    PrimeStocksRuntimeStateRecord,
    PrimeStocksRuntimeStoreError,
    build_default_runtime_config,
)
from app.services.gemini_ai_scoring import merge_ai_cache_records, serialize_ai_decision
from app.shared.config import AppConfig


logger = logging.getLogger(__name__)

ACTIVE_ORDER_STATUSES = {"accepted", "new", "partially_filled", "filled", "submitted"}
PRIME_DIAGNOSTIC_ATR_REVIEW = "DIAGNOSTIC_ATR_REVIEW"
PRIME_DIAGNOSTIC_REGIME_REVIEW = "DIAGNOSTIC_REGIME_REVIEW"
PRIME_DIAGNOSTIC_ACTIONS = {
    PRIME_DIAGNOSTIC_ATR_REVIEW,
    PRIME_DIAGNOSTIC_REGIME_REVIEW,
}
ADMIN_CRYPTO_MONITOR_UIDS = frozenset({"admin-runtime-monitor-prime", "admin-runtime-monitor-execution"})
ADMIN_CRYPTO_MONITOR_SYMBOLS = frozenset({"UNI/USD", "LINK/USD"})
POSITION_RESIDUAL_QTY_THRESHOLD = 0.000001
POSITION_RESIDUAL_MARKET_VALUE_THRESHOLD = 0.01
RESIDUAL_CLEANUP_ACTIVE_STATUSES = frozenset({"accepted", "new", "partially_filled", "submitted", "pending_new"})
RESIDUAL_CLEANUP_MAX_ATTEMPTS = 3
RESIDUAL_CLEANUP_COOLDOWN_MINUTES = 15


def _is_active_order_status(order_status: str | None) -> bool:
    return order_status in ACTIVE_ORDER_STATUSES


def _position_has_broker_quantity(position: Any | None) -> bool:
    qty = _maybe_float(getattr(position, "qty", None)) if position is not None else None
    return qty is not None and qty > 0


def _position_is_residual(position: Any | None) -> bool:
    if position is None:
        return False
    qty = _maybe_float(getattr(position, "qty", None))
    if qty is None or qty <= 0:
        return False
    if abs(qty) <= POSITION_RESIDUAL_QTY_THRESHOLD:
        return True
    market_value = _maybe_float(getattr(position, "market_value", None))
    return market_value is not None and abs(market_value) <= POSITION_RESIDUAL_MARKET_VALUE_THRESHOLD


def _position_is_strategy_managed_open(position: Any | None) -> bool:
    return _position_has_broker_quantity(position) and not _position_is_residual(position)


def _normalized_order_symbol(order: dict[str, Any]) -> str:
    return str(order.get("symbol") or "").strip().upper()


def _normalized_order_side(order: dict[str, Any]) -> str:
    return str(order.get("side") or "").strip().lower()


def _normalized_order_status(order: dict[str, Any]) -> str:
    return str(order.get("status") or "").strip().lower()


def _normalized_order_client_id(order: dict[str, Any]) -> str:
    return str(order.get("client_order_id") or "").strip()


def _parse_order_time(order: dict[str, Any]) -> datetime | None:
    for key in ("submitted_at", "created_at", "updated_at", "filled_at"):
        value = order.get(key)
        if not isinstance(value, str) or value.strip() == "":
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            continue
    return None


def _residual_cleanup_attempt_count(*, recent_orders: list[dict[str, Any]], symbol: str, prefix: str) -> int:
    symbol = symbol.strip().upper()
    return sum(
        1
        for order in recent_orders
        if _normalized_order_symbol(order) == symbol
        and _normalized_order_side(order) == "sell"
        and _normalized_order_client_id(order).startswith(prefix)
    )


def _has_recent_residual_cleanup_attempt(*, recent_orders: list[dict[str, Any]], symbol: str, prefix: str, now: datetime) -> bool:
    symbol = symbol.strip().upper()
    cooldown_started_after = now.astimezone(UTC) - timedelta(minutes=RESIDUAL_CLEANUP_COOLDOWN_MINUTES)
    for order in recent_orders:
        if (
            _normalized_order_symbol(order) != symbol
            or _normalized_order_side(order) != "sell"
            or not _normalized_order_client_id(order).startswith(prefix)
        ):
            continue
        order_time = _parse_order_time(order)
        if order_time is not None and order_time >= cooldown_started_after:
            return True
    return False


def _has_pending_residual_cleanup_order(*, recent_orders: list[dict[str, Any]], symbol: str, prefix: str) -> bool:
    symbol = symbol.strip().upper()
    for order in recent_orders:
        client_order_id = _normalized_order_client_id(order)
        if (
            _normalized_order_symbol(order) == symbol
            and _normalized_order_side(order) == "sell"
            and client_order_id.startswith(prefix)
            and _normalized_order_status(order) in RESIDUAL_CLEANUP_ACTIVE_STATUSES
        ):
            return True
    return False


def _has_recent_bismel1_full_close(*, recent_orders: list[dict[str, Any]], symbol: str, prefixes: tuple[str, ...]) -> bool:
    symbol = symbol.strip().upper()
    close_reasons = {"take_profit", "strategy_exit", "stop_loss", "manual_close", "fractional_residual_cleanup"}
    for order in recent_orders:
        client_order_id = _normalized_order_client_id(order)
        if _normalized_order_symbol(order) != symbol or _normalized_order_side(order) != "sell":
            continue
        if not any(client_order_id.startswith(prefix) for prefix in prefixes):
            continue
        close_scope = str(order.get("close_scope") or order.get("request_action") or "").strip().lower()
        close_reason = str(order.get("close_reason") or order.get("exit_reason") or "").strip().lower()
        if close_scope == "full_position" or close_reason in close_reasons or client_order_id:
            return True
    return False


@dataclass(frozen=True)
class PrimeStocksRuntimeResult:
    run_id: str
    mode: str
    runtime_target: str
    product_key: str
    strategy_key: str
    strategy_title: str
    symbol: str
    asset_type: str
    enabled: bool
    trigger_type: str
    trigger_source: str
    candidate_action: str
    execution_decision: str
    order_status: str
    order_submitted: bool
    order_id: str | None
    client_order_id: str | None
    add_tier: int | None
    execution_allowed: bool
    skipped_reason: str | None
    latest_signal_time: str | None
    ai: dict[str, object] | None
    status: str
    message: str
    bars_processed_execution: int
    bars_processed_trend: int
    firestore_paths: dict[str, str]
    strategy_reasoning: dict[str, Any] | None = None
    signal_score: float | None = None
    timing: dict[str, Any] | None = None


@dataclass(frozen=True)
class PrimeStocksExecutionFailure:
    execution_decision: str
    skipped_reason: str
    execution_allowed: bool = False
    retry_count: int = 0
    broker_error_code: str | None = None
    broker_error_message: str | None = None


class PrimeStocksRuntimeService:
    def __init__(
        self,
        settings: AppConfig,
        market_data: AlpacaMarketDataAdapter,
        runtime_store: PrimeStocksFirestoreRuntimeStore,
        paper_trading: AlpacaPaperTradingAdapter | None = None,
        account_resolver: LaravelAlpacaAccountResolver | None = None,
        strategy_runner: Callable[..., object] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._market_data = market_data
        self._runtime_store = runtime_store
        self._paper_trading = paper_trading or build_alpaca_broker_adapter(settings=settings)
        self._account_resolver = account_resolver or LaravelAlpacaAccountResolver(settings=settings)
        self._strategy_runner = strategy_runner or run_prime_stocks_strategy
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))

    def list_scheduler_targets(self) -> list[RuntimeAccountTarget]:
        return self._account_resolver.list_runtime_targets()

    def list_target_symbols(
        self,
        *,
        uid: str | None,
        account_id: int | None,
        alpaca_account_id: int | None,
        slot_number: int | None = None,
        symbol: str | None = None,
    ) -> list[str]:
        if symbol is not None and symbol.strip():
            return [symbol.strip().upper()]

        default_runtime_config = build_default_runtime_config(self._settings)
        fallback_runtime_config = _override_symbol(
            default_runtime_config,
            None,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
        )
        fallback_runtime_config = self._hydrate_scoped_slot_context(fallback_runtime_config)
        runtime_config = self._runtime_store.load_runtime_config(fallback_runtime_config)
        resolved_runtime_config = _override_runtime_selection(
            runtime_config,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
        )

        return _resolve_schedulable_symbols(resolved_runtime_config)

    def _hydrate_scoped_slot_context(
        self,
        runtime_config: PrimeStocksRuntimeConfigRecord,
    ) -> PrimeStocksRuntimeConfigRecord:
        if runtime_config.slot_number is not None:
            return runtime_config
        if runtime_config.account_id is None or runtime_config.alpaca_account_id is None:
            return runtime_config

        try:
            account_context = self._account_resolver.resolve_runtime_account(runtime_config)
        except AlpacaAccountResolutionError:
            return runtime_config

        return replace(
            runtime_config,
            uid=runtime_config.uid or account_context.uid,
            account_id=runtime_config.account_id or account_context.account_id,
            alpaca_account_id=runtime_config.alpaca_account_id or account_context.alpaca_account_id,
            slot_number=account_context.slot_number,
        )

    def record_cycle_summary(
        self,
        *,
        uid: str,
        account_id: int,
        alpaca_account_id: int | None,
        slot_number: int | None = None,
        run_id: str,
        trigger_type: str,
        trigger_source: str,
        results: list[dict[str, object]],
        target_count: int,
        completed_count: int,
        timing: dict[str, object] | None = None,
    ) -> None:
        self._runtime_store.write_runtime_cycle_summary(
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
            run_id=run_id,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            target_count=target_count,
            completed_count=completed_count,
            results=results,
            timing=timing,
            service_revision=self._settings.cloud_run_revision,
            service_name=self._settings.cloud_run_service_name,
        )

    def _run_validation_ping(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        trigger_type: str,
        trigger_source: str,
    ) -> PrimeStocksRuntimeResult:
        account_context = _build_fallback_account_context(runtime_config)
        latest_signal_time: datetime | None = None
        bars_processed_execution = 0
        bars_processed_trend = 0
        ai_decision = None
        candidate_action = "PING"

        try:
            account_context = self._account_resolver.resolve_runtime_account(runtime_config)
        except AlpacaAccountResolutionError as exc:
            return self._persist_validation_ping_result(
                run_id=run_id,
                runtime_config=runtime_config,
                account_context=account_context,
                execution_decision="linked_account_unavailable",
                skipped_reason="linked_account_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                latest_signal_time=None,
                bars_processed_execution=0,
                bars_processed_trend=0,
                ai_decision=None,
                message=(
                    "Prime Stocks validation ping blocked because the selected linked Alpaca account could not be resolved. "
                    f"{exc}"
                ),
                status="blocked",
            )

        try:
            bar_set = self._market_data.fetch_prime_stocks_bars(
                symbol=runtime_config.symbol,
                asset_type=runtime_config.asset_type,
                product_key=runtime_config.product_key,
                execution_timeframe=runtime_config.execution_timeframe,
                trend_timeframe=runtime_config.trend_timeframe,
                execution_limit=runtime_config.execution_bar_limit,
                trend_limit=runtime_config.trend_bar_limit,
                credential_context=account_context,
            )
        except Exception as exc:
            return self._persist_validation_ping_result(
                run_id=run_id,
                runtime_config=runtime_config,
                account_context=account_context,
                execution_decision="market_data_unavailable",
                skipped_reason="market_data_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                latest_signal_time=None,
                bars_processed_execution=0,
                bars_processed_trend=0,
                ai_decision=None,
                message=(
                    "Prime Stocks validation ping blocked because Alpaca market data could not be fetched. "
                    f"{exc}"
                ),
                status="blocked",
            )

        latest_signal_time = _resolve_latest_signal_time(bar_set.execution_bars)
        bars_processed_execution = len(bar_set.execution_bars)
        bars_processed_trend = len(bar_set.trend_bars)
        market_data_stale_bypassed = _is_stale_market_data(
            latest_signal_time=latest_signal_time,
            execution_timeframe=runtime_config.execution_timeframe,
            now=self._now_provider(),
        )

        try:
            ai_decision = self._load_validation_ping_ai_decision(runtime_config.symbol)
        except PrimeStocksRuntimeStoreError as exc:
            return self._persist_validation_ping_result(
                run_id=run_id,
                runtime_config=runtime_config,
                account_context=account_context,
                execution_decision="validation_ping_ai_cache_unavailable",
                skipped_reason="validation_ping_ai_cache_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                latest_signal_time=latest_signal_time,
                bars_processed_execution=bars_processed_execution,
                bars_processed_trend=bars_processed_trend,
                ai_decision=None,
                message=(
                    "Prime Stocks validation ping blocked because AI cache could not be loaded for validation. "
                    f"{exc}"
                ),
                status="blocked",
            )

        if ai_decision.is_stale:
            ai_decision = replace(
                ai_decision,
                Ai_execution_allowed=True,
                Ai_block_new_entries=False,
                Ai_block_adds=False,
                Ai_blocked_reason=None,
            )
        if not ai_decision.is_available:
            ai_decision = replace(
                ai_decision,
                Ai_execution_allowed=True,
                Ai_block_new_entries=False,
                Ai_block_adds=False,
                Ai_blocked_reason=None,
            )

        try:
            latest_execution = self._runtime_store.load_latest_execution_record(
                uid=account_context.uid,
                account_id=runtime_config.account_id,
                slot_number=runtime_config.slot_number,
            )
        except PrimeStocksRuntimeStoreError as exc:
            return self._persist_validation_ping_result(
                run_id=run_id,
                runtime_config=runtime_config,
                account_context=account_context,
                execution_decision="runtime_store_unavailable",
                skipped_reason="runtime_store_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                latest_signal_time=latest_signal_time,
                bars_processed_execution=bars_processed_execution,
                bars_processed_trend=bars_processed_trend,
                ai_decision=ai_decision,
                message=(
                    "Prime Stocks validation ping blocked because Firestore execution state could not be loaded. "
                    f"{exc}"
                ),
                status="blocked",
            )

        execution_decision = "validation_ping_ok"
        skipped_reason = None
        status = "validation"
        execution_key = _build_execution_key(candidate_action, latest_signal_time, symbol=runtime_config.symbol)
        if latest_execution.execution_key == execution_key:
            execution_decision = "validation_ping_duplicate"
            skipped_reason = "validation_ping_duplicate"
            status = "no_op"

        return self._persist_validation_ping_result(
            run_id=run_id,
            runtime_config=runtime_config,
            account_context=account_context,
            execution_decision=execution_decision,
            skipped_reason=skipped_reason,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            latest_signal_time=latest_signal_time,
            bars_processed_execution=bars_processed_execution,
            bars_processed_trend=bars_processed_trend,
            ai_decision=ai_decision,
            message=_build_validation_ping_message(
                execution_decision=execution_decision,
                market_data_stale_bypassed=market_data_stale_bypassed,
                ai_stale_bypassed=ai_decision.is_stale,
            ),
            status=status,
        )

    def _persist_validation_ping_result(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        account_context: ResolvedAlpacaAccountContext,
        execution_decision: str,
        skipped_reason: str | None,
        trigger_type: str,
        trigger_source: str,
        latest_signal_time: datetime | None,
        bars_processed_execution: int,
        bars_processed_trend: int,
        ai_decision,
        message: str,
        status: str,
    ) -> PrimeStocksRuntimeResult:
        strategy_result = _build_placeholder_strategy_result(
            runtime_config=runtime_config,
            status=status,
            message=message,
        )
        self._runtime_store.write_runtime_result(
            run_id=run_id,
            runtime_config=runtime_config,
            account_context=account_context,
            strategy_result=strategy_result,
            candidate_action="PING",
            latest_signal_time=latest_signal_time,
            runtime_message=message,
            execution_mode="dry-run",
            execution_decision=execution_decision,
            execution_result=None,
            skipped_reason=skipped_reason,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            ai_decision=ai_decision,
            test_mode=True,
            test_trigger="ping",
            symbol_override=runtime_config.symbol,
            validation_only=True,
        )
        return PrimeStocksRuntimeResult(
            run_id=run_id,
            mode="dry-run",
            runtime_target=runtime_config.runtime_target,
            product_key=runtime_config.product_key,
            strategy_key=runtime_config.strategy_key,
            strategy_title=runtime_config.strategy_title,
            symbol=runtime_config.symbol,
            asset_type=runtime_config.asset_type,
            enabled=runtime_config.enabled,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            candidate_action="PING",
            execution_decision=execution_decision,
            order_status="not_submitted",
            order_submitted=False,
            order_id=None,
            client_order_id=None,
            add_tier=None,
            execution_allowed=False,
            skipped_reason=skipped_reason,
            latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            ai=serialize_ai_decision(ai_decision),
            status=status,
            message=message,
            bars_processed_execution=bars_processed_execution,
            bars_processed_trend=bars_processed_trend,
            firestore_paths=self._runtime_store.get_paths(
                uid=runtime_config.uid,
                account_id=runtime_config.account_id,
                slot_number=runtime_config.slot_number,
            ).__dict__,
        )

    def _load_validation_ping_ai_decision(self, symbol: str):
        market_record = self._runtime_store.load_ai_market_record()
        symbol_record = self._runtime_store.load_ai_symbol_record(symbol)
        if symbol_record is None and market_record is not None:
            symbol_record = AiCacheRecord(
                scope="symbol",
                symbol=symbol,
                Ai_regime_label=market_record.Ai_regime_label,
                Ai_sentiment_label=market_record.Ai_sentiment_label,
                Ai_safety_label=market_record.Ai_safety_label,
                Ai_confidence=market_record.Ai_confidence,
                Ai_reason=f"validation_fallback:{market_record.Ai_reason}",
                Ai_updated_at=market_record.Ai_updated_at,
                Ai_source=market_record.Ai_source,
                Ai_execution_allowed=market_record.Ai_execution_allowed,
                Ai_block_new_entries=market_record.Ai_block_new_entries,
                Ai_block_adds=market_record.Ai_block_adds,
                Ai_blocked_reason=market_record.Ai_blocked_reason,
                is_stale=market_record.is_stale,
                is_available=market_record.is_available,
            )
        return merge_ai_cache_records(
            market_record=market_record,
            symbol_record=symbol_record,
            max_age_minutes=self._settings.ai_cache_max_age_minutes,
            now=self._now_provider(),
        )

    def _persist_blocked_runtime_result(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        account_context: ResolvedAlpacaAccountContext,
        execution_decision: str,
        skipped_reason: str,
        trigger_type: str,
        trigger_source: str,
        message: str,
        candidate_action: str = "BLOCKED",
        latest_signal_time: datetime | None = None,
        bars_processed_execution: int = 0,
        bars_processed_trend: int = 0,
        ai_decision=None,
        status: str = "blocked",
    ) -> PrimeStocksRuntimeResult:
        strategy_result = _build_placeholder_strategy_result(
            runtime_config=runtime_config,
            status=status,
            message=message,
        )
        try:
            self._runtime_store.write_runtime_result(
                run_id=run_id,
                runtime_config=runtime_config,
                account_context=account_context,
                strategy_result=strategy_result,
                candidate_action=candidate_action,
                latest_signal_time=latest_signal_time,
                runtime_message=message,
                execution_mode="dry-run",
                execution_decision=execution_decision,
                execution_result=None,
                skipped_reason=skipped_reason,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                ai_decision=ai_decision,
            )
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked with execution_decision=%s but Firestore result persistence failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                execution_decision,
                trigger_type,
                trigger_source,
                run_id,
            )
            return PrimeStocksRuntimeResult(
                run_id=run_id,
                mode="dry-run",
                runtime_target=runtime_config.runtime_target,
                product_key=runtime_config.product_key,
                strategy_key=runtime_config.strategy_key,
                strategy_title=runtime_config.strategy_title,
                symbol=runtime_config.symbol,
                asset_type=runtime_config.asset_type,
                enabled=runtime_config.enabled,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                candidate_action=candidate_action,
                execution_decision=execution_decision,
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="runtime_result_persistence_failed",
                latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                ai=serialize_ai_decision(ai_decision),
                status="degraded",
                message=f"{message} Firestore result persistence failed. {exc}",
                bars_processed_execution=bars_processed_execution,
                bars_processed_trend=bars_processed_trend,
                firestore_paths=self._runtime_store.get_paths(
                    uid=runtime_config.uid,
                    account_id=runtime_config.account_id,
                    slot_number=runtime_config.slot_number,
                ).__dict__,
            )

        return PrimeStocksRuntimeResult(
            run_id=run_id,
            mode="dry-run",
            runtime_target=runtime_config.runtime_target,
            product_key=runtime_config.product_key,
            strategy_key=runtime_config.strategy_key,
            strategy_title=runtime_config.strategy_title,
            symbol=runtime_config.symbol,
            asset_type=runtime_config.asset_type,
            enabled=runtime_config.enabled,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            candidate_action=candidate_action,
            execution_decision=execution_decision,
            order_status="not_submitted",
            order_submitted=False,
            order_id=None,
            client_order_id=None,
            add_tier=_parse_add_tier(candidate_action),
            execution_allowed=False,
            skipped_reason=skipped_reason,
            latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            ai=serialize_ai_decision(ai_decision),
            status=status,
            message=message,
            bars_processed_execution=bars_processed_execution,
            bars_processed_trend=bars_processed_trend,
            firestore_paths=self._runtime_store.get_paths(
                uid=runtime_config.uid,
                account_id=runtime_config.account_id,
                slot_number=runtime_config.slot_number,
            ).__dict__,
        )

    def run_once(
        self,
        symbol: str | None = None,
        uid: str | None = None,
        account_id: int | None = None,
        alpaca_account_id: int | None = None,
        slot_number: int | None = None,
        allow_execution: bool | None = None,
        preview_only: bool = False,
        trigger_type: str = "manual",
        trigger_source: str = "api",
        test_trigger: str | None = None,
    ) -> PrimeStocksRuntimeResult:
        runtime_started_at = datetime.now(tz=UTC)
        runtime_started_perf = perf_counter()
        stage_timings_ms: dict[str, int] = {}

        def _mark_stage(stage: str, started_at: float) -> None:
            stage_timings_ms[stage] = int(round((perf_counter() - started_at) * 1000))

        def _runtime_timing_payload() -> dict[str, Any]:
            return {
                "symbol_duration_ms": int(round((perf_counter() - runtime_started_perf) * 1000)),
                "runtime_started_at": runtime_started_at.isoformat(),
                "runtime_finished_at": datetime.now(tz=UTC).isoformat(),
                "broker_context_ms": stage_timings_ms.get("broker_context_ms"),
                "market_data_ms": stage_timings_ms.get("market_data_ms"),
                "strategy_eval_ms": stage_timings_ms.get("strategy_eval_ms"),
                "broker_check_ms": stage_timings_ms.get("broker_check_ms"),
                "writeback_ms": stage_timings_ms.get("writeback_ms"),
            }

        run_id = self._runtime_store.create_run_id()
        default_runtime_config = build_default_runtime_config(self._settings)
        fallback_runtime_config = _override_symbol(
            default_runtime_config,
            symbol,
            uid=uid,
            account_id=account_id,
            alpaca_account_id=alpaca_account_id,
            slot_number=slot_number,
        )
        fallback_runtime_config = self._hydrate_scoped_slot_context(fallback_runtime_config)
        try:
            runtime_config = self._runtime_store.load_runtime_config(fallback_runtime_config)
            resolved_runtime_config = _override_runtime_selection(
                runtime_config,
                symbol=symbol,
                uid=uid,
                account_id=account_id,
                alpaca_account_id=alpaca_account_id,
                slot_number=slot_number,
            )
            if _is_ping_short_circuit_request(
                runtime_config=resolved_runtime_config,
                trigger_source=trigger_source,
                test_trigger=test_trigger,
            ):
                if resolved_runtime_config.ping_daily_heartbeat_enabled and _ping_daily_heartbeat_due(
                    self._runtime_store.load_heartbeat_record(
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    )
                ):
                    self._runtime_store.write_runtime_heartbeat(
                        run_id=run_id,
                        test_mode=True,
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    )
                return _build_validation_ping_disabled_result(
                    run_id=run_id,
                    runtime_config=resolved_runtime_config,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    firestore_paths=self._runtime_store.get_paths(
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    ).__dict__,
                )
            if _is_validation_ping_request(runtime_config=resolved_runtime_config, test_trigger=test_trigger):
                ping_runtime_config = _build_validation_ping_runtime_config(resolved_runtime_config)
                return self._run_validation_ping(
                    run_id=run_id,
                    runtime_config=ping_runtime_config,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                )
            resolved_symbol = _resolve_primary_runtime_symbol(
                resolved_runtime_config,
                explicit_symbol=symbol,
            )
            if resolved_symbol is None:
                return self._persist_blocked_runtime_result(
                    run_id=run_id,
                    runtime_config=resolved_runtime_config,
                    account_context=_build_fallback_account_context(resolved_runtime_config),
                    execution_decision="no_active_symbols_configured",
                    skipped_reason="no_active_symbols_configured",
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    message=(
                        "Prime Stocks runtime skipped because no active automation symbols are configured for the "
                        "selected account."
                    ),
                    candidate_action="BLOCKED",
                    status="blocked",
                )
            resolved_runtime_config = _override_runtime_selection(
                resolved_runtime_config,
                symbol=resolved_symbol,
                uid=uid,
                account_id=account_id,
                alpaca_account_id=alpaca_account_id,
            )
            _ensure_prime_stocks_runtime_context(resolved_runtime_config, allow_execution=allow_execution)
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked before execution because Firestore runtime control access failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return _build_runtime_store_failure_result(
                run_id=run_id,
                runtime_config=fallback_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=fallback_runtime_config.uid,
                    account_id=fallback_runtime_config.account_id,
                ).__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore runtime control data could not be loaded. "
                    f"{exc}"
                ),
            )
        if not resolved_runtime_config.enabled:
            message = "Prime Stocks runtime skipped because the runtime config is disabled."
            logger.info(message)
            return self._persist_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=_build_fallback_account_context(resolved_runtime_config),
                execution_decision="skipped",
                skipped_reason="runtime_disabled",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                message=message,
                candidate_action="DISABLED",
                status="disabled",
            )
        broker_context_started = perf_counter()
        try:
            account_context = self._account_resolver.resolve_runtime_account(resolved_runtime_config)
        except AlpacaAccountResolutionError as exc:
            logger.exception(
                "Prime Stocks runtime blocked before market-data fetch because linked Alpaca account resolution failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return self._persist_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=_build_fallback_account_context(resolved_runtime_config),
                execution_decision="linked_account_unavailable",
                skipped_reason="linked_account_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                message=(
                    "Prime Stocks runtime blocked because the selected linked Alpaca account could not be resolved. "
                    f"{exc}"
                ),
            )
        _mark_stage("broker_context_ms", broker_context_started)
        selected_slot_number = resolved_runtime_config.slot_number or account_context.slot_number
        if selected_slot_number != resolved_runtime_config.slot_number:
            resolved_runtime_config = replace(resolved_runtime_config, slot_number=selected_slot_number)
            runtime_config = self._runtime_store.load_runtime_config(resolved_runtime_config)
            resolved_runtime_config = _override_runtime_selection(
                runtime_config,
                symbol=symbol,
                uid=uid,
                account_id=account_id,
                alpaca_account_id=alpaca_account_id,
                slot_number=selected_slot_number,
            )
        try:
            account_runtime_state = self._runtime_store.load_runtime_state_record(
                uid=account_context.uid,
                account_id=resolved_runtime_config.account_id,
                slot_number=resolved_runtime_config.slot_number,
            )
            runtime_state = account_runtime_state
            if resolved_runtime_config.uid is not None and resolved_runtime_config.account_id is not None:
                symbol_runtime_state = self._runtime_store.load_runtime_symbol_state_record(
                    uid=account_context.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                    symbol=resolved_runtime_config.symbol,
                )
                processed_bar_state = symbol_runtime_state or PrimeStocksRuntimeStateRecord()
            else:
                processed_bar_state = account_runtime_state
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked after account resolution because Firestore state access failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return _build_runtime_store_failure_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore runtime state could not be loaded after linked account "
                    f"resolution. {exc}"
                ),
                )

        entitlement_failure = _resolve_runtime_entitlement_failure(
            runtime_config=resolved_runtime_config,
            account_context=account_context,
        )
        if entitlement_failure is not None:
            execution_decision, runtime_message = entitlement_failure
            strategy_result = _build_placeholder_strategy_result(
                runtime_config=resolved_runtime_config,
                status="blocked",
                message=runtime_message,
            )
            try:
                self._runtime_store.write_runtime_result(
                    run_id=run_id,
                    runtime_config=resolved_runtime_config,
                    account_context=account_context,
                    strategy_result=strategy_result,
                    candidate_action="BLOCKED",
                    latest_signal_time=None,
                    runtime_message=runtime_message,
                    execution_mode="dry-run",
                    execution_decision=execution_decision,
                    execution_result=None,
                    skipped_reason=execution_decision,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    ai_decision=None,
                    state_record=_build_runtime_state_record(
                        run_id=run_id,
                        runtime_state=runtime_state,
                        runtime_config=resolved_runtime_config,
                        account_context=account_context,
                        strategy_result=strategy_result,
                        candidate_action="BLOCKED",
                        execution_decision=execution_decision,
                        latest_signal_time=None,
                        execution_result=None,
                        advance_processed_bar=False,
                    ),
                )
            except PrimeStocksRuntimeStoreError as exc:
                logger.exception(
                    "Prime Stocks runtime completed entitlement blocking but Firestore result persistence failed "
                    "trigger_type=%s trigger_source=%s run_id=%s",
                    trigger_type,
                    trigger_source,
                    run_id,
                )
                return PrimeStocksRuntimeResult(
                    run_id=run_id,
                    mode="dry-run",
                    runtime_target=resolved_runtime_config.runtime_target,
                    product_key=resolved_runtime_config.product_key,
                    strategy_key=resolved_runtime_config.strategy_key,
                    strategy_title=resolved_runtime_config.strategy_title,
                    symbol=resolved_runtime_config.symbol,
                    asset_type=resolved_runtime_config.asset_type,
                    enabled=resolved_runtime_config.enabled,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    candidate_action="BLOCKED",
                    execution_decision=execution_decision,
                    order_status="not_submitted",
                    order_submitted=False,
                    order_id=None,
                    client_order_id=None,
                    add_tier=None,
                    execution_allowed=False,
                    skipped_reason="runtime_result_persistence_failed",
                    latest_signal_time=None,
                    ai=None,
                    status="degraded",
                    message=(
                        "Prime Stocks runtime completed entitlement blocking but Firestore result persistence failed. "
                        f"{exc}"
                    ),
                    bars_processed_execution=0,
                    bars_processed_trend=0,
                    firestore_paths=self._runtime_store.get_paths(
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    ).__dict__,
                )
            return _build_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
                message=runtime_message,
                execution_decision=execution_decision,
                skipped_reason=execution_decision,
            )

        market_data_started = perf_counter()
        try:
            bar_set = self._market_data.fetch_prime_stocks_bars(
                symbol=resolved_runtime_config.symbol,
                asset_type=resolved_runtime_config.asset_type,
                product_key=resolved_runtime_config.product_key,
                execution_timeframe=resolved_runtime_config.execution_timeframe,
                trend_timeframe=resolved_runtime_config.trend_timeframe,
                execution_limit=resolved_runtime_config.execution_bar_limit,
                trend_limit=resolved_runtime_config.trend_bar_limit,
                credential_context=account_context,
            )
        except Exception as exc:
            logger.exception(
                "Prime Stocks runtime blocked before strategy evaluation because market-data fetch failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return self._persist_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=account_context,
                execution_decision="market_data_unavailable",
                skipped_reason="market_data_unavailable",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                message=(
                    "Prime Stocks runtime blocked because Alpaca market data could not be fetched after runtime config load. "
                    f"{exc}"
                ),
            )
        _mark_stage("market_data_ms", market_data_started)
        latest_signal_time = _resolve_latest_signal_time(bar_set.execution_bars)
        if _is_stale_market_data(
            latest_signal_time=latest_signal_time,
            execution_timeframe=resolved_runtime_config.execution_timeframe,
            now=self._now_provider(),
        ):
            execution_decision = "stale_data"
            runtime_message = (
                "Prime Stocks runtime blocked because the latest Alpaca execution bar is stale for the configured "
                f"{resolved_runtime_config.execution_timeframe} timeframe."
            )
            strategy_result = _build_placeholder_strategy_result(
                runtime_config=resolved_runtime_config,
                status="blocked",
                message=runtime_message,
            )
            try:
                self._runtime_store.write_runtime_result(
                    run_id=run_id,
                    runtime_config=resolved_runtime_config,
                    account_context=account_context,
                    strategy_result=strategy_result,
                    candidate_action="BLOCKED",
                    latest_signal_time=latest_signal_time,
                    runtime_message=runtime_message,
                    execution_mode="dry-run",
                    execution_decision=execution_decision,
                    execution_result=None,
                    skipped_reason=execution_decision,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    ai_decision=None,
                    state_record=_build_runtime_state_record(
                        run_id=run_id,
                        runtime_state=runtime_state,
                        runtime_config=resolved_runtime_config,
                        account_context=account_context,
                        strategy_result=strategy_result,
                        candidate_action="BLOCKED",
                        execution_decision=execution_decision,
                        latest_signal_time=latest_signal_time,
                        execution_result=None,
                        advance_processed_bar=False,
                    ),
                )
            except PrimeStocksRuntimeStoreError as exc:
                logger.exception(
                    "Prime Stocks runtime completed stale-data blocking but Firestore result persistence failed "
                    "trigger_type=%s trigger_source=%s run_id=%s",
                    trigger_type,
                    trigger_source,
                    run_id,
                )
                return PrimeStocksRuntimeResult(
                    run_id=run_id,
                    mode="dry-run",
                    runtime_target=resolved_runtime_config.runtime_target,
                    product_key=resolved_runtime_config.product_key,
                    strategy_key=resolved_runtime_config.strategy_key,
                    strategy_title=resolved_runtime_config.strategy_title,
                    symbol=resolved_runtime_config.symbol,
                    asset_type=resolved_runtime_config.asset_type,
                    enabled=resolved_runtime_config.enabled,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    candidate_action="BLOCKED",
                    execution_decision=execution_decision,
                    order_status="not_submitted",
                    order_submitted=False,
                    order_id=None,
                    client_order_id=None,
                    add_tier=None,
                    execution_allowed=False,
                    skipped_reason="runtime_result_persistence_failed",
                    latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                    ai=None,
                    status="degraded",
                    message=f"{runtime_message} Firestore result persistence failed. {exc}",
                    bars_processed_execution=0,
                    bars_processed_trend=0,
                    firestore_paths=self._runtime_store.get_paths(
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    ).__dict__,
                )
            return _build_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
                message=runtime_message,
                latest_signal_time=latest_signal_time,
                execution_decision=execution_decision,
                skipped_reason=execution_decision,
            )
        try:
            ai_decision = merge_ai_cache_records(
                market_record=self._runtime_store.load_ai_market_record(),
                symbol_record=self._runtime_store.load_ai_symbol_record(resolved_runtime_config.symbol),
                max_age_minutes=self._settings.ai_cache_max_age_minutes,
                now=self._now_provider(),
            )
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked after market-data fetch because AI cache access failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return _build_runtime_store_failure_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore AI cache could not be loaded after market data "
                    f"was fetched. {exc}"
                ),
                latest_signal_time=latest_signal_time,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
            )
        if ai_decision.is_stale:
            logger.warning(
                "Prime Stocks runtime continuing with advisory stale AI cache "
                "trigger_type=%s trigger_source=%s run_id=%s symbol=%s",
                trigger_type,
                trigger_source,
                run_id,
                resolved_runtime_config.symbol,
            )
            ai_decision = replace(
                ai_decision,
                Ai_execution_allowed=True,
                Ai_block_new_entries=False,
                Ai_block_adds=False,
                Ai_blocked_reason=None,
            )
        strategy_eval_started = perf_counter()
        try:
            strategy_result = self._strategy_runner(
                strategy_input=BismillahTrobotStocksV1Input(
                    execution_bars=bar_set.execution_bars,
                    htf_bars=bar_set.trend_bars,
                    symbol=resolved_runtime_config.symbol,
                    asset_type=resolved_runtime_config.asset_type,
                ),
                config=_build_strategy_config(resolved_runtime_config),
                ai_decision=ai_decision,
                initial_state=(
                    processed_bar_state.to_strategy_state()
                    if processed_bar_state is not runtime_state
                    else runtime_state.to_strategy_state()
                ),
            )
        except Exception as exc:
            logger.exception(
                "Prime Stocks runtime failed during strategy evaluation trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return self._persist_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=account_context,
                execution_decision="unexpected_runtime_exception",
                skipped_reason="unexpected_runtime_exception",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                message=f"Prime Stocks runtime failed during strategy evaluation. {exc}",
                latest_signal_time=latest_signal_time,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
            )
        _mark_stage("strategy_eval_ms", strategy_eval_started)
        original_candidate_action = _resolve_candidate_action(strategy_result)
        candidate_action = _resolve_forced_candidate_action(
            candidate_action=original_candidate_action,
            runtime_config=resolved_runtime_config,
            settings=self._settings,
        )
        signal_score = getattr(strategy_result, "signal_score", None)
        logger.info(
            "Prime Stocks trigger created symbol=%s candidate_action=%s signal_score=%s base_entry_signal=%s base_entry_trigger=%s strategy_status=%s strategy_message=%s latest_signal_time=%s run_id=%s",
            resolved_runtime_config.symbol,
            candidate_action,
            signal_score,
            getattr(strategy_result.latest_signal, "base_entry_signal", None),
            getattr(strategy_result.latest_signal, "base_entry_trigger", None),
            getattr(strategy_result, "status", None),
            getattr(strategy_result, "message", None),
            None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            run_id,
        )
        if candidate_action != original_candidate_action:
            logger.warning(
                "Prime Stocks runtime force-candidate override applied original_candidate_action=%s forced_candidate_action=%s "
                "trigger_type=%s trigger_source=%s run_id=%s",
                original_candidate_action,
                candidate_action,
                trigger_type,
                trigger_source,
                run_id,
            )
        has_no_new_closed_bar = _has_no_new_closed_bar(
            runtime_state=processed_bar_state,
            latest_signal_time=latest_signal_time,
        )
        actionable_buy_candidate = candidate_action == "FirstLot" or candidate_action.startswith("MULTI-")
        if has_no_new_closed_bar and not actionable_buy_candidate:
            logger.info(
                "Prime Stocks continuing with existing closed bar for %s so status and exit monitoring remain fresh "
                "trigger_type=%s trigger_source=%s run_id=%s candidate_action=%s",
                resolved_runtime_config.symbol,
                trigger_type,
                trigger_source,
                run_id,
                candidate_action,
            )
        if has_no_new_closed_bar and actionable_buy_candidate:
            logger.info(
                "Prime Stocks continuing past no_new_bar because candidate_action=%s is actionable trigger_type=%s trigger_source=%s run_id=%s",
                candidate_action,
                trigger_type,
                trigger_source,
                run_id,
            )
        ai_blocked_decision = _resolve_ai_blocked_candidate_action(candidate_action=candidate_action, ai_decision=ai_decision)
        if _ai_validation_bypass_enabled(resolved_runtime_config) and ai_blocked_decision is not None:
            logger.warning(
                "Prime Stocks runtime validation-only AI bypass ignored blocked candidate_action=%s trigger_type=%s trigger_source=%s run_id=%s",
                candidate_action,
                trigger_type,
                trigger_source,
                run_id,
            )
            ai_blocked_decision = None
            strategy_result = replace(
                strategy_result,
                status="signal" if candidate_action != "HOLD" else "no_op",
                message=f"{strategy_result.message} Admin validation-only AI bypass was active.",
            )
        if strategy_result.status == "blocked" or ai_blocked_decision is not None:
            blocked_decision = (
                ai_blocked_decision
                or (
                    "ai_blocked"
                    if strategy_result.ai_decision is None or strategy_result.ai_decision.Ai_blocked_reason is None
                    else strategy_result.ai_decision.Ai_blocked_reason
                )
            )
            strategy_reasoning = _build_strategy_reasoning(
                strategy_result=strategy_result,
                candidate_action="AI_BLOCKED",
                execution_decision=blocked_decision,
                ai_decision=ai_decision,
            )
            runtime_message = _build_runtime_message(execution_mode="dry-run", execution_decision=blocked_decision)
            try:
                self._runtime_store.write_runtime_result(
                    run_id=run_id,
                    runtime_config=resolved_runtime_config,
                    account_context=account_context,
                    strategy_result=strategy_result,
                    candidate_action="AI_BLOCKED",
                    latest_signal_time=latest_signal_time,
                    runtime_message=runtime_message,
                    execution_mode="dry-run",
                    execution_decision=blocked_decision,
                    execution_result=None,
                    skipped_reason=blocked_decision,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    ai_decision=ai_decision,
                    state_record=_build_runtime_state_record(
                        run_id=run_id,
                        runtime_state=runtime_state,
                        runtime_config=resolved_runtime_config,
                        account_context=account_context,
                        strategy_result=strategy_result,
                        candidate_action="AI_BLOCKED",
                        execution_decision=blocked_decision,
                        latest_signal_time=latest_signal_time,
                        execution_result=None,
                        advance_processed_bar=True,
                    ),
                )
            except PrimeStocksRuntimeStoreError as exc:
                logger.exception(
                    "Prime Stocks runtime completed AI blocking but Firestore result persistence failed "
                    "trigger_type=%s trigger_source=%s run_id=%s",
                    trigger_type,
                    trigger_source,
                    run_id,
                )
                return PrimeStocksRuntimeResult(
                    run_id=run_id,
                    mode="dry-run",
                    runtime_target=resolved_runtime_config.runtime_target,
                    product_key=resolved_runtime_config.product_key,
                    strategy_key=resolved_runtime_config.strategy_key,
                    strategy_title=resolved_runtime_config.strategy_title,
                    symbol=resolved_runtime_config.symbol,
                    asset_type=resolved_runtime_config.asset_type,
                    enabled=resolved_runtime_config.enabled,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    candidate_action="AI_BLOCKED",
                    execution_decision=blocked_decision,
                    order_status="not_submitted",
                    order_submitted=False,
                    order_id=None,
                    client_order_id=None,
                    add_tier=None,
                    execution_allowed=False,
                    skipped_reason="runtime_result_persistence_failed",
                    latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                    ai=serialize_ai_decision(ai_decision),
                    strategy_reasoning=strategy_reasoning,
                    status="degraded",
                    message=(
                        "Prime Stocks runtime completed AI blocking but Firestore result persistence failed. "
                        f"{exc}"
                    ),
                    bars_processed_execution=len(bar_set.execution_bars),
                    bars_processed_trend=len(bar_set.trend_bars),
                    firestore_paths=self._runtime_store.get_paths(
                        uid=resolved_runtime_config.uid,
                        account_id=resolved_runtime_config.account_id,
                        slot_number=resolved_runtime_config.slot_number,
                    ).__dict__,
                )
            return PrimeStocksRuntimeResult(
                run_id=run_id,
                mode="dry-run",
                runtime_target=resolved_runtime_config.runtime_target,
                product_key=resolved_runtime_config.product_key,
                strategy_key=resolved_runtime_config.strategy_key,
                strategy_title=resolved_runtime_config.strategy_title,
                symbol=resolved_runtime_config.symbol,
                asset_type=resolved_runtime_config.asset_type,
                enabled=resolved_runtime_config.enabled,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                candidate_action="AI_BLOCKED",
                execution_decision=blocked_decision,
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason=blocked_decision,
                latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                ai=serialize_ai_decision(ai_decision),
                strategy_reasoning=strategy_reasoning,
                status="blocked",
                message=runtime_message,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
            )
        try:
            latest_execution = self._runtime_store.load_latest_execution_record(
                uid=account_context.uid,
                account_id=resolved_runtime_config.account_id,
                slot_number=resolved_runtime_config.slot_number,
            )
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked after strategy evaluation because Firestore execution state access failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return _build_runtime_store_failure_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore execution state could not be loaded after strategy "
                    f"evaluation. {exc}"
                ),
                latest_signal_time=latest_signal_time,
                candidate_action=candidate_action,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
        )
        strategy_config = _build_strategy_config(resolved_runtime_config)
        broker_check_started = perf_counter()
        try:
            (
                execution_result,
                execution_decision,
                skipped_reason,
                execution_allowed,
                retry_count,
                broker_error_code,
                broker_error_message,
                current_total_exposure_pct,
            ) = self._execute_candidate_action(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                strategy_config=strategy_config,
                strategy_result=strategy_result,
                account_context=account_context,
                candidate_action=candidate_action,
                latest_signal_time=latest_signal_time,
                latest_execution_bar=bar_set.execution_bars[-1] if bar_set.execution_bars else None,
                latest_execution=latest_execution,
                allow_execution=allow_execution,
                preview_only=preview_only,
            )
            if execution_result is not None and execution_result.action == "take_profit":
                candidate_action = "take_profit"
            elif skipped_reason == "tp_high_touched_not_executable":
                candidate_action = "HOLD"
        except Exception as exc:
            logger.exception(
                "Prime Stocks runtime failed during candidate execution trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return self._persist_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=account_context,
                execution_decision="unexpected_runtime_exception",
                skipped_reason="unexpected_runtime_exception",
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                message=f"Prime Stocks runtime failed during candidate execution. {exc}",
                candidate_action=candidate_action,
                latest_signal_time=latest_signal_time,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
                ai_decision=ai_decision,
            )
        _mark_stage("broker_check_ms", broker_check_started)
        runtime_message = _build_runtime_message(
            execution_mode=_resolve_mode(
                resolved_runtime_config,
                allow_execution=allow_execution,
                account_context=account_context,
                settings=self._settings,
            ),
            execution_decision=execution_decision,
        )
        writeback_started = perf_counter()
        try:
            self._runtime_store.write_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                account_context=account_context,
                strategy_result=strategy_result,
                candidate_action=candidate_action,
                latest_signal_time=latest_signal_time,
                runtime_message=runtime_message,
                execution_mode=_resolve_mode(
                    resolved_runtime_config,
                    allow_execution=allow_execution,
                    account_context=account_context,
                    settings=self._settings,
                ),
                execution_decision=execution_decision,
                execution_result=execution_result,
                skipped_reason=skipped_reason,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                ai_decision=ai_decision,
                retry_count=retry_count,
                broker_error_code=broker_error_code,
                broker_error_message=broker_error_message,
                state_record=_build_runtime_state_record(
                    run_id=run_id,
                    runtime_state=runtime_state,
                    runtime_config=resolved_runtime_config,
                    account_context=account_context,
                    strategy_result=strategy_result,
                    candidate_action=candidate_action,
                    execution_decision=execution_decision,
                    latest_signal_time=latest_signal_time,
                    execution_result=execution_result,
                    advance_processed_bar=True,
                    current_total_exposure_pct=current_total_exposure_pct,
                ),
            )
            _mark_stage("writeback_ms", writeback_started)
        except PrimeStocksRuntimeStoreError as exc:
            _mark_stage("writeback_ms", writeback_started)
            logger.exception(
                "Prime Stocks runtime completed execution but Firestore result persistence failed "
                "trigger_type=%s trigger_source=%s run_id=%s",
                trigger_type,
                trigger_source,
                run_id,
            )
            return PrimeStocksRuntimeResult(
                run_id=run_id,
                mode=_resolve_mode(
                    resolved_runtime_config,
                    allow_execution=allow_execution,
                    account_context=account_context,
                    settings=self._settings,
                ),
                runtime_target=resolved_runtime_config.runtime_target,
                product_key=resolved_runtime_config.product_key,
                strategy_key=resolved_runtime_config.strategy_key,
                strategy_title=resolved_runtime_config.strategy_title,
                symbol=resolved_runtime_config.symbol,
                asset_type=resolved_runtime_config.asset_type,
                enabled=resolved_runtime_config.enabled,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                candidate_action=candidate_action,
                execution_decision=execution_decision,
                order_status=execution_result.order_status if execution_result is not None else "not_submitted",
                order_submitted=execution_result.submitted if execution_result is not None else False,
                order_id=execution_result.order_id if execution_result is not None else None,
                client_order_id=execution_result.client_order_id if execution_result is not None else None,
                add_tier=execution_result.add_tier if execution_result is not None else _parse_add_tier(candidate_action),
                execution_allowed=execution_allowed,
                skipped_reason="runtime_result_persistence_failed",
                latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                ai=serialize_ai_decision(ai_decision),
                strategy_reasoning=_build_strategy_reasoning(
                    strategy_result=strategy_result,
                    candidate_action=candidate_action,
                    execution_decision=execution_decision,
                    ai_decision=ai_decision,
                ),
                signal_score=signal_score,
                timing=_runtime_timing_payload(),
                status="degraded",
                message=(
                    "Prime Stocks runtime reached execution but Firestore result persistence failed. "
                    f"{exc}"
                ),
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
                firestore_paths=self._runtime_store.get_paths(
                    uid=resolved_runtime_config.uid,
                    account_id=resolved_runtime_config.account_id,
                    slot_number=resolved_runtime_config.slot_number,
                ).__dict__,
            )
        result = PrimeStocksRuntimeResult(
            run_id=run_id,
            mode=_resolve_mode(
                resolved_runtime_config,
                allow_execution=allow_execution,
                account_context=account_context,
                settings=self._settings,
            ),
            runtime_target=resolved_runtime_config.runtime_target,
            product_key=resolved_runtime_config.product_key,
            strategy_key=resolved_runtime_config.strategy_key,
            strategy_title=resolved_runtime_config.strategy_title,
            symbol=resolved_runtime_config.symbol,
            asset_type=resolved_runtime_config.asset_type,
            enabled=resolved_runtime_config.enabled,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            candidate_action=candidate_action,
            execution_decision=execution_decision,
            order_status=execution_result.order_status if execution_result is not None else "not_submitted",
            order_submitted=execution_result.submitted if execution_result is not None else False,
            order_id=execution_result.order_id if execution_result is not None else None,
            client_order_id=execution_result.client_order_id if execution_result is not None else None,
            add_tier=execution_result.add_tier if execution_result is not None else _parse_add_tier(candidate_action),
            execution_allowed=execution_allowed,
            skipped_reason=skipped_reason if execution_result is None else execution_result.skipped_reason,
            latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            ai=serialize_ai_decision(ai_decision),
            strategy_reasoning=_build_strategy_reasoning(
                strategy_result=strategy_result,
                candidate_action=candidate_action,
                execution_decision=execution_decision,
                ai_decision=ai_decision,
            ),
            signal_score=signal_score,
            timing=_runtime_timing_payload(),
            status=strategy_result.status,
            message=runtime_message,
            bars_processed_execution=len(bar_set.execution_bars),
            bars_processed_trend=len(bar_set.trend_bars),
            firestore_paths=self._runtime_store.get_paths(
                uid=resolved_runtime_config.uid,
                account_id=resolved_runtime_config.account_id,
                slot_number=resolved_runtime_config.slot_number,
            ).__dict__,
        )
        logger.info(
            "Prime Stocks runtime completed for %s with candidate_action=%s execution_decision=%s trigger_type=%s trigger_source=%s run_id=%s",
            resolved_runtime_config.symbol,
            candidate_action,
            execution_decision,
            trigger_type,
            trigger_source,
            run_id,
        )
        return result

    def _execute_candidate_action(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        strategy_config: BismillahTrobotStocksV1Config,
        strategy_result,
        account_context: ResolvedAlpacaAccountContext,
        candidate_action: str,
        latest_signal_time: datetime | None,
        latest_execution_bar: Any | None,
        latest_execution: PrimeStocksLatestExecutionRecord,
        allow_execution: bool | None,
        preview_only: bool = False,
    ) -> tuple[AlpacaPaperExecutionResult | None, str, str | None, bool, int, str | None, str | None, float | None]:
        logger.info(
            "Prime Stocks executor called symbol=%s candidate_action=%s execution_mode=%s latest_signal_time=%s",
            runtime_config.symbol,
            candidate_action,
            _resolve_mode(
                runtime_config,
                allow_execution=allow_execution,
                account_context=account_context,
                settings=self._settings,
            ),
            None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
        )
        execution_mode = _resolve_mode(
            runtime_config,
            allow_execution=allow_execution,
            account_context=account_context,
            settings=self._settings,
        )
        if candidate_action in PRIME_DIAGNOSTIC_ACTIONS:
            return None, "prime_diagnostic_review", _prime_diagnostic_reason(candidate_action) or "prime_review", False, 0, None, None, None
        if preview_only and (candidate_action == "FirstLot" or candidate_action.startswith("MULTI-")):
            return None, "preview_only", "preview_only", False, 0, None, None, None
        if execution_mode == "dry-run":
            return None, "dry_run_only", "paper_execution_disabled", False, 0, None, None, None
        if candidate_action in {"EXIT_ATR", "EXIT_REGIME"}:
            logger.warning(
                "Prime Stocks non-TP close blocked before broker submission symbol=%s candidate_action=%s",
                runtime_config.symbol,
                candidate_action,
            )
            return None, "prime_non_tp_exit_blocked", "prime_non_tp_exit_blocked", False, 0, None, None, None
        if not account_context.trade_enabled:
            return None, "credential_trade_access_missing", "credential_trade_access_missing", False, 0, None, None, None
        if not account_context.environment.strip():
            return None, "account_environment_missing", "account_environment_missing", False, 0, None, None, None
        pre_broker_execution_key = _build_execution_key(candidate_action, latest_signal_time, symbol=runtime_config.symbol)
        pre_broker_latest_signal_time_value = (
            latest_signal_time.astimezone(UTC).isoformat()
            if latest_signal_time is not None
            else None
        )
        if candidate_action == "FirstLot" or candidate_action.startswith("MULTI-"):
            if latest_execution.execution_key == pre_broker_execution_key and latest_execution.order_status in ACTIVE_ORDER_STATUSES:
                return None, "skipped_duplicate", "duplicate_candidate_action", False, 0, None, None, None
            if (
                latest_execution.latest_signal_time == pre_broker_latest_signal_time_value
                and latest_execution.order_status in ACTIVE_ORDER_STATUSES
                and latest_execution.candidate_action not in {None, candidate_action}
            ):
                return None, "conflicting_same_bar_execution", "conflicting_same_bar_execution", False, 0, None, None, None

        try:
            broker_state, state_retry_count = self._call_broker_with_retry(
                operation=lambda: self._paper_trading.get_submission_state(
                    symbol=runtime_config.symbol,
                    credential_context=account_context,
                ),
                can_retry=True,
                max_retries=runtime_config.broker_retry_max_attempts,
                operation_label="load submission state",
            )
        except AlpacaPaperTradingError as exc:
            return None, exc.code, exc.code, False, int(getattr(exc, "retry_count", 0)), exc.code, exc.message, None

        current_total_exposure_pct = _resolve_total_exposure_pct(broker_state=broker_state)
        if _position_is_residual(broker_state.position):
            try:
                recent_orders = self._paper_trading.list_recent_orders(credential_context=account_context)
            except Exception as exc:
                logger.warning("Prime Stocks residual cleanup skipped because recent orders could not be loaded symbol=%s error=%s", runtime_config.symbol, exc)
                return None, "residual_cleanup_history_unavailable", "residual_cleanup_history_unavailable", False, state_retry_count, "residual_cleanup_history_unavailable", "Prime residual cleanup requires recent Bismel1 close history.", current_total_exposure_pct
            if _has_pending_residual_cleanup_order(recent_orders=recent_orders, symbol=runtime_config.symbol, prefix="prime-cleanup-"):
                return None, "skipped_residual_cleanup_pending", "residual_cleanup_pending", False, state_retry_count, None, None, current_total_exposure_pct
            if _residual_cleanup_attempt_count(recent_orders=recent_orders, symbol=runtime_config.symbol, prefix="prime-cleanup-") >= RESIDUAL_CLEANUP_MAX_ATTEMPTS:
                return None, "residual_cleanup_attempt_limit", "residual_cleanup_attempt_limit", False, state_retry_count, "residual_cleanup_attempt_limit", "Prime residual cleanup skipped because the cleanup attempt limit was reached.", current_total_exposure_pct
            if _has_recent_residual_cleanup_attempt(recent_orders=recent_orders, symbol=runtime_config.symbol, prefix="prime-cleanup-", now=datetime.now(UTC)):
                return None, "residual_cleanup_cooldown", "residual_cleanup_cooldown", False, state_retry_count, "residual_cleanup_cooldown", "Prime residual cleanup skipped because a cleanup attempt was made recently.", current_total_exposure_pct
            if not _has_recent_bismel1_full_close(recent_orders=recent_orders, symbol=runtime_config.symbol, prefixes=("prime-tp-", "prime-close-", "prime-cleanup-", "bismel1-close-")):
                return None, "residual_cleanup_not_eligible", "missing_bismel1_close_history", False, state_retry_count, None, None, current_total_exposure_pct
            if not bool(getattr(broker_state.asset, "tradable", False)):
                return None, "residual_cleanup_not_tradable", "residual_cleanup_not_tradable", False, state_retry_count, "residual_cleanup_not_tradable", "Prime residual cleanup skipped because the symbol is not tradable.", current_total_exposure_pct
            position_qty = _maybe_float(getattr(broker_state.position, "qty", None))
            if position_qty is None or position_qty <= 0:
                return None, "residual_cleanup_position_unavailable", "residual_cleanup_position_unavailable", False, state_retry_count, "residual_cleanup_position_unavailable", "Prime residual cleanup requires a broker-reported residual position.", current_total_exposure_pct
            client_order_id = f"prime-cleanup-{uuid4().hex[:15]}"
            try:
                logger.info(
                    "Prime Stocks residual cleanup submit attempted symbol=%s qty=%s client_order_id=%s",
                    runtime_config.symbol,
                    _format_qty(position_qty),
                    client_order_id,
                )
                result, submit_retry_count = self._call_broker_with_retry(
                    operation=lambda: self._paper_trading.submit_market_order_qty(
                        symbol=runtime_config.symbol,
                        side="sell",
                        qty=position_qty,
                        action="fractional_residual_cleanup",
                        client_order_id=client_order_id,
                        credential_context=account_context,
                    ),
                    can_retry=False,
                    max_retries=0,
                    operation_label="submit residual cleanup",
                )
            except AlpacaPaperTradingError as exc:
                return None, exc.code, exc.code, False, state_retry_count + int(getattr(exc, "retry_count", 0)), exc.code, exc.message, current_total_exposure_pct
            raw_response = result.raw_response if isinstance(result.raw_response, dict) else {}
            result = replace(
                result,
                retry_count=state_retry_count + submit_retry_count,
                raw_response={
                    **raw_response,
                    "request_action": "close",
                    "close_reason": "fractional_residual_cleanup",
                    "close_scope": "residual_cleanup",
                    "broker_position_qty_source": "broker_current_position",
                    "requested_close_qty": _format_qty(position_qty),
                    "source": "runtime_reconciliation",
                    "broker_residual_open": True,
                    "below_residual_threshold": True,
                    "strategy_manage": False,
                    "cleanup_required": True,
                },
            )
            return result, "submitted_residual_cleanup", None, True, state_retry_count + submit_retry_count, None, None, current_total_exposure_pct
        tp_candidate = _resolve_prime_take_profit_candidate(
            runtime_config=runtime_config,
            strategy_result=strategy_result,
            broker_state=broker_state,
            latest_signal_time=latest_signal_time,
            latest_execution_bar=latest_execution_bar,
        )
        take_profit_skip_reason = "tp_not_reached" if broker_state.position is not None else "no_action_candidate"
        if tp_candidate is not None:
            candidate_action = "take_profit"
            logger.info(
                "Prime Stocks take-profit candidate confirmed symbol=%s source=%s price=%s threshold=%s",
                runtime_config.symbol,
                tp_candidate["market_confirmation_source"],
                tp_candidate["market_confirmation_price"],
                tp_candidate["tp_threshold"],
            )
        elif broker_state.position is not None and _prime_take_profit_high_touched_without_executable_confirmation(
            runtime_config=runtime_config,
            strategy_result=strategy_result,
            latest_execution_bar=latest_execution_bar,
            position=broker_state.position,
        ):
            candidate_action = "HOLD"
            take_profit_skip_reason = "tp_high_touched_not_executable"
            logger.info(
                "Prime Stocks take-profit high touched but executable confirmation missing symbol=%s",
                runtime_config.symbol,
            )

        if candidate_action not in {"FirstLot", "EXIT_ATR", "EXIT_REGIME", "take_profit"} and not candidate_action.startswith("MULTI-"):
            return None, "no_op", take_profit_skip_reason, False, state_retry_count, None, None, current_total_exposure_pct

        execution_key = _build_execution_key(candidate_action, latest_signal_time, symbol=runtime_config.symbol)
        if latest_execution.execution_key == execution_key and latest_execution.order_status in ACTIVE_ORDER_STATUSES:
            return None, "skipped_duplicate", "duplicate_candidate_action", False, state_retry_count, None, None, current_total_exposure_pct
        if (
            latest_execution.latest_signal_time == pre_broker_latest_signal_time_value
            and latest_execution.order_status in ACTIVE_ORDER_STATUSES
            and latest_execution.candidate_action not in {None, candidate_action}
        ):
            return None, "conflicting_same_bar_execution", "conflicting_same_bar_execution", False, state_retry_count, None, None, current_total_exposure_pct

        if candidate_action == "FirstLot":
            duplicate_failure = self._resolve_first_lot_duplicate_failure(
                runtime_config=runtime_config,
                latest_signal_time=latest_signal_time,
                latest_execution=latest_execution,
                account_context=account_context,
            )
            if duplicate_failure is not None:
                return (
                    None,
                    duplicate_failure.execution_decision,
                    duplicate_failure.skipped_reason,
                    duplicate_failure.execution_allowed,
                    duplicate_failure.retry_count,
                    duplicate_failure.broker_error_code,
                    duplicate_failure.broker_error_message,
                    current_total_exposure_pct,
                )

        guard_failure = self._evaluate_submission_guards(
            runtime_config=runtime_config,
            strategy_config=strategy_config,
            strategy_result=strategy_result,
            account_context=account_context,
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            latest_execution=latest_execution,
            broker_state=broker_state,
        )
        if guard_failure is not None:
            return (
                None,
                guard_failure.execution_decision,
                guard_failure.skipped_reason,
                guard_failure.execution_allowed,
                guard_failure.retry_count,
                guard_failure.broker_error_code,
                guard_failure.broker_error_message,
                current_total_exposure_pct,
            )

        client_order_id = _build_client_order_id(run_id=run_id, candidate_action=candidate_action)
        requested_notional = _resolve_effective_notional(
            candidate_action=candidate_action,
            runtime_config=runtime_config,
            strategy_config=strategy_config,
            account_equity=broker_state.account.equity,
        )
        if candidate_action == "FirstLot":
            try:
                logger.info(
                    "Prime Stocks broker submit attempted symbol=%s candidate_action=%s order_type=first_lot_buy notional=%s client_order_id=%s",
                    runtime_config.symbol,
                    candidate_action,
                    requested_notional or runtime_config.first_lot_notional,
                    client_order_id,
                )
                result, submit_retry_count = self._call_broker_with_retry(
                    operation=lambda: self._paper_trading.submit_first_lot_buy(
                        symbol=runtime_config.symbol,
                        asset_type=runtime_config.asset_type,
                        product_key=runtime_config.product_key,
                        notional=requested_notional or runtime_config.first_lot_notional,
                        client_order_id=client_order_id,
                        credential_context=account_context,
                    ),
                    can_retry=True,
                    max_retries=runtime_config.broker_retry_max_attempts,
                    operation_label="submit first lot buy",
                )
            except AlpacaPaperTradingError as exc:
                return None, exc.code, exc.code, False, state_retry_count + int(getattr(exc, "retry_count", 0)), exc.code, exc.message, current_total_exposure_pct
            total_retry_count = state_retry_count + submit_retry_count
            result = replace(result, retry_count=total_retry_count)
            invalid_execution_result = _validate_successful_execution_result(result=result, candidate_action=candidate_action)
            if invalid_execution_result is not None:
                return None, invalid_execution_result.code, invalid_execution_result.code, False, total_retry_count, invalid_execution_result.code, invalid_execution_result.message, current_total_exposure_pct
            return result, "submitted_buy", None, True, total_retry_count, None, None, _resolve_total_exposure_pct(
                broker_state=broker_state,
                additional_notional=result.notional,
            )
        if candidate_action.startswith("MULTI-"):
            add_tier = _parse_add_tier(candidate_action)
            try:
                logger.info(
                    "Prime Stocks broker submit attempted symbol=%s candidate_action=%s order_type=multi_buy add_tier=%s notional=%s client_order_id=%s",
                    runtime_config.symbol,
                    candidate_action,
                    add_tier,
                    requested_notional or 0.0,
                    client_order_id,
                )
                result, submit_retry_count = self._call_broker_with_retry(
                    operation=lambda: self._paper_trading.submit_multi_buy(
                        symbol=runtime_config.symbol,
                        asset_type=runtime_config.asset_type,
                        product_key=runtime_config.product_key,
                        notional=requested_notional or 0.0,
                        client_order_id=client_order_id,
                        action=candidate_action,
                        add_tier=add_tier,
                        credential_context=account_context,
                    ),
                    can_retry=True,
                    max_retries=runtime_config.broker_retry_max_attempts,
                    operation_label="submit recovery add",
                )
            except AlpacaPaperTradingError as exc:
                return None, exc.code, exc.code, False, state_retry_count + int(getattr(exc, "retry_count", 0)), exc.code, exc.message, current_total_exposure_pct
            total_retry_count = state_retry_count + submit_retry_count
            result = replace(result, retry_count=total_retry_count)
            invalid_execution_result = _validate_successful_execution_result(result=result, candidate_action=candidate_action)
            if invalid_execution_result is not None:
                return None, invalid_execution_result.code, invalid_execution_result.code, False, total_retry_count, invalid_execution_result.code, invalid_execution_result.message, current_total_exposure_pct
            return result, "submitted_buy", None, True, total_retry_count, None, None, _resolve_total_exposure_pct(
                broker_state=broker_state,
                additional_notional=result.notional,
            )
        close_guard = _validate_prime_close_order_metadata(
            candidate_action=candidate_action,
            client_order_id=client_order_id,
        )
        if close_guard is not None:
            logger.warning(
                "Prime Stocks close blocked by final guard symbol=%s candidate_action=%s blocked_reason=%s",
                runtime_config.symbol,
                candidate_action,
                close_guard,
            )
            return None, close_guard, close_guard, False, state_retry_count, close_guard, "Prime Stocks close blocked before broker submission by final product close guard.", current_total_exposure_pct
        position_qty = _maybe_float(getattr(broker_state.position, "qty", None)) if _position_has_broker_quantity(broker_state.position) else None
        if position_qty is None or position_qty <= 0:
            return None, "take_profit_position_unavailable", "take_profit_position_unavailable", False, state_retry_count, "take_profit_position_unavailable", "Prime Stocks take-profit close requires a current broker position.", current_total_exposure_pct
        try:
            logger.info(
                "Prime Stocks broker submit attempted symbol=%s candidate_action=%s order_type=take_profit_market_sell client_order_id=%s",
                runtime_config.symbol,
                candidate_action,
                client_order_id,
            )
            result, submit_retry_count = self._call_broker_with_retry(
                operation=lambda: self._paper_trading.submit_market_order_qty(
                    symbol=runtime_config.symbol,
                    side="sell",
                    qty=position_qty,
                    action=candidate_action,
                    client_order_id=client_order_id,
                    credential_context=account_context,
                ),
                can_retry=False,
                max_retries=0,
                operation_label="submit take-profit close",
            )
        except AlpacaPaperTradingError as exc:
            return None, exc.code, exc.code, False, state_retry_count + int(getattr(exc, "retry_count", 0)), exc.code, exc.message, current_total_exposure_pct
        total_retry_count = state_retry_count + submit_retry_count
        if tp_candidate is not None:
            raw_response = result.raw_response if isinstance(result.raw_response, dict) else {}
            result = replace(
                result,
                raw_response={
                    **raw_response,
                    "request_action": "close",
                    "close_reason": "take_profit",
                    "close_scope": "full_position",
                    "broker_position_qty_source": "broker_current_position",
                    "requested_close_qty": _format_qty(position_qty),
                    "market_confirmation": tp_candidate,
                    "bismel1_market_confirmation": tp_candidate,
                },
            )
        result = replace(result, retry_count=total_retry_count)
        invalid_execution_result = _validate_successful_execution_result(result=result, candidate_action=candidate_action)
        if invalid_execution_result is not None:
            return None, invalid_execution_result.code, invalid_execution_result.code, False, total_retry_count, invalid_execution_result.code, invalid_execution_result.message, current_total_exposure_pct
        self._maybe_record_closed_trade_performance(
            runtime_config=runtime_config,
            account_context=account_context,
            strategy_result=strategy_result,
            broker_state=broker_state,
            execution_result=result,
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            run_id=run_id,
        )
        return result, "submitted_exit", None, True, total_retry_count, None, None, current_total_exposure_pct

    def _call_broker_with_retry(
        self,
        *,
        operation,
        can_retry: bool,
        max_retries: int,
        operation_label: str,
    ):
        retry_count = 0
        while True:
            try:
                return operation(), retry_count
            except AlpacaPaperTradingError as exc:
                setattr(exc, "retry_count", retry_count)
                if (not can_retry) or (not exc.retryable) or retry_count >= max_retries:
                    raise
                retry_count += 1
                logger.warning(
                    "Prime Stocks broker retry scheduled operation=%s retry_count=%s code=%s",
                    operation_label,
                    retry_count,
                    exc.code,
                )

    def _maybe_record_closed_trade_performance(
        self,
        *,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        account_context: ResolvedAlpacaAccountContext,
        strategy_result: PrimeStocksStrategyResult,
        broker_state: AlpacaPaperSubmissionState,
        execution_result: AlpacaPaperExecutionResult,
        candidate_action: str,
        latest_signal_time: datetime | None,
        run_id: str,
    ) -> None:
        if candidate_action != "take_profit":
            return

        if execution_result is None or not execution_result.submitted:
            return

        if str(execution_result.order_status or "").strip().lower() not in {"accepted", "new", "partially_filled", "filled", "submitted"}:
            return

        exit_price = _extract_order_fill_price(execution_result.raw_response)
        if exit_price is None or exit_price <= 0:
            logger.info(
                "Prime Stocks performance tracking skipped because no filled exit price was available symbol=%s order_status=%s",
                runtime_config.symbol,
                execution_result.order_status,
            )
            return

        position = broker_state.position
        if not _position_is_strategy_managed_open(position):
            logger.info(
                "Prime Stocks performance tracking skipped because no broker position was available symbol=%s",
                runtime_config.symbol,
            )
            return

        latest_bar = strategy_result.latest_bar
        state_before = None if latest_bar is None else latest_bar.state_before
        entry_price = _maybe_float(getattr(state_before, "position_avg_price", None))
        if entry_price is None or entry_price <= 0:
            entry_price = _maybe_float(getattr(strategy_result.final_state, "position_avg_price", None))
        if entry_price is None or entry_price <= 0:
            logger.info(
                "Prime Stocks performance tracking skipped because the entry price could not be derived symbol=%s",
                runtime_config.symbol,
            )
            return

        qty = abs(float(position.qty))
        entry_notional = qty * float(entry_price)
        exit_notional = qty * float(exit_price)
        pnl_dollars = (float(exit_price) - float(entry_price)) * qty
        pnl_percent = (pnl_dollars / entry_notional * 100.0) if entry_notional > 0 else 0.0
        if position.side is not None and str(position.side).strip().lower() == "short":
            pnl_dollars *= -1.0
            pnl_percent *= -1.0

        trade_id = (
            execution_result.order_id
            or execution_result.client_order_id
            or _build_execution_key(candidate_action, latest_signal_time, symbol=runtime_config.symbol)
        )

        trade_record = {
            "uid": account_context.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "product_key": runtime_config.product_key,
            "strategy_key": runtime_config.strategy_key,
            "symbol": runtime_config.symbol.upper(),
            "trade_id": trade_id,
            "direction": "long" if str(position.side or "long").strip().lower() != "short" else "short",
            "side": "sell",
            "qty": qty,
            "entry_price": float(entry_price),
            "exit_price": float(exit_price),
            "entry_notional": round(entry_notional, 2),
            "exit_notional": round(exit_notional, 2),
            "realized_pnl_dollars": round(pnl_dollars, 2),
            "realized_pnl_percent": round(pnl_percent, 2),
            "trade_outcome": "win" if pnl_dollars > 0 else "loss" if pnl_dollars < 0 else "breakeven",
            "entry_order_id": None,
            "exit_order_id": execution_result.order_id,
            "entry_client_order_id": None,
            "exit_client_order_id": execution_result.client_order_id,
            "entry_filled_at": None,
            "exit_filled_at": _extract_order_time(execution_result.raw_response, "filled_at"),
            "run_id": run_id,
            "latest_signal_time": None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            "candidate_action": candidate_action,
            "execution_decision": "submitted_exit",
            "execution_mode": "paper" if runtime_config.paper_execution_enabled else "dry-run",
            "execution_key": execution_result.client_order_id,
            "broker_environment": account_context.environment,
            "account_equity": _maybe_float(getattr(broker_state.account, "equity", None)),
            "raw_entry_position": {
                "symbol": position.symbol,
                "qty": position.qty,
                "market_value": position.market_value,
                "avg_entry_price": entry_price,
                "asset_symbol": position.symbol,
            },
            "raw_exit_order": execution_result.raw_response or {},
            "raw_broker_state": {
                "account": {
                    "equity": broker_state.account.equity,
                    "buying_power": broker_state.account.buying_power,
                    "total_exposure": broker_state.account.total_exposure,
                    "open_positions_count": broker_state.account.open_positions_count,
                },
                "position": None
                if broker_state.position is None
                else {
                    "symbol": broker_state.position.symbol,
                    "qty": broker_state.position.qty,
                    "market_value": broker_state.position.market_value,
                },
            },
            "strategy_reasoning": _build_strategy_reasoning_payload(
                strategy_result=strategy_result,
                candidate_action=candidate_action,
                execution_decision="submitted_exit",
                ai_decision=strategy_result.ai_decision,
            ),
        }

        try:
            self._runtime_store.write_prime_stocks_trade_performance(
                uid=account_context.uid,
                account_id=runtime_config.account_id,
                trade_id=str(trade_id),
                trade_payload=trade_record,
            )
            logger.info(
                "Prime Stocks trade performance recorded symbol=%s trade_id=%s realized_pnl_dollars=%s",
                runtime_config.symbol,
                trade_id,
                trade_record["realized_pnl_dollars"],
            )
        except Exception as exc:  # pragma: no cover - performance tracking must not block execution flow
            logger.warning(
                "Prime Stocks trade performance write failed symbol=%s trade_id=%s error=%s",
                runtime_config.symbol,
                trade_id,
                exc,
            )

    def _evaluate_submission_guards(
        self,
        *,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        strategy_config: BismillahTrobotStocksV1Config,
        strategy_result,
        account_context: ResolvedAlpacaAccountContext,
        candidate_action: str,
        latest_signal_time: datetime | None,
        latest_execution: PrimeStocksLatestExecutionRecord,
        broker_state: AlpacaPaperSubmissionState,
    ) -> PrimeStocksExecutionFailure | None:
        if not broker_state.asset.tradable:
            return PrimeStocksExecutionFailure(
                execution_decision="broker_asset_not_tradable",
                skipped_reason="broker_asset_not_tradable",
                broker_error_code="broker_asset_not_tradable",
                broker_error_message=f"{runtime_config.symbol} is not tradable in Alpaca paper.",
            )
        latest_bar = strategy_result.latest_bar
        state_before = None if latest_bar is None else latest_bar.state_before
        state_final = strategy_result.final_state
        has_runtime_position = bool(
            (latest_bar is not None and latest_bar.in_position_before)
            or (state_before is not None and state_before.position_size > 0)
        )
        has_broker_position = _position_is_strategy_managed_open(broker_state.position)
        requested_notional = _resolve_effective_notional(
            candidate_action=candidate_action,
            runtime_config=runtime_config,
            strategy_config=strategy_config,
            account_equity=broker_state.account.equity,
        )
        budget_failure = _resolve_prime_budget_failure(
            runtime_config=runtime_config,
            candidate_action=candidate_action,
            broker_state=broker_state,
            requested_notional=requested_notional,
        )
        if budget_failure is not None:
            return budget_failure
        if candidate_action in {"FirstLot"} or candidate_action.startswith("MULTI-"):
            if _is_symbol_paused(runtime_config=runtime_config):
                return PrimeStocksExecutionFailure("symbol_paused", "symbol_paused")
            if requested_notional is None or requested_notional <= 0:
                return PrimeStocksExecutionFailure("invalid_order_notional", "invalid_order_notional")
            if broker_state.account.equity is None or broker_state.account.equity <= 0:
                return PrimeStocksExecutionFailure("broker_equity_unavailable", "broker_equity_unavailable")
            if broker_state.account.buying_power is not None and broker_state.account.buying_power < requested_notional:
                return PrimeStocksExecutionFailure(
                    "broker_insufficient_buying_power",
                    "broker_insufficient_buying_power",
                    broker_error_code="broker_insufficient_buying_power",
                    broker_error_message="Alpaca buying power is below the requested Prime Stocks notional.",
                )
        if candidate_action == "FirstLot":
            duplicate_failure = self._resolve_first_lot_duplicate_failure(
                runtime_config=runtime_config,
                latest_signal_time=latest_signal_time,
                latest_execution=latest_execution,
                account_context=account_context,
            )
            if duplicate_failure is not None:
                return duplicate_failure
        if candidate_action == "FirstLot":
            # Trust the live broker position for fresh FirstLot eligibility.
            # Stale runtime position snapshots can lag behind Firestore repairs or broker syncs,
            # which would otherwise suppress the first real trade even when Alpaca is flat.
            if has_broker_position:
                return PrimeStocksExecutionFailure("base_position_exists", "base_position_exists")
            return None
        if candidate_action.startswith("MULTI-"):
            add_tier = _parse_add_tier(candidate_action)
            if add_tier is None or add_tier < 1:
                return PrimeStocksExecutionFailure("invalid_add_tier", "invalid_add_tier")
            if not (has_runtime_position or has_broker_position):
                return PrimeStocksExecutionFailure("add_requires_base_position", "add_requires_base_position")
            if add_tier > runtime_config.max_add_count:
                return PrimeStocksExecutionFailure("max_add_count_exceeded", "max_add_count_exceeded")
            if state_before is not None and add_tier != (state_before.add_count + 1):
                return PrimeStocksExecutionFailure("invalid_add_tier", "invalid_add_tier")
            return None
        if candidate_action == "take_profit":
            if not has_broker_position:
                return PrimeStocksExecutionFailure("take_profit_requires_open_position", "take_profit_requires_open_position")
            return None
        if candidate_action in {"EXIT_ATR", "EXIT_REGIME"} and not (has_runtime_position or has_broker_position):
            return PrimeStocksExecutionFailure("exit_requires_open_position", "exit_requires_open_position")
        return None

    def _resolve_first_lot_duplicate_failure(
        self,
        *,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        latest_signal_time: datetime | None,
        latest_execution: PrimeStocksLatestExecutionRecord,
        account_context: ResolvedAlpacaAccountContext,
    ) -> PrimeStocksExecutionFailure | None:
        execution_key = _build_execution_key("FirstLot", latest_signal_time, symbol=runtime_config.symbol)
        latest_signal_time_value = None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat()

        if latest_execution.execution_key == execution_key and latest_execution.order_status in ACTIVE_ORDER_STATUSES:
            logger.info(
                "Prime Stocks duplicate suppressed via latest execution key symbol=%s execution_key=%s",
                runtime_config.symbol,
                execution_key,
            )
            return PrimeStocksExecutionFailure("skipped_duplicate", "duplicate_firstlot_order")

        try:
            symbol_state = self._runtime_store.load_runtime_symbol_state_record(
                uid=account_context.uid,
                account_id=runtime_config.account_id,
                slot_number=runtime_config.slot_number,
                symbol=runtime_config.symbol,
            )
        except PrimeStocksRuntimeStoreError as exc:
            logger.warning(
                "Prime Stocks duplicate guard could not load symbol state symbol=%s execution_key=%s error=%s",
                runtime_config.symbol,
                execution_key,
                exc,
            )
            symbol_state = None
        if symbol_state is not None:
            if symbol_state.execution_key == execution_key and symbol_state.latest_execution_decision == "submitted_buy":
                logger.info(
                    "Prime Stocks duplicate suppressed via symbol state execution key symbol=%s execution_key=%s",
                    runtime_config.symbol,
                    execution_key,
                )
                return PrimeStocksExecutionFailure("skipped_duplicate", "duplicate_firstlot_order")
            if (
                symbol_state.last_processed_bar_time is not None
                and latest_signal_time_value is not None
                and symbol_state.last_processed_bar_time == latest_signal_time_value
                and symbol_state.latest_candidate_action == "FirstLot"
                and symbol_state.latest_execution_decision == "submitted_buy"
            ):
                logger.info(
                    "Prime Stocks duplicate suppressed via symbol state last processed bar symbol=%s bar_time=%s",
                    runtime_config.symbol,
                    latest_signal_time_value,
                )
                return PrimeStocksExecutionFailure("skipped_duplicate", "duplicate_firstlot_order")

        try:
            recent_orders = self._paper_trading.list_recent_orders(credential_context=account_context)
        except Exception as exc:  # pragma: no cover - network/runtime safety only
            logger.warning(
                "Prime Stocks duplicate guard could not load recent Alpaca orders symbol=%s error=%s",
                runtime_config.symbol,
                exc,
            )
            return None

        if latest_signal_time is None:
            return None
        for order in recent_orders:
            if str(order.get("symbol", "")).strip().upper() != runtime_config.symbol.upper():
                continue
            if str(order.get("side", "")).strip().lower() != "buy":
                continue
            if str(order.get("type", "")).strip().lower() not in {"market", "limit", "stop", "stop_limit"}:
                continue
            created_at_raw = order.get("created_at")
            created_at = _parse_iso_utc(str(created_at_raw)) if created_at_raw is not None else None
            if created_at is not None and created_at >= latest_signal_time.astimezone(UTC):
                logger.info(
                    "Prime Stocks duplicate suppressed via Alpaca recent order symbol=%s order_id=%s created_at=%s",
                    runtime_config.symbol,
                    order.get("id"),
                    created_at.astimezone(UTC).isoformat(),
                )
                return PrimeStocksExecutionFailure("skipped_duplicate", "duplicate_firstlot_order")

        return None


def _build_runtime_state_record(
    *,
    run_id: str,
    runtime_state: PrimeStocksRuntimeStateRecord,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    account_context: ResolvedAlpacaAccountContext,
    strategy_result: PrimeStocksStrategyResult,
    candidate_action: str,
    execution_decision: str,
    latest_signal_time: datetime | None,
    execution_result: AlpacaPaperExecutionResult | None,
    advance_processed_bar: bool,
    current_total_exposure_pct: float | None = None,
) -> PrimeStocksRuntimeStateRecord:
    latest_signal_time_iso = None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat()
    final_state = strategy_result.final_state
    latest_close = final_state.position_avg_price or runtime_state.position_avg_price

    position_open = final_state.position_size > 0
    position_size = final_state.position_size
    position_avg_price = final_state.position_avg_price
    dollars_used = final_state.dollars_used
    add_count = final_state.add_count
    last_add_price = final_state.last_add_price
    pos_high = final_state.pos_high
    trail_stop = final_state.trail_stop
    last_entry_time = runtime_state.last_entry_time
    last_exit_time = runtime_state.last_exit_time

    if execution_decision in {"stale_data", "skipped_no_new_bar"}:
        position_open = runtime_state.position_open
        position_size = runtime_state.position_size
        position_avg_price = runtime_state.position_avg_price
        dollars_used = runtime_state.dollars_used
        add_count = runtime_state.add_count
        last_add_price = runtime_state.last_add_price
        pos_high = runtime_state.pos_high
        trail_stop = runtime_state.trail_stop
    elif execution_decision == "submitted_buy":
        projected_price = latest_close or runtime_state.position_avg_price or 1.0
        projected_notional = (
            execution_result.notional
            if execution_result is not None and execution_result.notional is not None
            else _resolve_effective_notional(
                candidate_action=candidate_action,
                runtime_config=runtime_config,
                strategy_config=_build_strategy_config(runtime_config),
                account_equity=broker_state.account.equity,
            ) or runtime_config.first_lot_notional
        )
        projected_qty = projected_notional / projected_price if projected_price > 0 else 0.0
        if candidate_action == "FirstLot":
            position_open = True
            position_size = max(projected_qty, final_state.position_size, runtime_state.position_size)
            position_avg_price = projected_price
            dollars_used = max(projected_notional, final_state.dollars_used)
            add_count = 0
            last_add_price = None
            pos_high = final_state.pos_high or latest_close
            trail_stop = final_state.trail_stop
            last_entry_time = latest_signal_time_iso or runtime_state.last_entry_time
        elif candidate_action.startswith("MULTI-"):
            position_open = True
            base_qty = max(runtime_state.position_size, final_state.position_size)
            base_avg = runtime_state.position_avg_price or final_state.position_avg_price or projected_price
            position_size = base_qty + projected_qty
            if position_size > 0 and base_qty > 0 and base_avg is not None:
                position_avg_price = ((base_avg * base_qty) + (projected_price * projected_qty)) / position_size
            else:
                position_avg_price = projected_price
            dollars_used = max(runtime_state.dollars_used + projected_notional, final_state.dollars_used)
            add_count = max(runtime_state.add_count + 1, _parse_add_tier(candidate_action) or 0, final_state.add_count)
            last_add_price = projected_price
            pos_high = final_state.pos_high or runtime_state.pos_high or latest_close
            trail_stop = final_state.trail_stop or runtime_state.trail_stop
            last_entry_time = runtime_state.last_entry_time or latest_signal_time_iso
    elif execution_decision == "submitted_exit":
        position_open = False
        position_size = 0.0
        position_avg_price = None
        dollars_used = 0.0
        add_count = 0
        last_add_price = None
        pos_high = None
        trail_stop = None
        last_exit_time = latest_signal_time_iso or runtime_state.last_exit_time

    return PrimeStocksRuntimeStateRecord(
        run_id=run_id,
        uid=account_context.uid,
        account_id=runtime_config.account_id,
        alpaca_account_id=runtime_config.alpaca_account_id,
        broker_environment=account_context.environment,
        symbol=runtime_config.symbol,
        position_open=position_open,
        position_size=position_size,
        position_avg_price=position_avg_price,
        dollars_used=dollars_used,
        add_count=add_count,
        add_tiers_filled=list(range(1, add_count + 1)),
        last_add_price=last_add_price,
        pos_high=pos_high,
        trail_stop=trail_stop,
        last_entry_time=last_entry_time,
        last_exit_time=last_exit_time,
        last_action=candidate_action,
        candidate_action=candidate_action,
        execution_key=_build_execution_key(candidate_action, latest_signal_time, symbol=runtime_config.symbol),
        last_processed_bar_time=(
            latest_signal_time_iso if advance_processed_bar and latest_signal_time_iso is not None else runtime_state.last_processed_bar_time
        ),
        latest_signal_time=latest_signal_time_iso,
        latest_candidate_action=candidate_action,
        latest_status=strategy_result.status,
        latest_execution_decision=execution_decision,
        current_total_exposure_pct=current_total_exposure_pct,
    )


def _resolve_runtime_entitlement_failure(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    account_context: ResolvedAlpacaAccountContext,
) -> tuple[str, str] | None:
    entitlement = account_context.entitlement if account_context.entitlement else runtime_config.entitlement
    if not entitlement:
        return (
            "entitlement_unavailable",
            "Prime Stocks runtime blocked because Laravel entitlement data was not available for the selected linked account.",
        )

    if not bool(entitlement.get("runtime_allowed", False)):
        summary = str(entitlement.get("blocked_summary") or "Prime Stocks runtime is not entitled for this workspace.").strip()
        blocked_reason = str(entitlement.get("blocked_reason") or "runtime_blocked").strip().lower() or "runtime_blocked"
        return (f"entitlement_{blocked_reason}", summary)

    account_slots = entitlement.get("account_slots_entitlement")
    if isinstance(account_slots, dict):
        selected_allowed = account_slots.get("selected_allowed")
        if selected_allowed is False:
            return (
                "entitlement_selected_account_slot_over_limit",
                "Prime Stocks runtime blocked because the selected linked account slot is above the entitled slot limit.",
            )

    if account_context.environment == "paper" and entitlement.get("paper_available") is False:
        return (
            "entitlement_paper_unavailable",
            "Prime Stocks runtime blocked because paper execution is not entitled for this workspace.",
        )

    if account_context.environment == "live" and entitlement.get("live_available") is False:
        return (
            "entitlement_live_unavailable",
            "Prime Stocks runtime blocked because live execution is not entitled for this workspace.",
        )

    return None


def build_prime_stocks_runtime_service(
    settings: AppConfig,
    market_data: AlpacaMarketDataAdapter | None = None,
    runtime_store: PrimeStocksFirestoreRuntimeStore | None = None,
    paper_trading: AlpacaPaperTradingAdapter | None = None,
    account_resolver: LaravelAlpacaAccountResolver | None = None,
) -> PrimeStocksRuntimeService:
    return PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data or AlpacaMarketDataAdapter(settings=settings),
        runtime_store=runtime_store or PrimeStocksFirestoreRuntimeStore(settings=settings),
        paper_trading=paper_trading or build_alpaca_broker_adapter(settings=settings),
        account_resolver=account_resolver or LaravelAlpacaAccountResolver(settings=settings),
    )


def build_prime_stocks_dry_run_service(
    settings: AppConfig,
    market_data: AlpacaMarketDataAdapter | None = None,
    runtime_store: PrimeStocksFirestoreRuntimeStore | None = None,
) -> PrimeStocksRuntimeService:
    return build_prime_stocks_runtime_service(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
    )


def _ensure_prime_stocks_runtime_context(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    allow_execution: bool | None,
) -> None:
    if runtime_config.product_key != "stocks.bismel1":
        raise ValueError(f"Prime Stocks runtime only supports product_key='stocks.bismel1'. Received {runtime_config.product_key!r}.")
    if runtime_config.asset_type != "stock" and not _is_admin_crypto_monitor_runtime(runtime_config):
        raise ValueError(f"Prime Stocks runtime only supports asset_type='stock' outside admin monitors. Received {runtime_config.asset_type!r}.")
    if allow_execution is True and not runtime_config.paper_execution_enabled:
        logger.info("Prime Stocks runtime execute trigger received while paper execution is disabled; request will stay no-op.")


def _is_admin_crypto_monitor_runtime(runtime_config: PrimeStocksRuntimeConfigRecord) -> bool:
    return (
        runtime_config.asset_type == "crypto"
        and (runtime_config.uid or "").strip() in ADMIN_CRYPTO_MONITOR_UIDS
        and (runtime_config.symbol or "").strip().upper() in ADMIN_CRYPTO_MONITOR_SYMBOLS
    )


def _is_validation_ping_request(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    test_trigger: str | None,
) -> bool:
    resolved_trigger = None if test_trigger is None else test_trigger.strip().lower()
    if resolved_trigger != "ping":
        return False
    if not runtime_config.ping_enabled or runtime_config.ping_mode == "off":
        raise ValueError("Prime Stocks validation ping trigger is disabled because admin ping control is off.")
    if not runtime_config.test_mode:
        raise ValueError("Prime Stocks validation ping trigger is disabled because test_mode is off.")
    configured_trigger = (runtime_config.test_trigger or "").strip().lower()
    if configured_trigger not in {"", "ping"}:
        raise ValueError("Prime Stocks validation ping trigger is disabled because test_trigger is not set to 'ping'.")
    return True


def _is_ping_short_circuit_request(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    trigger_source: str,
    test_trigger: str | None,
) -> bool:
    resolved_trigger = None if test_trigger is None else test_trigger.strip().lower()
    return (
        resolved_trigger == "ping"
        and trigger_source == "cloud_scheduler"
        and (not runtime_config.ping_enabled or runtime_config.ping_mode == "off")
    )


def _ping_daily_heartbeat_due(heartbeat_record: dict[str, object] | None) -> bool:
    if not isinstance(heartbeat_record, dict):
        return True
    last_ping_at = heartbeat_record.get("last_ping_at")
    if not isinstance(last_ping_at, str) or last_ping_at.strip() == "":
        return True
    try:
        last_ping = datetime.fromisoformat(last_ping_at.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return True
    return last_ping.date() != datetime.now(tz=UTC).date()


def _build_validation_ping_runtime_config(runtime_config: PrimeStocksRuntimeConfigRecord) -> PrimeStocksRuntimeConfigRecord:
    override_symbol = (runtime_config.test_symbol_override or "").strip().upper()
    if override_symbol != "SHIBUSD":
        raise ValueError("Prime Stocks validation ping requires test_symbol_override=SHIBUSD.")
    return replace(
        runtime_config,
        symbol=override_symbol,
        asset_type="crypto",
        dry_run=True,
        execution_timeframe="1M",
        trend_timeframe="1M",
        execution_bar_limit=19,
        trend_bar_limit=11,
    )


def _override_symbol(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    symbol: str | None,
    *,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
    slot_number: int | None = None,
) -> PrimeStocksRuntimeConfigRecord:
    return _override_runtime_selection(
        runtime_config,
        symbol=symbol,
        uid=uid,
        account_id=account_id,
        alpaca_account_id=alpaca_account_id,
        slot_number=slot_number,
    )


def _override_runtime_selection(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    symbol: str | None = None,
    uid: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
    slot_number: int | None = None,
) -> PrimeStocksRuntimeConfigRecord:
    if symbol is None or not symbol.strip():
        resolved_symbol = runtime_config.symbol
    else:
        resolved_symbol = symbol.strip().upper()

    resolved_uid = runtime_config.uid if uid is None else uid.strip()
    resolved_account_id = runtime_config.account_id if account_id is None else account_id
    resolved_alpaca_account_id = runtime_config.alpaca_account_id if alpaca_account_id is None else alpaca_account_id
    resolved_slot_number = runtime_config.slot_number if slot_number is None else max(1, slot_number)

    if (
        resolved_symbol == runtime_config.symbol
        and resolved_uid == runtime_config.uid
        and resolved_account_id == runtime_config.account_id
        and resolved_alpaca_account_id == runtime_config.alpaca_account_id
        and resolved_slot_number == runtime_config.slot_number
    ):
        return runtime_config

    return replace(
        runtime_config,
        symbol=resolved_symbol,
        uid=resolved_uid,
        account_id=resolved_account_id,
        alpaca_account_id=resolved_alpaca_account_id,
        slot_number=resolved_slot_number,
    )


def _normalize_configured_symbol(symbol: object) -> str | None:
    resolved_symbol = str(symbol or "").strip().upper()
    return resolved_symbol or None


def _resolve_schedulable_symbols(runtime_config: PrimeStocksRuntimeConfigRecord) -> list[str]:
    configured_modes: dict[str, str] = {}
    configured_order: list[str] = []

    for item in runtime_config.symbol_states:
        item_symbol = _normalize_configured_symbol(item.get("symbol"))
        if item_symbol is None:
            continue
        configured_modes[item_symbol] = str(item.get("mode", "active")).strip().lower()
        if item_symbol not in configured_order:
            configured_order.append(item_symbol)

    for symbol in runtime_config.selected_symbols:
        resolved_symbol = _normalize_configured_symbol(symbol)
        if resolved_symbol is None:
            continue
        configured_modes.setdefault(resolved_symbol, "active")
        if resolved_symbol not in configured_order:
            configured_order.append(resolved_symbol)

    if configured_order:
        return [
            symbol
            for symbol in configured_order
            if configured_modes.get(symbol, "active") not in {"paused", "standby"}
        ]

    if runtime_config.slot_number is not None:
        return []

    runtime_symbol = _normalize_configured_symbol(runtime_config.symbol)
    return [] if runtime_symbol is None else [runtime_symbol]


def _resolve_primary_runtime_symbol(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    explicit_symbol: str | None = None,
) -> str | None:
    if explicit_symbol is not None and explicit_symbol.strip():
        return explicit_symbol.strip().upper()

    schedulable_symbols = _resolve_schedulable_symbols(runtime_config)
    if schedulable_symbols:
        return schedulable_symbols[0]

    if runtime_config.slot_number is not None:
        return None

    return _normalize_configured_symbol(runtime_config.symbol)


def _resolve_latest_signal_time(execution_bars) -> datetime | None:
    if not execution_bars:
        return None
    latest_bar = execution_bars[-1]
    return latest_bar.ends_at or latest_bar.starts_at


def _has_no_new_closed_bar(
    *,
    runtime_state: PrimeStocksRuntimeStateRecord,
    latest_signal_time: datetime | None,
) -> bool:
    if latest_signal_time is None:
        return True
    if runtime_state.last_processed_bar_time is None:
        return False
    last_processed_bar_time = _parse_iso_utc(runtime_state.last_processed_bar_time)
    return latest_signal_time <= last_processed_bar_time


def _resolve_candidate_action(strategy_result) -> str:
    latest_signal = strategy_result.latest_signal
    if latest_signal.hit_atr_trail:
        return PRIME_DIAGNOSTIC_ATR_REVIEW
    if latest_signal.hit_regime:
        return PRIME_DIAGNOSTIC_REGIME_REVIEW
    if latest_signal.add_trigger:
        add_tier = 1
        if strategy_result.latest_bar is not None:
            add_tier = strategy_result.latest_bar.state_before.add_count + 1
        return f"MULTI-{add_tier}"
    if latest_signal.base_entry_trigger:
        return "FirstLot"
    return "HOLD"


def _prime_diagnostic_reason(candidate_action: str) -> str | None:
    if candidate_action in {PRIME_DIAGNOSTIC_ATR_REVIEW, "EXIT_ATR"}:
        return "atr_review"
    if candidate_action in {PRIME_DIAGNOSTIC_REGIME_REVIEW, "EXIT_REGIME"}:
        return "regime_review"
    return None


def _prime_diagnostic_summary(candidate_action: str) -> str | None:
    reason = _prime_diagnostic_reason(candidate_action)
    if reason == "atr_review":
        return "Non-executable review signal: ATR condition observed. Prime closes only by take profit."
    if reason == "regime_review":
        return "Non-executable review signal: regime condition observed. Prime closes only by take profit."
    return None


def _resolve_ai_blocked_candidate_action(*, candidate_action: str, ai_decision) -> str | None:
    if ai_decision is None:
        return None
    if candidate_action == "FirstLot" and ai_decision.Ai_block_new_entries:
        return ai_decision.Ai_blocked_reason or "ai_blocked"
    if candidate_action.startswith("MULTI-") and ai_decision.Ai_block_adds:
        return ai_decision.Ai_blocked_reason or "ai_blocked"
    return None


def _build_strategy_reasoning(
    *,
    strategy_result: PrimeStocksStrategyResult,
    candidate_action: str,
    execution_decision: str,
    ai_decision,
) -> dict[str, object]:
    def _series_list(name: str) -> list[object]:
        value = getattr(strategy_result.series, name, None)
        return value if isinstance(value, list) else []

    latest_bar = strategy_result.latest_bar
    latest_signal = strategy_result.latest_signal
    series = strategy_result.series
    latest_index = latest_bar.bar_index if latest_bar is not None else max(0, len(series.trend_ok) - 1)
    trend_ok = _bool_at(series.trend_ok, latest_index)
    trend_base = _bool_at(series.trend_base_htf, latest_index)
    trend_slope = _bool_at(series.htf_ema_slow_slope_up, latest_index)
    regime_fail = _bool_at(series.regime_fail, latest_index)
    pullback = _bool_at(series.in_pullback_zone, latest_index)
    setup_ready = _bool_at(_series_list("setup_ready"), latest_index)
    setup_age_bars_series = _series_list("setup_age_bars")
    setup_age_bars = setup_age_bars_series[latest_index] if latest_index < len(setup_age_bars_series) else None
    setup_invalidated = _bool_at(_series_list("setup_invalidated"), latest_index)
    reversal_context = _bool_at(_series_list("reversal_context"), latest_index)
    continuation_context = _bool_at(_series_list("continuation_context"), latest_index)
    confirmation = latest_signal.base_entry_trigger or latest_signal.add_trigger
    trigger_active = latest_signal.base_entry_trigger or latest_signal.add_trigger or latest_signal.hit_atr_trail or latest_signal.hit_regime
    setup_valid = bool(latest_signal.base_entry_signal)
    strategy_mode = str(getattr(strategy_result, "strategy_mode", "scalper")).strip().lower()
    trend_weight = 1.0 if trend_ok else 0.7 if strategy_mode == "scalper" else 0.7

    if trend_ok:
        trend_1d = "Up"
    elif regime_fail or (not trend_base and trend_slope is False):
        trend_1d = "Down"
    else:
        trend_1d = "Neutral"

    if trend_1d == "Up":
        bias_state = "Long preferred"
    elif trend_1d == "Down":
        bias_state = "Against trend (scalper mode)" if strategy_mode == "scalper" else "Against bias"
    else:
        bias_state = "Neutral"

    if candidate_action == "FirstLot":
        trigger_state = "Buy" if latest_signal.base_entry_trigger else ("Waiting" if latest_signal.base_entry_signal else "No signal")
    elif candidate_action.startswith("MULTI-"):
        trigger_state = "Add" if latest_signal.add_trigger else "Waiting"
    elif candidate_action in PRIME_DIAGNOSTIC_ACTIONS or latest_signal.hit_atr_trail or latest_signal.hit_regime:
        trigger_state = "Review"
    elif candidate_action in {"HOLD", "no_op"}:
        trigger_state = "Hold"
    else:
        trigger_state = "No signal"

    if ai_decision is None:
        ai_filter_state = "Neutral"
    elif ai_decision.Ai_blocked_reason == "ai_safety_unsafe":
        ai_filter_state = "Hard block"
    elif ai_decision.is_stale or not ai_decision.is_available:
        ai_filter_state = "Cautious"
    elif ai_decision.Ai_safety_label == "safe" and ai_decision.Ai_regime_label == "risk_on" and ai_decision.Ai_sentiment_label == "bullish":
        ai_filter_state = "Supportive"
    elif ai_decision.Ai_safety_label in {"safe", "caution"}:
        ai_filter_state = "Cautious"
    else:
        ai_filter_state = "Neutral"

    if execution_decision in {"submitted_buy", "submitted_add_buy", "FirstLot"}:
        final_decision = "Buy"
    elif execution_decision == "submitted_exit":
        final_decision = "Sell"
    elif execution_decision == "prime_diagnostic_review" or candidate_action in PRIME_DIAGNOSTIC_ACTIONS:
        final_decision = "Review"
    elif execution_decision in {"skipped_no_new_bar"}:
        final_decision = "Wait"
    elif execution_decision in {"no_op", "hold", "no_signal"}:
        final_decision = "No action"
    elif str(execution_decision).startswith("blocked") or "blocked" in str(execution_decision).lower():
        final_decision = "Blocked"
    else:
        final_decision = "Wait"

    if ai_decision is not None and ai_decision.Ai_blocked_reason == "ai_safety_unsafe":
        primary_reason = f"AI {ai_decision.Ai_blocked_reason}"
    elif execution_decision == "skipped_no_new_bar":
        primary_reason = "Awaiting latest 15M close."
    elif candidate_action in PRIME_DIAGNOSTIC_ACTIONS:
        primary_reason = _prime_diagnostic_summary(candidate_action) or "Non-executable review signal observed. Prime closes only by take profit."
    elif candidate_action == "take_profit":
        primary_reason = "Take-profit close submitted after fresh market confirmation."
    elif latest_signal.hit_regime:
        primary_reason = "Non-executable review signal: regime condition observed. Prime closes only by take profit."
    elif latest_signal.hit_atr_trail:
        primary_reason = "Non-executable review signal: ATR condition observed. Prime closes only by take profit."
    elif candidate_action == "FirstLot":
        if strategy_mode == "trend" and not trend_ok:
            primary_reason = "Against 1D bias."
        elif strategy_mode == "scalper" and not trend_ok:
            primary_reason = "Against trend (scalper mode)."
        elif not pullback:
            primary_reason = "Awaiting pullback into the setup zone."
        elif not latest_signal.base_entry_trigger:
            primary_reason = "Awaiting 15M confirmation."
        else:
            primary_reason = "15M trigger aligned with 1D bias."
    elif candidate_action.startswith("MULTI-"):
        if not latest_signal.add_trigger:
            primary_reason = "Awaiting add confirmation."
        else:
            primary_reason = "Add gates aligned."
    elif latest_signal.base_entry_signal:
        primary_reason = "15M setup is developing on the latest closed bar."
    else:
        primary_reason = "No setup on the latest closed bar."

    pullback_state_label = "Yes" if pullback else "No"
    if strategy_mode == "scalper" and continuation_context:
        pullback_state_label = "Not required"

    return {
        "execution_timeframe": strategy_result.execution_timeframe,
        "trend_timeframe": strategy_result.trend_timeframe,
        "strategy_context": f"{strategy_result.execution_timeframe} closed bars + {strategy_result.trend_timeframe} trend",
        "setup_context": "Reversal" if reversal_context else ("Continuation" if continuation_context else "Neutral"),
        "trend_1d": trend_1d,
        "bias_state": bias_state,
        "trend_weight": trend_weight,
        "pullback_state": pullback_state_label,
        "setup_ready": setup_ready,
        "setup_age_bars": setup_age_bars,
        "setup_invalidated": setup_invalidated,
        "confirmation_state": "Yes" if confirmation else "No",
        "trigger_state": trigger_state,
        "ai_filter_state": ai_filter_state,
        "setup_state": "Valid" if setup_valid else "Not valid",
        "final_decision": final_decision,
        "primary_reason": primary_reason,
        "trigger_active": trigger_active,
        "diagnostic_reason": _prime_diagnostic_reason(candidate_action),
        "diagnostic_executable": False if _prime_diagnostic_reason(candidate_action) is not None else None,
    }


def _resolve_mode(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    allow_execution: bool | None,
    account_context: ResolvedAlpacaAccountContext,
    settings: AppConfig,
) -> str:
    if allow_execution is not True:
        return "dry-run"

    entitlement = account_context.entitlement if account_context.entitlement else runtime_config.entitlement
    if not entitlement or not bool(entitlement.get("runtime_allowed", False)):
        return "dry-run"

    if account_context.environment == "live":
        if entitlement.get("live_available") is True:
            return "live"
        return "dry-run"

    if entitlement.get("paper_available") is False:
        return "dry-run"

    if account_context.environment == "paper":
        return "paper"

    return "dry-run"


def _build_placeholder_strategy_result(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    status: str,
    message: str,
) -> PrimeStocksStrategyResult:
    return PrimeStocksStrategyResult(
        product_key=runtime_config.product_key,
        pine_strategy_title=runtime_config.strategy_title,
        status=status,
        message=message,
        series=PineComputedSeries(),
        latest_signal=PineSignalSnapshot(
            base_entry_signal=False,
            base_entry_trigger=False,
            add_bounce_confirm=False,
            gate_atr_ok=False,
            gate_dp_ok=False,
            cap_ok=False,
            add_signal_raw=False,
            add_trigger=False,
            hit_atr_trail=False,
            hit_regime=False,
        ),
        latest_bar=None,
        final_state=BismillahTrobotStocksV1State(),
        ai_decision=None,
        execution_allowed=False,
        execution_timeframe=runtime_config.execution_timeframe,
        trend_timeframe=runtime_config.trend_timeframe,
    )


def _build_fallback_account_context(runtime_config: PrimeStocksRuntimeConfigRecord) -> ResolvedAlpacaAccountContext:
    return ResolvedAlpacaAccountContext(
        uid=runtime_config.uid or "",
        account_id=runtime_config.account_id or 0,
        alpaca_account_id=runtime_config.alpaca_account_id or 0,
        broker_connection_id=0,
        broker_credential_id=0,
        environment="paper",
        data_feed="iex",
        access_mode="read_only",
        trade_enabled=False,
        key_id="",
        secret="",
        entitlement=runtime_config.entitlement,
    )


def _build_runtime_message(*, execution_mode: str, execution_decision: str) -> str:
    if execution_decision == "validation_ping_ok":
        return (
            "Prime Stocks validation ping completed successfully with SHIBUSD test-mode market data, linked account "
            "resolution, AI cache heartbeat, and Firestore persistence."
        )
    if execution_decision == "validation_ping_duplicate":
        return (
            "Prime Stocks validation ping detected the same SHIBUSD validation bar and persisted a duplicate-safe "
            "heartbeat without reprocessing."
        )
    if execution_decision == "validation_ping_ai_cache_unavailable":
        return "Prime Stocks validation ping continued with advisory AI validation cache unavailable."
    if execution_decision == "validation_ping_ai_cache_stale":
        return "Prime Stocks validation ping continued with advisory AI validation cache stale."
    if execution_decision == "validation_ping_stale_data":
        return "Prime Stocks validation ping blocked because SHIBUSD validation bars are stale."
    if execution_decision == "skipped_no_new_bar":
        return (
            "Prime Stocks Cloud Run runtime skipped because no newly closed Prime Stocks execution bar was available. "
            "Scheduled and manual triggers only evaluate newly closed bars."
        )
    if execution_decision == "stale_data":
        return (
            "Prime Stocks Cloud Run runtime blocked because the fetched execution bars are stale for the configured "
            "timeframe."
        )
    if execution_decision in {"ai_cache_stale", "ai_cache_unavailable"}:
        return (
            "Prime Stocks Cloud Run runtime continued with advisory AI cache freshness state "
            f"({execution_decision})."
        )
    if execution_decision in {"ai_safety_unsafe", "ai_blocked"}:
        return (
            "Prime Stocks Cloud Run runtime blocked buy-side execution because cached AI safety data is unsafe "
            f"({execution_decision})."
        )
    if execution_decision == "prime_diagnostic_review":
        return (
            "Prime Stocks runtime recorded a non-executable diagnostic review signal. Prime closes only by take profit."
        )
    if execution_mode == "paper":
        return (
            "Prime Stocks Cloud Run paper runtime completed with server-side market data, strategy evaluation, "
            f"Firestore writes, and guarded Alpaca paper execution. Execution decision: {execution_decision}."
        )
    if execution_mode == "live":
        return (
            "Prime Stocks Cloud Run live runtime completed with server-side market data, strategy evaluation, "
            f"Firestore writes, and guarded Alpaca live execution. Execution decision: {execution_decision}."
        )
    return (
        "Prime Stocks Cloud Run dry-run completed with server-side market data, strategy evaluation, and Firestore writes. "
        f"No live orders were placed. Execution decision: {execution_decision}."
    )


def _build_validation_ping_message(
    *,
    execution_decision: str,
    market_data_stale_bypassed: bool,
    ai_stale_bypassed: bool,
) -> str:
    message = _build_runtime_message(execution_mode="dry-run", execution_decision=execution_decision)
    notes: list[str] = []
    if market_data_stale_bypassed:
        notes.append("market-data stale bypass active")
    if ai_stale_bypassed:
        notes.append("AI stale bypass active")
    if not notes:
        return message
    return f"{message} Ping test mode notes: {', '.join(notes)}."


def _build_execution_key(candidate_action: str, latest_signal_time: datetime | None, symbol: str | None = None) -> str:
    latest_signal_time_value = None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat()
    if candidate_action == "FirstLot" and symbol is not None and str(symbol).strip() != "":
        return f"FirstLot:{str(symbol).strip().upper()}:{latest_signal_time_value}"
    return f"{candidate_action}:{latest_signal_time_value}"


def _extract_order_fill_price(raw_response: dict[str, Any] | None) -> float | None:
    if not isinstance(raw_response, dict):
        return None

    for key in ("filled_avg_price", "avg_fill_price", "fill_price", "average_fill_price", "price"):
        value = raw_response.get(key)
        try:
            if value is not None and str(value).strip() != "":
                parsed = float(value)
                if parsed > 0:
                    return parsed
        except (TypeError, ValueError):
            continue
    return None


def _extract_order_time(raw_response: dict[str, Any] | None, key: str) -> str | None:
    if not isinstance(raw_response, dict):
        return None

    value = raw_response.get(key)
    if not isinstance(value, str) or value.strip() == "":
        return None

    try:
        return _parse_iso_utc(value).isoformat()
    except Exception:
        return value


def _build_client_order_id(*, run_id: str, candidate_action: str) -> str:
    action_slug = "tp" if candidate_action == "take_profit" else candidate_action.lower().replace("_", "-")
    run_slug = run_id.replace("dryrun-", "").replace("run-", "")
    return f"prime-{action_slug}-{run_slug}"[:47]


def _format_qty(value: float) -> str:
    return format(float(value), ".9f").rstrip("0").rstrip(".")


def _resolve_prime_take_profit_candidate(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_result: PrimeStocksStrategyResult,
    broker_state: AlpacaPaperSubmissionState,
    latest_signal_time: datetime | None,
    latest_execution_bar: Any | None,
) -> dict[str, Any] | None:
    position = broker_state.position
    if not _position_is_strategy_managed_open(position):
        return None
    qty = _maybe_float(getattr(position, "qty", None))
    entry_price = _maybe_float(getattr(position, "avg_entry_price", None))
    if qty is None or qty <= 0 or entry_price is None or entry_price <= 0:
        return None

    threshold_metadata = _resolve_prime_take_profit_threshold_metadata(
        runtime_config=runtime_config,
        strategy_result=strategy_result,
        entry_price=entry_price,
    )
    threshold = _maybe_float(threshold_metadata.get("tp_threshold"))
    if threshold is None or threshold <= entry_price:
        return None

    confirmation = _resolve_prime_take_profit_confirmation(
        latest_execution_bar=latest_execution_bar,
        latest_signal_time=latest_signal_time,
        threshold=threshold,
    )
    if confirmation is None:
        return None

    return {
        "product": "prime_stocks",
        "request_action": "close",
        "close_reason": "take_profit",
        "symbol": runtime_config.symbol.upper(),
        "qty": qty,
        "entry_price": entry_price,
        "tp_mode": runtime_config.tp_mode,
        "tp_atr_mult": runtime_config.tp_atr_mult,
        "tp_percent": runtime_config.tp_percent,
        **threshold_metadata,
        **confirmation,
    }


def _resolve_prime_take_profit_threshold(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_result: PrimeStocksStrategyResult,
    entry_price: float,
) -> float | None:
    threshold_metadata = _resolve_prime_take_profit_threshold_metadata(
        runtime_config=runtime_config,
        strategy_result=strategy_result,
        entry_price=entry_price,
    )
    return _maybe_float(threshold_metadata.get("tp_threshold"))


def _resolve_prime_take_profit_threshold_metadata(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_result: PrimeStocksStrategyResult,
    entry_price: float,
) -> dict[str, Any]:
    mode = (runtime_config.tp_mode or "atr").strip().lower()
    atr_value = _latest_series_float(strategy_result, "atr_val")
    atr_threshold = (
        entry_price + (atr_value * runtime_config.tp_atr_mult)
        if atr_value is not None and atr_value > 0 and runtime_config.tp_atr_mult > 0
        else None
    )
    floor_threshold = (
        entry_price * (1.0 + (runtime_config.tp_percent / 100.0))
        if runtime_config.tp_percent > 0
        else None
    )
    threshold: float | None = None
    floor_applied = False

    if mode == "atr":
        if atr_threshold is not None and floor_threshold is not None:
            threshold = max(atr_threshold, floor_threshold)
            floor_applied = floor_threshold >= atr_threshold
        elif atr_threshold is not None:
            threshold = atr_threshold
        elif floor_threshold is not None:
            threshold = floor_threshold
            floor_applied = True
    if mode in {"percent", "pct"}:
        threshold = floor_threshold
        floor_applied = threshold is not None
    elif mode in {"off", "none", "disabled"}:
        threshold = None
        floor_applied = False
    elif mode != "atr":
        if atr_threshold is not None:
            threshold = atr_threshold
        elif floor_threshold is not None:
            threshold = floor_threshold
            floor_applied = True

    return {
        "tp_percent_floor": runtime_config.tp_percent if floor_threshold is not None else None,
        "atr_value": atr_value,
        "atr_threshold": atr_threshold,
        "floor_threshold": floor_threshold,
        "tp_threshold": threshold,
        "tp_floor_applied": floor_applied,
    }


def _resolve_prime_take_profit_confirmation(
    *,
    latest_execution_bar: Any | None,
    latest_signal_time: datetime | None,
    threshold: float,
) -> dict[str, Any] | None:
    if latest_execution_bar is None:
        return None
    bar_close = _maybe_float(getattr(latest_execution_bar, "close", None))
    confirmation_price = None
    confirmation_source = None
    if bar_close is not None and bar_close >= threshold:
        confirmation_price = bar_close
        confirmation_source = "bar_close"
    if confirmation_price is None:
        return None
    confirmation_at = latest_signal_time
    if confirmation_at is None:
        raw_at = getattr(latest_execution_bar, "ends_at", None) or getattr(latest_execution_bar, "starts_at", None)
        confirmation_at = raw_at if isinstance(raw_at, datetime) else None
    return {
        "market_confirmation_source": confirmation_source,
        "market_confirmation_price": confirmation_price,
        "market_confirmation_at": None if confirmation_at is None else confirmation_at.astimezone(UTC).isoformat(),
        "market_confirmation_fresh": True,
    }


def _prime_take_profit_high_touched_without_executable_confirmation(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_result: PrimeStocksStrategyResult,
    latest_execution_bar: Any | None,
    position: AlpacaPaperPositionState,
) -> bool:
    if latest_execution_bar is None:
        return False
    entry_price = _maybe_float(getattr(position, "avg_entry_price", None))
    if entry_price is None or entry_price <= 0:
        return False
    threshold = _resolve_prime_take_profit_threshold(
        runtime_config=runtime_config,
        strategy_result=strategy_result,
        entry_price=entry_price,
    )
    if threshold is None or threshold <= entry_price:
        return False
    bar_high = _maybe_float(getattr(latest_execution_bar, "high", None))
    bar_close = _maybe_float(getattr(latest_execution_bar, "close", None))
    return bool(bar_high is not None and bar_high >= threshold and (bar_close is None or bar_close < threshold))


def _latest_series_float(strategy_result: PrimeStocksStrategyResult, name: str) -> float | None:
    values = getattr(strategy_result.series, name, None)
    if not isinstance(values, list) or not values:
        return None
    index = len(values) - 1
    latest_bar = getattr(strategy_result, "latest_bar", None)
    if latest_bar is not None and isinstance(getattr(latest_bar, "bar_index", None), int):
        index = latest_bar.bar_index
    if index < 0 or index >= len(values):
        return None
    return _maybe_float(values[index])


def _validate_prime_close_order_metadata(*, candidate_action: str, client_order_id: str) -> str | None:
    close_reason = {
        "take_profit": "take_profit",
        "TAKE_PROFIT": "take_profit",
        "TP": "take_profit",
        "EXIT_ATR": "atr_exit",
        "EXIT_REGIME": "regime_exit",
    }.get(candidate_action, candidate_action.lower())
    if not client_order_id.startswith("prime-"):
        return "prime_close_metadata_invalid"
    if close_reason != "take_profit":
        return "prime_non_tp_close_blocked"
    return None


def _validate_successful_execution_result(
    *,
    result: AlpacaPaperExecutionResult,
    candidate_action: str,
) -> AlpacaPaperTradingError | None:
    order_id = str(result.order_id).strip() if result.order_id is not None else ""
    client_order_id = str(result.client_order_id).strip() if result.client_order_id is not None else ""
    if not result.submitted:
        return AlpacaPaperTradingError(
            code="broker_invalid_response",
            message=(
                "Prime Stocks broker submission did not return a submitted order state "
                f"for candidate_action={candidate_action}."
            ),
            retryable=False,
            raw_response=result.raw_response,
        )
    if order_id == "" and client_order_id == "":
        return AlpacaPaperTradingError(
            code="broker_invalid_response",
            message=(
                "Prime Stocks broker submission returned no order identifier "
                f"for candidate_action={candidate_action}."
            ),
            retryable=False,
            raw_response=result.raw_response,
        )
    return None


def _build_runtime_store_failure_result(
    *,
    run_id: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    allow_execution: bool | None,
    trigger_type: str,
    trigger_source: str,
    firestore_paths: dict[str, str],
    message: str,
    latest_signal_time: datetime | None = None,
    candidate_action: str = "BLOCKED",
    bars_processed_execution: int = 0,
    bars_processed_trend: int = 0,
    ) -> PrimeStocksRuntimeResult:
    return _build_blocked_runtime_result(
        run_id=run_id,
        runtime_config=runtime_config,
        allow_execution=allow_execution,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        firestore_paths=firestore_paths,
        message=message,
        latest_signal_time=latest_signal_time,
        candidate_action=candidate_action,
        bars_processed_execution=bars_processed_execution,
        bars_processed_trend=bars_processed_trend,
        execution_decision="runtime_store_unavailable",
        skipped_reason="runtime_store_unavailable",
    )


def _build_validation_ping_disabled_result(
    *,
    run_id: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    trigger_type: str,
    trigger_source: str,
    firestore_paths: dict[str, str],
) -> PrimeStocksRuntimeResult:
    return PrimeStocksRuntimeResult(
        run_id=run_id,
        mode="dry-run",
        runtime_target=runtime_config.runtime_target,
        product_key=runtime_config.product_key,
        strategy_key=runtime_config.strategy_key,
        strategy_title=runtime_config.strategy_title,
        symbol=runtime_config.symbol,
        asset_type=runtime_config.asset_type,
        enabled=runtime_config.enabled,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        candidate_action="PING_OFF",
        execution_decision="validation_ping_disabled",
        order_status="not_submitted",
        order_submitted=False,
        order_id=None,
        client_order_id=None,
        add_tier=None,
        execution_allowed=False,
        skipped_reason="validation_ping_disabled",
        latest_signal_time=None,
        ai=None,
        status="disabled",
        message=(
            "Prime Stocks validation ping is disabled by admin control, so the scheduler ping path returned cleanly "
            "without writing the normal runtime docs."
        ),
        bars_processed_execution=0,
        bars_processed_trend=0,
        firestore_paths=firestore_paths,
    )


def _build_blocked_runtime_result(
    *,
    run_id: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    allow_execution: bool | None,
    trigger_type: str,
    trigger_source: str,
    firestore_paths: dict[str, str],
    message: str,
    execution_decision: str,
    skipped_reason: str,
    latest_signal_time: datetime | None = None,
    candidate_action: str = "BLOCKED",
    bars_processed_execution: int = 0,
    bars_processed_trend: int = 0,
) -> PrimeStocksRuntimeResult:
    return PrimeStocksRuntimeResult(
        run_id=run_id,
        mode="dry-run",
        runtime_target=runtime_config.runtime_target,
        product_key=runtime_config.product_key,
        strategy_key=runtime_config.strategy_key,
        strategy_title=runtime_config.strategy_title,
        symbol=runtime_config.symbol,
        asset_type=runtime_config.asset_type,
        enabled=runtime_config.enabled,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        candidate_action=candidate_action,
        execution_decision=execution_decision,
        order_status="not_submitted",
        order_submitted=False,
        order_id=None,
        client_order_id=None,
        add_tier=None,
        execution_allowed=False,
        skipped_reason=skipped_reason,
        latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
        ai=None,
        status="blocked",
        message=message,
        bars_processed_execution=bars_processed_execution,
        bars_processed_trend=bars_processed_trend,
        firestore_paths=firestore_paths,
    )


def _parse_iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _maybe_float(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_add_tier(candidate_action: str) -> int | None:
    if not candidate_action.startswith("MULTI-"):
        return None
    try:
        return int(candidate_action.split("-", maxsplit=1)[1])
    except (IndexError, ValueError):
        return None


def _resolve_multi_notional(
    *,
    add_tier: int | None,
    strategy_config: BismillahTrobotStocksV1Config,
) -> float:
    if add_tier is None:
        return strategy_config.first_lot_dollars
    return round(strategy_config.first_lot_dollars * _runtime_qty_mult(add_tier, strategy_config), 2)


def _resolve_requested_notional(
    *,
    candidate_action: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_config: BismillahTrobotStocksV1Config,
    account_equity: float | None = None,
) -> float | None:
    if candidate_action == "FirstLot":
        if account_equity is not None and account_equity > 0:
            per_symbol_entry_pct = max(0.0, float(runtime_config.per_symbol_entry_pct or 0.0))
            return round(account_equity * (per_symbol_entry_pct / 100.0), 2)
        return runtime_config.first_lot_notional
    if candidate_action.startswith("MULTI-"):
        return _resolve_multi_notional(add_tier=_parse_add_tier(candidate_action), strategy_config=strategy_config)
    return None


def _resolve_effective_notional(
    *,
    candidate_action: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    strategy_config: BismillahTrobotStocksV1Config,
    account_equity: float | None = None,
) -> float | None:
    requested_notional = _resolve_requested_notional(
        candidate_action=candidate_action,
        runtime_config=runtime_config,
        strategy_config=strategy_config,
        account_equity=account_equity,
    )
    if requested_notional is None:
        return None
    if not runtime_config.safe_mode_enabled:
        return requested_notional
    size_pct = max(1.0, min(100.0, runtime_config.safe_mode_size_pct))
    return round(requested_notional * (size_pct / 100.0), 2)


def _resolve_kill_switch_failure(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    candidate_action: str,
) -> PrimeStocksExecutionFailure | None:
    if candidate_action not in {"FirstLot"} and not candidate_action.startswith("MULTI-"):
        return None
    if runtime_config.global_kill_switch_enabled:
        return PrimeStocksExecutionFailure("global_kill_switch_enabled", "global_kill_switch_enabled")
    if runtime_config.account_kill_switch_enabled:
        return PrimeStocksExecutionFailure("account_kill_switch_enabled", "account_kill_switch_enabled")
    return None


def _resolve_prime_budget_failure(
    *,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    candidate_action: str,
    broker_state: AlpacaPaperSubmissionState,
    requested_notional: float | None,
) -> PrimeStocksExecutionFailure | None:
    if requested_notional is None or requested_notional <= 0:
        return None
    account_equity = broker_state.account.equity
    if account_equity is None or account_equity <= 0:
        return None
    current_exposure = max(0.0, float(broker_state.account.total_exposure or 0.0))

    if candidate_action == "FirstLot":
        per_symbol_entry_pct = max(0.0, float(runtime_config.per_symbol_entry_pct or 0.0))
        per_symbol_entry_budget = round(account_equity * (per_symbol_entry_pct / 100.0), 2)
        if per_symbol_entry_budget > 0 and requested_notional > (per_symbol_entry_budget + 0.01):
            return PrimeStocksExecutionFailure(
                execution_decision="prime_symbol_entry_budget_exceeded",
                skipped_reason="prime_symbol_entry_budget_exceeded",
            )
        total_entry_budget = round(account_equity * (max(0.0, float(runtime_config.total_entry_exposure_cap_pct or 0.0)) / 100.0), 2)
        if total_entry_budget > 0 and (current_exposure + requested_notional) > (total_entry_budget + 0.01):
            return PrimeStocksExecutionFailure(
                execution_decision="prime_total_entry_budget_reached",
                skipped_reason="prime_total_entry_budget_reached",
            )
        return None

    if candidate_action.startswith("MULTI-"):
        total_add_budget = round(account_equity * (max(0.0, float(runtime_config.total_add_exposure_cap_pct or 0.0)) / 100.0), 2)
        if total_add_budget > 0 and (current_exposure + requested_notional) > (total_add_budget + 0.01):
            return PrimeStocksExecutionFailure(
                execution_decision="prime_total_add_budget_reached",
                skipped_reason="prime_total_add_budget_reached",
            )
    return None


def _is_symbol_paused(*, runtime_config: PrimeStocksRuntimeConfigRecord) -> bool:
    symbol = runtime_config.symbol.strip().upper()
    if symbol == "":
        return False
    for item in runtime_config.symbol_states:
        item_symbol = str(item.get("symbol", "")).strip().upper()
        if item_symbol != symbol:
            continue
        return str(item.get("mode", "active")).strip().lower() in {"paused", "standby"}
    return False


def _resolve_total_exposure_pct(
    *,
    broker_state: AlpacaPaperSubmissionState,
    additional_notional: float | None = None,
) -> float | None:
    equity = broker_state.account.equity
    if equity is None or equity <= 0:
        return None
    total_exposure = 0.0 if broker_state.account.total_exposure is None else broker_state.account.total_exposure
    if additional_notional is not None:
        total_exposure += additional_notional
    return round((total_exposure / equity) * 100.0, 2)


def _runtime_qty_mult(step: int, config: BismillahTrobotStocksV1Config) -> float:
    if step == 1:
        return config.q1
    if step == 2:
        return config.q2
    if step == 3:
        return config.q3
    return config.q4


def _is_stale_market_data(
    *,
    latest_signal_time: datetime | None,
    execution_timeframe: str,
    now: datetime,
) -> bool:
    if latest_signal_time is None:
        return False
    threshold = _timeframe_stale_threshold(execution_timeframe)
    resolved_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    return resolved_now.astimezone(UTC) - latest_signal_time.astimezone(UTC) > threshold


def _timeframe_stale_threshold(execution_timeframe: str) -> timedelta:
    normalized = _normalize_runtime_timeframe(execution_timeframe)
    if normalized == "15M":
        return timedelta(hours=2, minutes=15)
    if normalized == "1H":
        return timedelta(hours=2, minutes=15)
    if normalized == "4H":
        return timedelta(hours=8, minutes=15)
    if normalized == "1D":
        return timedelta(days=2, hours=1)
    return timedelta(hours=2, minutes=15)


def _build_strategy_config(runtime_config: PrimeStocksRuntimeConfigRecord) -> BismillahTrobotStocksV1Config:
    return BismillahTrobotStocksV1Config(
        execution_timeframe=_normalize_runtime_timeframe(runtime_config.execution_timeframe),
        trend_timeframe=_normalize_runtime_timeframe(runtime_config.trend_timeframe),
        exec_tf_note=f"Run Bismillah on {_normalize_runtime_timeframe(runtime_config.execution_timeframe)} chart",
        trend_tf=_normalize_trend_tf(runtime_config.trend_timeframe),
        swing_len=max(5, int(runtime_config.pullback_window)),
        first_lot_dollars=runtime_config.first_lot_notional,
        max_adds=runtime_config.max_add_count,
    )


def _resolve_forced_candidate_action(
    *,
    candidate_action: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    settings: AppConfig,
) -> str:
    forced_candidate_action = _normalize_forced_candidate_action(
        runtime_config.force_candidate_action or settings.prime_stocks_force_candidate_action
    )
    if forced_candidate_action is None:
        return candidate_action
    return forced_candidate_action


def _ai_validation_bypass_enabled(runtime_config: PrimeStocksRuntimeConfigRecord) -> bool:
    return runtime_config.test_mode and runtime_config.ai_validation_bypass_enabled


def _normalize_forced_candidate_action(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    aliases = {
        "FIRSTLOT": "FirstLot",
        "EXIT_ATR": PRIME_DIAGNOSTIC_ATR_REVIEW,
        "ATR_REVIEW": PRIME_DIAGNOSTIC_ATR_REVIEW,
        "DIAGNOSTIC_ATR_REVIEW": PRIME_DIAGNOSTIC_ATR_REVIEW,
        "EXIT_REGIME": PRIME_DIAGNOSTIC_REGIME_REVIEW,
        "REGIME_REVIEW": PRIME_DIAGNOSTIC_REGIME_REVIEW,
        "DIAGNOSTIC_REGIME_REVIEW": PRIME_DIAGNOSTIC_REGIME_REVIEW,
        "HOLD": "HOLD",
    }
    if normalized in aliases:
        return aliases[normalized]
    if normalized.startswith("MULTI-"):
        try:
            tier = int(normalized.split("-", maxsplit=1)[1])
        except (IndexError, ValueError):
            return None
        if tier < 1:
            return None
        return f"MULTI-{tier}"
    return None


def _normalize_runtime_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().upper()
    aliases = {
        "15M": "15M",
        "15MIN": "15M",
        "1HOUR": "1H",
        "1H": "1H",
        "4HOUR": "4H",
        "4H": "4H",
        "1DAY": "1D",
        "DAY": "1D",
        "D": "1D",
        "1D": "1D",
    }
    return aliases.get(normalized, normalized)


def _normalize_trend_tf(timeframe: str) -> str:
    normalized = _normalize_runtime_timeframe(timeframe)
    return "D" if normalized == "1D" else normalized


PrimeStocksDryRunService = PrimeStocksRuntimeService
PrimeStocksDryRunResult = PrimeStocksRuntimeResult
