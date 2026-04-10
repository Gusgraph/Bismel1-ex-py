# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/services/firestore_runtime_store.py
# ======================================================

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Protocol, TypeVar
from uuid import uuid4

from app.brokers.alpaca_paper_trading import AlpacaPaperExecutionResult
from app.products.stocks.bismel1.models import (
    AiCacheRecord,
    BismillahTrobotStocksV1State,
    PrimeStocksAiDecision,
    PrimeStocksStrategyResult,
)
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.services.gemini_ai_scoring import serialize_ai_decision
from app.shared.config import AppConfig

SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
BOOTSTRAP_RUN_ID = "bootstrap-prime-stocks"
T = TypeVar("T")


class PrimeStocksRuntimeStoreError(RuntimeError):
    """Raised when Firestore-backed runtime control reads or writes are unavailable."""


@dataclass(frozen=True)
class PrimeStocksRuntimeConfigRecord:
    product_key: str
    strategy_key: str
    strategy_title: str
    symbol: str
    asset_type: str
    enabled: bool
    dry_run: bool
    paper_execution_enabled: bool
    live_execution_enabled: bool
    ping_enabled: bool
    ping_mode: str
    ping_daily_heartbeat_enabled: bool
    test_mode: bool
    test_trigger: str | None
    test_symbol_override: str | None
    force_candidate_action: str | None
    ai_validation_bypass_enabled: bool
    execution_timeframe: str
    trend_timeframe: str
    pullback_window: int
    execution_bar_limit: int
    trend_bar_limit: int
    first_lot_notional: float
    multi_notional: float
    max_notional_per_order: float
    max_total_notional_per_symbol: float
    max_add_count: int
    daily_order_cap: int | None = None
    max_open_positions: int | None = None
    broker_retry_max_attempts: int = 1
    uid: str | None = None
    account_id: int | None = None
    alpaca_account_id: int | None = None
    runtime_target: str = "cloud_run"
    entitlement: dict[str, Any] = field(default_factory=dict)
    safe_mode_enabled: bool = False
    safe_mode_size_pct: float = 100.0
    live_cap_pct: float = 3.0
    max_total_exposure_pct: float = 70.0
    per_symbol_entry_pct: float = 3.0
    total_entry_exposure_cap_pct: float = 20.0
    total_add_exposure_cap_pct: float = 70.0
    global_kill_switch_enabled: bool = False
    account_kill_switch_enabled: bool = False
    selected_symbols: list[str] = field(default_factory=list)
    symbol_states: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class PrimeStocksRuntimeStorePaths:
    config_document: str
    state_document: str
    heartbeat_document: str
    snapshot_document: str
    signal_document: str
    execution_document: str
    action_document: str
    logs_collection: str
    signal_audit_collection: str
    action_audit_collection: str
    order_audit_collection: str
    notifications_collection: str
    notification_state_document: str
    ai_market_document: str
    ai_symbols_collection: str


@dataclass(frozen=True)
class PrimeStocksLatestExecutionRecord:
    execution_key: str | None = None
    order_status: str | None = None
    run_id: str | None = None
    candidate_action: str | None = None
    latest_signal_time: str | None = None


@dataclass(frozen=True)
class PrimeStocksRuntimeStateRecord:
    run_id: str | None = None
    uid: str | None = None
    account_id: int | None = None
    alpaca_account_id: int | None = None
    broker_environment: str | None = None
    symbol: str | None = None
    position_open: bool = False
    position_size: float = 0.0
    position_avg_price: float | None = None
    dollars_used: float = 0.0
    add_count: int = 0
    add_tiers_filled: list[int] = field(default_factory=list)
    last_add_price: float | None = None
    pos_high: float | None = None
    trail_stop: float | None = None
    last_entry_time: str | None = None
    last_exit_time: str | None = None
    last_action: str | None = None
    candidate_action: str | None = None
    execution_key: str | None = None
    last_processed_bar_time: str | None = None
    latest_signal_time: str | None = None
    latest_candidate_action: str | None = None
    latest_status: str | None = None
    latest_execution_decision: str | None = None
    current_total_exposure_pct: float | None = None

    def to_strategy_state(self) -> BismillahTrobotStocksV1State:
        return BismillahTrobotStocksV1State(
            add_count=self.add_count,
            last_add_price=self.last_add_price,
            dollars_used=self.dollars_used,
            pos_high=self.pos_high,
            trail_stop=self.trail_stop,
            position_avg_price=self.position_avg_price,
            position_size=self.position_size,
        )


class FirestoreClientProtocol(Protocol):
    def collection(self, name: str) -> Any:
        ...


class PrimeStocksFirestoreRuntimeStore:
    def __init__(
        self,
        settings: AppConfig,
        client: FirestoreClientProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._client = client

    def get_paths(self, *, uid: str | None = None, account_id: int | None = None) -> PrimeStocksRuntimeStorePaths:
        resolved_uid = None if uid is None else uid.strip()
        if resolved_uid and account_id is not None:
            root = f"users/{resolved_uid}/accounts/{account_id}/prime_stocks/current"
        else:
            root = f"{self._settings.firestore_runtime_collection}/{self._settings.firestore_product_document}"
        return PrimeStocksRuntimeStorePaths(
            config_document=f"{root}/config/current",
            state_document=f"{root}/state/current",
            heartbeat_document=f"{root}/heartbeat/current",
            snapshot_document=f"{root}/snapshots/latest",
            signal_document=f"{root}/signals/latest",
            execution_document=f"{root}/execution/current",
            action_document=f"{root}/actions/latest",
            logs_collection=f"{root}/logs",
            signal_audit_collection=f"{root}/audit_signals",
            action_audit_collection=f"{root}/audit_actions",
            order_audit_collection=f"{root}/audit_orders",
            notifications_collection=f"{root}/notifications",
            notification_state_document=f"{root}/notification_state/current",
            ai_market_document=f"{root}/ai_market/current",
            ai_symbols_collection=f"{root}/ai_symbols",
        )

    def load_runtime_config(self, default_config: PrimeStocksRuntimeConfigRecord) -> PrimeStocksRuntimeConfigRecord:
        global_paths = self.get_paths()
        scoped_paths = self.get_paths(uid=default_config.uid, account_id=default_config.account_id)
        global_payload = self._load_document_payload(
            action="load global runtime config",
            path=global_paths.config_document,
            document_getter=lambda: self._config_document(global_paths).get(),
        )
        payload = dict(global_payload or {})
        if scoped_paths.config_document != global_paths.config_document:
            scoped_payload = self._load_document_payload(
                action="load scoped runtime config",
                path=scoped_paths.config_document,
                document_getter=lambda: self._config_document(scoped_paths).get(),
            )
            payload.update(scoped_payload or {})
        if global_payload:
            for field_name in [
                "enabled",
                "dry_run",
                "paper_execution_enabled",
                "live_execution_enabled",
                "ping_enabled",
                "ping_mode",
                "ping_daily_heartbeat_enabled",
                "test_mode",
                "test_trigger",
                "test_symbol_override",
                "force_candidate_action",
                "ai_validation_bypass_enabled",
                "global_kill_switch_enabled",
            ]:
                if field_name in global_payload:
                    payload[field_name] = global_payload[field_name]
        if not payload:
            if scoped_paths.config_document == global_paths.config_document:
                self._firestore_call(
                    action="seed default runtime config",
                    path=scoped_paths.config_document,
                    fn=lambda: self._config_document(scoped_paths).set(asdict(default_config), merge=True),
                )
            return default_config
        return PrimeStocksRuntimeConfigRecord(
            uid=_maybe_string(payload.get("uid", default_config.uid)),
            product_key=str(payload.get("product_key", default_config.product_key)),
            strategy_key=str(payload.get("strategy_key", default_config.strategy_key)),
            strategy_title=str(payload.get("strategy_title", default_config.strategy_title)),
            symbol=str(payload.get("symbol", default_config.symbol)).upper(),
            asset_type=_normalize_asset_type(str(payload.get("asset_type", default_config.asset_type))),
            enabled=bool(payload.get("enabled", default_config.enabled)),
            dry_run=bool(payload.get("dry_run", default_config.dry_run)),
            paper_execution_enabled=bool(payload.get("paper_execution_enabled", default_config.paper_execution_enabled)),
            live_execution_enabled=bool(payload.get("live_execution_enabled", default_config.live_execution_enabled)),
            ping_enabled=bool(payload.get("ping_enabled", default_config.ping_enabled)),
            ping_mode=_normalize_ping_mode(payload.get("ping_mode", default_config.ping_mode)),
            ping_daily_heartbeat_enabled=bool(
                payload.get("ping_daily_heartbeat_enabled", default_config.ping_daily_heartbeat_enabled)
            ),
            test_mode=bool(payload.get("test_mode", default_config.test_mode)),
            test_trigger=_maybe_string(payload.get("test_trigger", default_config.test_trigger)),
            test_symbol_override=_maybe_string(payload.get("test_symbol_override", default_config.test_symbol_override)),
            force_candidate_action=_maybe_string(
                payload.get("force_candidate_action", default_config.force_candidate_action)
            ),
            ai_validation_bypass_enabled=bool(
                payload.get("ai_validation_bypass_enabled", default_config.ai_validation_bypass_enabled)
            ),
            execution_timeframe=_normalize_runtime_timeframe(
                str(payload.get("execution_timeframe", default_config.execution_timeframe))
            ),
            trend_timeframe=_normalize_runtime_timeframe(
                str(payload.get("trend_timeframe", default_config.trend_timeframe))
            ),
            pullback_window=max(5, int(payload.get("pullback_window", default_config.pullback_window))),
            execution_bar_limit=int(payload.get("execution_bar_limit", default_config.execution_bar_limit)),
            trend_bar_limit=int(payload.get("trend_bar_limit", default_config.trend_bar_limit)),
            first_lot_notional=float(payload.get("first_lot_notional", default_config.first_lot_notional)),
            multi_notional=float(payload.get("multi_notional", default_config.multi_notional)),
            max_notional_per_order=float(payload.get("max_notional_per_order", default_config.max_notional_per_order)),
            max_total_notional_per_symbol=float(
                payload.get("max_total_notional_per_symbol", default_config.max_total_notional_per_symbol)
            ),
            max_add_count=max(0, int(payload.get("max_add_count", default_config.max_add_count))),
            daily_order_cap=_maybe_int(payload.get("daily_order_cap", default_config.daily_order_cap)),
            max_open_positions=_maybe_int(payload.get("max_open_positions", default_config.max_open_positions)),
            broker_retry_max_attempts=max(
                0,
                int(payload.get("broker_retry_max_attempts", default_config.broker_retry_max_attempts)),
            ),
            account_id=_maybe_int(payload.get("account_id", default_config.account_id)),
            alpaca_account_id=_maybe_int(payload.get("alpaca_account_id", default_config.alpaca_account_id)),
            runtime_target=str(payload.get("runtime_target", default_config.runtime_target)),
            entitlement=_normalize_entitlement_payload(payload.get("entitlement", default_config.entitlement)),
            safe_mode_enabled=bool(payload.get("safe_mode_enabled", default_config.safe_mode_enabled)),
            safe_mode_size_pct=_clamp_pct(
                payload.get("safe_mode_size_pct", default_config.safe_mode_size_pct),
                default=default_config.safe_mode_size_pct,
            ),
            live_cap_pct=_clamp_pct(
                payload.get("live_cap_pct", default_config.live_cap_pct),
                default=default_config.live_cap_pct,
            ),
            max_total_exposure_pct=_clamp_pct(
                payload.get("max_total_exposure_pct", default_config.max_total_exposure_pct),
                default=default_config.max_total_exposure_pct,
            ),
            per_symbol_entry_pct=_clamp_pct(
                payload.get("per_symbol_entry_pct", payload.get("live_cap_pct", default_config.per_symbol_entry_pct)),
                default=default_config.per_symbol_entry_pct,
            ),
            total_entry_exposure_cap_pct=_clamp_pct(
                payload.get("total_entry_exposure_cap_pct", default_config.total_entry_exposure_cap_pct),
                default=default_config.total_entry_exposure_cap_pct,
            ),
            total_add_exposure_cap_pct=_clamp_pct(
                payload.get(
                    "total_add_exposure_cap_pct",
                    payload.get("max_total_exposure_pct", default_config.total_add_exposure_cap_pct),
                ),
                default=default_config.total_add_exposure_cap_pct,
            ),
            global_kill_switch_enabled=bool(
                payload.get("global_kill_switch_enabled", default_config.global_kill_switch_enabled)
            ),
            account_kill_switch_enabled=bool(
                payload.get("account_kill_switch_enabled", default_config.account_kill_switch_enabled)
            ),
            selected_symbols=[
                str(symbol).strip().upper()
                for symbol in payload.get("selected_symbols", default_config.selected_symbols)
                if str(symbol).strip() != ""
            ],
            symbol_states=[
                item
                for item in payload.get("symbol_states", default_config.symbol_states)
                if isinstance(item, dict) and str(item.get("symbol", "")).strip() != ""
            ],
        )

    def load_latest_execution_record(self, *, uid: str | None = None, account_id: int | None = None) -> PrimeStocksLatestExecutionRecord:
        paths = self.get_paths(uid=uid, account_id=account_id)
        snapshot = self._firestore_call(
            action="load latest execution record",
            path=paths.execution_document,
            fn=lambda: self._execution_document(paths).get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        if not payload:
            return PrimeStocksLatestExecutionRecord()
        return PrimeStocksLatestExecutionRecord(
            execution_key=_maybe_string(payload.get("execution_key")),
            order_status=_maybe_string(payload.get("order_status")),
            run_id=_maybe_string(payload.get("run_id")),
            candidate_action=_maybe_string(payload.get("candidate_action")),
            latest_signal_time=_maybe_string(payload.get("latest_signal_time")),
        )

    def load_runtime_state_record(self, *, uid: str | None = None, account_id: int | None = None) -> PrimeStocksRuntimeStateRecord:
        paths = self.get_paths(uid=uid, account_id=account_id)
        snapshot = self._firestore_call(
            action="load runtime state record",
            path=paths.state_document,
            fn=lambda: self._state_document(paths).get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        if not payload:
            return PrimeStocksRuntimeStateRecord()
        return PrimeStocksRuntimeStateRecord(
            run_id=_maybe_string(payload.get("run_id")),
            uid=_maybe_string(payload.get("uid")),
            account_id=_maybe_int(payload.get("account_id")),
            alpaca_account_id=_maybe_int(payload.get("alpaca_account_id")),
            broker_environment=_maybe_string(payload.get("broker_environment")),
            symbol=_maybe_string(payload.get("symbol")),
            position_open=bool(payload.get("position_open", False)),
            position_size=float(payload.get("position_size", 0.0)),
            position_avg_price=_maybe_float(payload.get("position_avg_price")),
            dollars_used=float(payload.get("dollars_used", 0.0)),
            add_count=max(0, int(payload.get("add_count", 0))),
            add_tiers_filled=_maybe_int_list(payload.get("add_tiers_filled")),
            last_add_price=_maybe_float(payload.get("last_add_price")),
            pos_high=_maybe_float(payload.get("pos_high")),
            trail_stop=_maybe_float(payload.get("trail_stop")),
            last_entry_time=_maybe_string(payload.get("last_entry_time")),
            last_exit_time=_maybe_string(payload.get("last_exit_time")),
            last_action=_maybe_string(payload.get("last_action")),
            candidate_action=_maybe_string(payload.get("candidate_action")),
            execution_key=_maybe_string(payload.get("execution_key")),
            last_processed_bar_time=_maybe_string(payload.get("last_processed_bar_time")),
            latest_signal_time=_maybe_string(payload.get("latest_signal_time")),
            latest_candidate_action=_maybe_string(payload.get("latest_candidate_action")),
            latest_status=_maybe_string(payload.get("latest_status")),
            latest_execution_decision=_maybe_string(payload.get("latest_execution_decision")),
            current_total_exposure_pct=_maybe_float(payload.get("current_total_exposure_pct")),
        )

    def load_heartbeat_record(self, *, uid: str | None = None, account_id: int | None = None) -> dict[str, Any] | None:
        paths = self.get_paths(uid=uid, account_id=account_id)
        snapshot = self._firestore_call(
            action="load runtime heartbeat record",
            path=paths.heartbeat_document,
            fn=lambda: self._heartbeat_document(paths).get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        return payload if isinstance(payload, dict) else None

    def count_submitted_orders_for_day(
        self,
        *,
        day_start: datetime,
        day_end: datetime,
        uid: str | None,
        account_id: int | None,
    ) -> int:
        if hasattr(self._client_or_default(), "storage"):
            return self._count_submitted_orders_from_fake_storage(
                day_start=day_start,
                day_end=day_end,
                uid=uid,
                account_id=account_id,
            )
        paths = self.get_paths(uid=uid, account_id=account_id)
        query = (
            self._logs_collection(paths)
            .where("kind", "==", "runtime_execution")
            .where("execution.submitted", "==", True)
            .where("created_at", ">=", day_start.astimezone(UTC).isoformat())
            .where("created_at", "<", day_end.astimezone(UTC).isoformat())
        )
        if account_id is not None:
            query = query.where("account_id", "==", account_id)
        snapshots = self._firestore_call(
            action="count submitted orders for day",
            path=paths.logs_collection,
            fn=lambda: list(query.stream()),
        )
        return len(snapshots)

    def load_ai_market_record(self) -> AiCacheRecord | None:
        snapshot = self._firestore_call(
            action="load ai market record",
            path=self.get_paths().ai_market_document,
            fn=lambda: self._ai_market_document().get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        return None if not payload else _deserialize_ai_cache_record(payload)

    def load_ai_symbol_record(self, symbol: str) -> AiCacheRecord | None:
        resolved_symbol = symbol.strip().upper()
        path = f"{self.get_paths().ai_symbols_collection}/{resolved_symbol}"
        snapshot = self._firestore_call(
            action="load ai symbol record",
            path=path,
            fn=lambda: self._ai_symbols_collection().document(resolved_symbol).get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        return None if not payload else _deserialize_ai_cache_record(payload)

    def write_ai_market_record(self, record: AiCacheRecord) -> None:
        payload = _serialize_ai_cache_record(record)
        self._firestore_call(
            action="write ai market record",
            path=self.get_paths().ai_market_document,
            fn=lambda: self._ai_market_document().set(payload, merge=True),
        )

    def write_ai_symbol_record(self, record: AiCacheRecord) -> None:
        resolved_symbol = (record.symbol or "").strip().upper()
        if resolved_symbol == "":
            raise PrimeStocksRuntimeStoreError("AI symbol record requires a non-empty symbol.")
        path = f"{self.get_paths().ai_symbols_collection}/{resolved_symbol}"
        payload = _serialize_ai_cache_record(record)
        self._firestore_call(
            action="write ai symbol record",
            path=path,
            fn=lambda: self._ai_symbols_collection().document(resolved_symbol).set(payload, merge=True),
        )

    def write_runtime_result(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        account_context: ResolvedAlpacaAccountContext,
        strategy_result: PrimeStocksStrategyResult,
        candidate_action: str,
        latest_signal_time: datetime | None,
        runtime_message: str,
        execution_mode: str,
        execution_decision: str,
        execution_result: AlpacaPaperExecutionResult | None,
        skipped_reason: str | None,
        trigger_type: str,
        trigger_source: str,
        ai_decision: PrimeStocksAiDecision | None = None,
        retry_count: int = 0,
        broker_error_code: str | None = None,
        broker_error_message: str | None = None,
        state_record: PrimeStocksRuntimeStateRecord | None = None,
        test_mode: bool = False,
        test_trigger: str | None = None,
        symbol_override: str | None = None,
        validation_only: bool = False,
    ) -> None:
        now = datetime.now(tz=UTC)
        serialized_signal = _serialize_signal(strategy_result)
        serialized_ai = serialize_ai_decision(ai_decision or strategy_result.ai_decision)
        serialized_execution = _serialize_execution(
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            execution_mode=execution_mode,
            execution_decision=execution_decision,
            execution_result=execution_result,
            skipped_reason=skipped_reason,
            ai_decision=ai_decision or strategy_result.ai_decision,
            retry_count=retry_count,
            broker_error_code=broker_error_code,
            broker_error_message=broker_error_message,
        )
        failure_markers = _resolve_failure_markers(
            execution_decision=execution_decision,
            blocked_reason=serialized_execution["blocked_reason"],
            broker_error_code=serialized_execution["broker_error_code"],
            broker_error_message=serialized_execution["broker_error_message"],
            runtime_message=runtime_message,
            now=now,
        )
        persisted_state = state_record or PrimeStocksRuntimeStateRecord(
            run_id=run_id,
            uid=account_context.uid,
            account_id=runtime_config.account_id,
            alpaca_account_id=runtime_config.alpaca_account_id,
            broker_environment=account_context.environment,
            symbol=runtime_config.symbol,
            position_open=strategy_result.final_state.position_size > 0,
            position_size=strategy_result.final_state.position_size,
            position_avg_price=strategy_result.final_state.position_avg_price,
            dollars_used=strategy_result.final_state.dollars_used,
            add_count=strategy_result.final_state.add_count,
            add_tiers_filled=list(range(1, strategy_result.final_state.add_count + 1)),
            last_add_price=strategy_result.final_state.last_add_price,
            pos_high=strategy_result.final_state.pos_high,
            trail_stop=strategy_result.final_state.trail_stop,
            last_action=candidate_action,
            candidate_action=candidate_action,
            execution_key=serialized_execution["execution_key"],
            last_processed_bar_time=_isoformat_or_none(latest_signal_time),
            latest_signal_time=_isoformat_or_none(latest_signal_time),
            latest_candidate_action=candidate_action,
            latest_status=strategy_result.status,
            latest_execution_decision=execution_decision,
        )
        latest_snapshot = {
            "run_id": run_id,
            "uid": account_context.uid,
            "product_key": runtime_config.product_key,
            "strategy_key": runtime_config.strategy_key,
            "symbol": runtime_config.symbol,
            "asset_type": runtime_config.asset_type,
            "status": strategy_result.status,
            "runtime_target": runtime_config.runtime_target,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "broker_environment": account_context.environment,
            "dry_run": runtime_config.dry_run,
            "paper_execution_enabled": runtime_config.paper_execution_enabled,
            "live_execution_enabled": runtime_config.live_execution_enabled,
            "safe_mode_enabled": runtime_config.safe_mode_enabled,
            "safe_mode_size_pct": runtime_config.safe_mode_size_pct,
            "live_cap_pct": runtime_config.live_cap_pct,
            "max_total_exposure_pct": runtime_config.max_total_exposure_pct,
            "per_symbol_entry_pct": runtime_config.per_symbol_entry_pct,
            "total_entry_exposure_cap_pct": runtime_config.total_entry_exposure_cap_pct,
            "total_add_exposure_cap_pct": runtime_config.total_add_exposure_cap_pct,
            "current_total_exposure_pct": persisted_state.current_total_exposure_pct,
            "global_kill_switch_enabled": runtime_config.global_kill_switch_enabled,
            "account_kill_switch_enabled": runtime_config.account_kill_switch_enabled,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "runtime_message": runtime_message,
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "candidate_action": candidate_action,
            "execution_timeframe": strategy_result.execution_timeframe,
            "trend_timeframe": strategy_result.trend_timeframe,
            "pullback_window": runtime_config.pullback_window,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "updated_at": now.isoformat(),
            "strategy_message": strategy_result.message,
            "execution_allowed": serialized_execution["execution_allowed"],
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "failed_at": failure_markers["failed_at"],
            "recovered_at": failure_markers["recovered_at"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "state": _serialize_runtime_state(persisted_state),
            "signal": serialized_signal,
            "execution": serialized_execution,
            "ai": serialized_ai,
        }
        latest_signal = {
            "run_id": run_id,
            "uid": account_context.uid,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "signal": serialized_signal,
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "updated_at": now.isoformat(),
            "execution_allowed": serialized_execution["execution_allowed"],
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "failed_at": failure_markers["failed_at"],
            "recovered_at": failure_markers["recovered_at"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "ai": serialized_ai,
        }
        current_state = {
            "run_id": run_id,
            "uid": account_context.uid,
            "enabled": runtime_config.enabled,
            "dry_run": runtime_config.dry_run,
            "paper_execution_enabled": runtime_config.paper_execution_enabled,
            "live_execution_enabled": runtime_config.live_execution_enabled,
            "safe_mode_enabled": runtime_config.safe_mode_enabled,
            "safe_mode_size_pct": runtime_config.safe_mode_size_pct,
            "live_cap_pct": runtime_config.live_cap_pct,
            "max_total_exposure_pct": runtime_config.max_total_exposure_pct,
            "per_symbol_entry_pct": runtime_config.per_symbol_entry_pct,
            "total_entry_exposure_cap_pct": runtime_config.total_entry_exposure_cap_pct,
            "total_add_exposure_cap_pct": runtime_config.total_add_exposure_cap_pct,
            "current_total_exposure_pct": persisted_state.current_total_exposure_pct,
            "global_kill_switch_enabled": runtime_config.global_kill_switch_enabled,
            "account_kill_switch_enabled": runtime_config.account_kill_switch_enabled,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "symbol": persisted_state.symbol,
            "position_open": persisted_state.position_open,
            "position_size": persisted_state.position_size,
            "position_avg_price": persisted_state.position_avg_price,
            "dollars_used": persisted_state.dollars_used,
            "add_count": persisted_state.add_count,
            "add_tiers_filled": persisted_state.add_tiers_filled,
            "last_add_price": persisted_state.last_add_price,
            "pos_high": persisted_state.pos_high,
            "trail_stop": persisted_state.trail_stop,
            "last_entry_time": persisted_state.last_entry_time,
            "last_exit_time": persisted_state.last_exit_time,
            "last_action": persisted_state.last_action,
            "candidate_action": persisted_state.candidate_action,
            "execution_key": persisted_state.execution_key,
            "last_processed_bar_time": persisted_state.last_processed_bar_time,
            "latest_signal_time": persisted_state.latest_signal_time,
            "latest_candidate_action": persisted_state.latest_candidate_action,
            "latest_status": persisted_state.latest_status,
            "runtime_target": runtime_config.runtime_target,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "broker_environment": account_context.environment,
            "latest_execution_decision": persisted_state.latest_execution_decision,
            "latest_order_status": serialized_execution["order_status"],
            "execution_allowed": serialized_execution["execution_allowed"],
            "blocked_reason": serialized_execution["blocked_reason"],
            "safe_mode_enabled": runtime_config.safe_mode_enabled,
            "safe_mode_size_pct": runtime_config.safe_mode_size_pct,
            "live_cap_pct": runtime_config.live_cap_pct,
            "max_total_exposure_pct": runtime_config.max_total_exposure_pct,
            "global_kill_switch_enabled": runtime_config.global_kill_switch_enabled,
            "account_kill_switch_enabled": runtime_config.account_kill_switch_enabled,
            "latest_retry_count": serialized_execution["retry_count"],
            "latest_broker_error_code": serialized_execution["broker_error_code"],
            "latest_broker_error_message": serialized_execution["broker_error_message"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "failed_at": failure_markers["failed_at"],
            "recovered_at": failure_markers["recovered_at"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "service_name": self._settings.cloud_run_service_name,
            "service_revision": self._settings.cloud_run_revision,
            "updated_at": now.isoformat(),
            "ai": serialized_ai,
        }
        execution_current = {
            "run_id": run_id,
            "uid": account_context.uid,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "execution_key": serialized_execution["execution_key"],
            "execution_mode": execution_mode,
            "execution_decision": execution_decision,
            "order_status": serialized_execution["order_status"],
            "order_id": serialized_execution["order_id"],
            "client_order_id": serialized_execution["client_order_id"],
            "submitted": serialized_execution["submitted"],
            "skipped_reason": serialized_execution["skipped_reason"],
            "blocked_reason": serialized_execution["blocked_reason"],
            "add_tier": serialized_execution["add_tier"],
            "execution_allowed": serialized_execution["execution_allowed"],
            "per_symbol_entry_pct": runtime_config.per_symbol_entry_pct,
            "total_entry_exposure_cap_pct": runtime_config.total_entry_exposure_cap_pct,
            "total_add_exposure_cap_pct": runtime_config.total_add_exposure_cap_pct,
            "current_total_exposure_pct": persisted_state.current_total_exposure_pct,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "exit_reason": serialized_execution["exit_reason"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "failed_at": failure_markers["failed_at"],
            "recovered_at": failure_markers["recovered_at"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "service_name": self._settings.cloud_run_service_name,
            "service_revision": self._settings.cloud_run_revision,
            "updated_at": now.isoformat(),
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "broker_environment": account_context.environment,
            "service_name": self._settings.cloud_run_service_name,
            "service_revision": self._settings.cloud_run_revision,
            "ai": serialized_ai,
        }
        latest_action = {
            "run_id": run_id,
            "uid": account_context.uid,
            "candidate_action": candidate_action,
            "execution_decision": execution_decision,
            "execution_mode": execution_mode,
            "execution": serialized_execution,
            "blocked_reason": serialized_execution["blocked_reason"],
            "execution_allowed": serialized_execution["execution_allowed"],
            "safe_mode_enabled": runtime_config.safe_mode_enabled,
            "safe_mode_size_pct": runtime_config.safe_mode_size_pct,
            "live_cap_pct": runtime_config.live_cap_pct,
            "max_total_exposure_pct": runtime_config.max_total_exposure_pct,
            "per_symbol_entry_pct": runtime_config.per_symbol_entry_pct,
            "total_entry_exposure_cap_pct": runtime_config.total_entry_exposure_cap_pct,
            "total_add_exposure_cap_pct": runtime_config.total_add_exposure_cap_pct,
            "current_total_exposure_pct": persisted_state.current_total_exposure_pct,
            "global_kill_switch_enabled": runtime_config.global_kill_switch_enabled,
            "account_kill_switch_enabled": runtime_config.account_kill_switch_enabled,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "exit_reason": serialized_execution["exit_reason"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "updated_at": now.isoformat(),
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "broker_environment": account_context.environment,
            "service_name": self._settings.cloud_run_service_name,
            "service_revision": self._settings.cloud_run_revision,
            "ai": serialized_ai,
        }
        log_payload = {
            "run_id": run_id,
            "uid": account_context.uid,
            "level": "INFO",
            "kind": "runtime_execution",
            "message": runtime_message,
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "symbol": runtime_config.symbol,
            "status": strategy_result.status,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "execution_decision": execution_decision,
            "execution_mode": execution_mode,
            "execution": serialized_execution,
            "blocked_reason": serialized_execution["blocked_reason"],
            "execution_allowed": serialized_execution["execution_allowed"],
            "per_symbol_entry_pct": runtime_config.per_symbol_entry_pct,
            "total_entry_exposure_cap_pct": runtime_config.total_entry_exposure_cap_pct,
            "total_add_exposure_cap_pct": runtime_config.total_add_exposure_cap_pct,
            "current_total_exposure_pct": persisted_state.current_total_exposure_pct,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "symbol_override": symbol_override,
            "validation_only": validation_only,
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "exit_reason": serialized_execution["exit_reason"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "failed_at": failure_markers["failed_at"],
            "recovered_at": failure_markers["recovered_at"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "broker_environment": account_context.environment,
            "service_name": self._settings.cloud_run_service_name,
            "service_revision": self._settings.cloud_run_revision,
            "created_at": now.isoformat(),
            "ai": serialized_ai,
        }
        signal_audit = {
            "run_id": run_id,
            "uid": account_context.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "execution_decision": execution_decision,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "Ai_regime_label": None if serialized_ai is None else serialized_ai["Ai_regime_label"],
            "Ai_sentiment_label": None if serialized_ai is None else serialized_ai["Ai_sentiment_label"],
            "Ai_safety_label": None if serialized_ai is None else serialized_ai["Ai_safety_label"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "blocked_reason": serialized_execution["blocked_reason"],
            "skipped_reason": serialized_execution["skipped_reason"],
            "signal": serialized_signal,
            "created_at": now.isoformat(),
            "broker_environment": account_context.environment,
        }
        action_audit = {
            "run_id": run_id,
            "uid": account_context.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "execution_decision": execution_decision,
            "blocked_reason": serialized_execution["blocked_reason"],
            "skipped_reason": serialized_execution["skipped_reason"],
            "retry_count": serialized_execution["retry_count"],
            "last_error_code": failure_markers["last_error_code"],
            "last_error_message": failure_markers["last_error_message"],
            "recovery_action": failure_markers["recovery_action"],
            "exit_reason": serialized_execution["exit_reason"],
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "Ai_regime_label": None if serialized_ai is None else serialized_ai["Ai_regime_label"],
            "Ai_sentiment_label": None if serialized_ai is None else serialized_ai["Ai_sentiment_label"],
            "Ai_safety_label": None if serialized_ai is None else serialized_ai["Ai_safety_label"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "created_at": now.isoformat(),
            "broker_environment": account_context.environment,
        }
        order_audit = {
            "run_id": run_id,
            "uid": account_context.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": runtime_config.alpaca_account_id,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "execution_decision": execution_decision,
            "order_id": serialized_execution["order_id"],
            "client_order_id": serialized_execution["client_order_id"],
            "order_status": serialized_execution["order_status"],
            "side": serialized_execution["side"],
            "notional": serialized_execution["notional"],
            "qty": serialized_execution["qty"],
            "add_tier": serialized_execution["add_tier"],
            "submitted": serialized_execution["submitted"],
            "blocked_reason": serialized_execution["blocked_reason"],
            "skipped_reason": serialized_execution["skipped_reason"],
            "retry_count": serialized_execution["retry_count"],
            "broker_error_code": serialized_execution["broker_error_code"],
            "broker_error_message": serialized_execution["broker_error_message"],
            "exit_reason": serialized_execution["exit_reason"],
            "Ai_regime_label": None if serialized_ai is None else serialized_ai["Ai_regime_label"],
            "Ai_sentiment_label": None if serialized_ai is None else serialized_ai["Ai_sentiment_label"],
            "Ai_safety_label": None if serialized_ai is None else serialized_ai["Ai_safety_label"],
            "Ai_execution_allowed": None if serialized_ai is None else serialized_ai["Ai_execution_allowed"],
            "Ai_blocked_reason": None if serialized_ai is None else serialized_ai["Ai_blocked_reason"],
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "created_at": now.isoformat(),
        }
        notification_context = _build_notification_payload(
            enabled=self._settings.prime_stocks_notifications_enabled,
            run_id=run_id,
            runtime_config=runtime_config,
            account_context=account_context,
            candidate_action=candidate_action,
            execution_decision=execution_decision,
            latest_signal_time=latest_signal_time,
            serialized_execution=serialized_execution,
            serialized_ai=serialized_ai,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            now=now,
            status=strategy_result.status,
            runtime_message=runtime_message,
            service_name=self._settings.cloud_run_service_name,
            service_revision=self._settings.cloud_run_revision,
        )
        paths = self.get_paths(uid=account_context.uid, account_id=runtime_config.account_id)
        self._firestore_call(
            action="write runtime snapshot",
            path=paths.snapshot_document,
            fn=lambda: self._snapshot_document(paths).set(latest_snapshot, merge=True),
        )
        self._firestore_call(
            action="write runtime signal",
            path=paths.signal_document,
            fn=lambda: self._signal_document(paths).set(latest_signal, merge=True),
        )
        self._firestore_call(
            action="write runtime state",
            path=paths.state_document,
            fn=lambda: self._state_document(paths).set(current_state, merge=True),
        )
        if test_trigger == "ping":
            self.write_runtime_heartbeat(
                run_id=run_id,
                test_mode=test_mode,
                uid=account_context.uid,
                account_id=runtime_config.account_id,
            )
        self._firestore_call(
            action="write latest execution",
            path=paths.execution_document,
            fn=lambda: self._execution_document(paths).set(execution_current, merge=True),
        )
        self._firestore_call(
            action="write latest action",
            path=paths.action_document,
            fn=lambda: self._action_document(paths).set(latest_action, merge=True),
        )
        log_document_path = f"{paths.logs_collection}/{run_id}"
        self._firestore_call(
            action="write runtime log",
            path=log_document_path,
            fn=lambda: self._logs_collection(paths).document(run_id).set(log_payload, merge=True),
        )
        self._firestore_call(
            action="write runtime signal audit",
            path=f"{paths.signal_audit_collection}/{run_id}",
            fn=lambda: self._signal_audit_collection(paths).document(run_id).set(signal_audit, merge=True),
        )
        self._firestore_call(
            action="write runtime action audit",
            path=f"{paths.action_audit_collection}/{run_id}",
            fn=lambda: self._action_audit_collection(paths).document(run_id).set(action_audit, merge=True),
        )
        self._firestore_call(
            action="write runtime order audit",
            path=f"{paths.order_audit_collection}/{run_id}",
            fn=lambda: self._order_audit_collection(paths).document(run_id).set(order_audit, merge=True),
        )
        if notification_context is not None:
            previous_notification_state = self._load_document_payload(
                action="load runtime notification state",
                path=paths.notification_state_document,
                document_getter=lambda: self._notification_state_document(paths).get(),
            ) or {}
            previous_key = _maybe_string(previous_notification_state.get("last_notification_key"))
            suppressed = previous_key == notification_context["notification_key"]
            delivery_status = "suppressed_duplicate" if suppressed else "written"
            notification_context["delivery"] = {
                "status": delivery_status,
                "channels": ["firestore", "webhook_ready"],
                "email": "not_configured",
                "webhook": "ready_payload_only",
            }
            if not suppressed:
                self._firestore_call(
                    action="write runtime notification",
                    path=f"{paths.notifications_collection}/{run_id}",
                    fn=lambda: self._notifications_collection(paths).document(run_id).set(notification_context, merge=True),
                )
                self._firestore_call(
                    action="write runtime notification state",
                    path=paths.notification_state_document,
                    fn=lambda: self._notification_state_document(paths).set(
                        {
                            "last_notification_key": notification_context["notification_key"],
                            "last_notification_run_id": run_id,
                            "last_notification_at": now.isoformat(),
                            "last_notification_event": notification_context["event_type"],
                            "delivery_status": delivery_status,
                        },
                        merge=True,
                    ),
                )
            self._firestore_call(
                action="write latest action notification markers",
                path=paths.action_document,
                fn=lambda: self._action_document(paths).set(
                    {
                        "notification_delivery_result": notification_context["delivery"],
                        "notification_event_type": notification_context["event_type"],
                        "notification_key": notification_context["notification_key"],
                    },
                    merge=True,
                ),
            )
            self._firestore_call(
                action="write latest execution notification markers",
                path=paths.execution_document,
                fn=lambda: self._execution_document(paths).set(
                    {
                        "notification_delivery_result": notification_context["delivery"],
                        "notification_event_type": notification_context["event_type"],
                        "notification_key": notification_context["notification_key"],
                    },
                    merge=True,
                ),
            )
            self._firestore_call(
                action="write runtime state notification markers",
                path=paths.state_document,
                fn=lambda: self._state_document(paths).set(
                    {
                        "notification_delivery_result": notification_context["delivery"],
                        "notification_event_type": notification_context["event_type"],
                        "notification_key": notification_context["notification_key"],
                    },
                    merge=True,
                ),
            )

    def write_runtime_heartbeat(
        self,
        *,
        run_id: str,
        status: str = "ok",
        test_mode: bool = True,
        uid: str | None = None,
        account_id: int | None = None,
    ) -> None:
        now = datetime.now(tz=UTC)
        heartbeat_payload = {
            "last_ping_at": now.isoformat(),
            "run_id": run_id,
            "status": status,
            "test_mode": test_mode,
        }
        paths = self.get_paths(uid=uid, account_id=account_id)
        self._firestore_call(
            action="write runtime heartbeat",
            path=paths.heartbeat_document,
            fn=lambda: self._heartbeat_document(paths).set(heartbeat_payload, merge=True),
        )

    def create_run_id(self) -> str:
        return f"run-{uuid4().hex[:15]}"

    def _config_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.config_document)

    def _state_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.state_document)

    def _snapshot_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.snapshot_document)

    def _heartbeat_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.heartbeat_document)

    def _signal_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.signal_document)

    def _execution_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.execution_document)

    def _action_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.action_document)

    def _logs_collection(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._collection_ref(paths.logs_collection)

    def _signal_audit_collection(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._collection_ref(paths.signal_audit_collection)

    def _action_audit_collection(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._collection_ref(paths.action_audit_collection)

    def _order_audit_collection(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._collection_ref(paths.order_audit_collection)

    def _notifications_collection(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._collection_ref(paths.notifications_collection)

    def _notification_state_document(self, paths: PrimeStocksRuntimeStorePaths) -> Any:
        return self._document_ref(paths.notification_state_document)

    def _ai_market_document(self) -> Any:
        return self._document_ref(self.get_paths().ai_market_document)

    def _ai_symbols_collection(self) -> Any:
        return self._collection_ref(self.get_paths().ai_symbols_collection)

    def _count_submitted_orders_from_fake_storage(
        self,
        *,
        day_start: datetime,
        day_end: datetime,
        uid: str | None,
        account_id: int | None,
    ) -> int:
        client = self._client_or_default()
        storage = getattr(client, "storage", {})
        if uid is not None and uid.strip() and account_id is not None:
            logs = (
                storage.get("users", {})
                .get(uid.strip(), {})
                .get("accounts", {})
                .get(str(account_id), {})
                .get("prime_stocks", {})
                .get("current", {})
                .get("logs", {})
            )
        else:
            logs = (
                storage.get(self._settings.firestore_runtime_collection, {})
                .get(self._settings.firestore_product_document, {})
                .get("logs", {})
            )
        count = 0
        for payload in logs.values():
            if not isinstance(payload, dict):
                continue
            if payload.get("kind") != "runtime_execution":
                continue
            execution_payload = payload.get("execution") or {}
            if not execution_payload.get("submitted", False):
                continue
            if account_id is not None and payload.get("account_id") != account_id:
                continue
            created_at = _maybe_string(payload.get("created_at"))
            if created_at is None:
                continue
            created_at_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(UTC)
            if day_start.astimezone(UTC) <= created_at_dt < day_end.astimezone(UTC):
                count += 1
        return count

    def _document_ref(self, path: str) -> Any:
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) % 2 != 0:
            raise PrimeStocksRuntimeStoreError(f"Invalid Firestore document path '{path}'.")
        ref = self._client_or_default()
        for index, segment in enumerate(segments):
            ref = ref.collection(segment) if index % 2 == 0 else ref.document(segment)
        return ref

    def _collection_ref(self, path: str) -> Any:
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) % 2 != 1:
            raise PrimeStocksRuntimeStoreError(f"Invalid Firestore collection path '{path}'.")
        ref = self._client_or_default()
        for index, segment in enumerate(segments):
            ref = ref.collection(segment) if index % 2 == 0 else ref.document(segment)
        return ref

    def _client_or_default(self) -> FirestoreClientProtocol:
        if self._client is not None:
            return self._client
        try:
            from google.cloud import firestore
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-firestore is required for Prime Stocks runtime state persistence."
            ) from exc
        try:
            self._client = firestore.Client(
                project=self._settings.firestore_project_id,
                database=self._settings.firestore_database_id,
            )
        except Exception as exc:
            raise PrimeStocksRuntimeStoreError(
                "Failed to initialize Firestore client for Prime Stocks runtime control state."
            ) from exc
        return self._client

    def _firestore_call(self, *, action: str, path: str, fn: Callable[[], T]) -> T:
        try:
            return fn()
        except PrimeStocksRuntimeStoreError:
            raise
        except Exception as exc:
            raise PrimeStocksRuntimeStoreError(
                f"Failed to {action} at Firestore path '{path}'."
            ) from exc

    def _load_document_payload(
        self,
        *,
        action: str,
        path: str,
        document_getter: Callable[[], Any],
    ) -> dict[str, Any] | None:
        snapshot = self._firestore_call(
            action=action,
            path=path,
            fn=document_getter,
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        return payload if isinstance(payload, dict) else None


def build_default_runtime_config(settings: AppConfig) -> PrimeStocksRuntimeConfigRecord:
    return PrimeStocksRuntimeConfigRecord(
        product_key="stocks.bismel1",
        strategy_key="prime_stocks",
        strategy_title="Prime Stocks Bot Trader",
        symbol=settings.prime_stocks_default_symbol.upper(),
        asset_type=_normalize_asset_type(settings.prime_stocks_asset_type),
        enabled=settings.prime_stocks_runtime_enabled,
        dry_run=settings.prime_stocks_dry_run,
        paper_execution_enabled=settings.prime_stocks_paper_execution_enabled,
        live_execution_enabled=settings.prime_stocks_live_execution_enabled,
        ping_enabled=False,
        ping_mode="off",
        ping_daily_heartbeat_enabled=False,
        test_mode=settings.prime_stocks_test_mode,
        test_trigger=settings.prime_stocks_test_trigger,
        test_symbol_override=None if settings.prime_stocks_test_symbol_override is None else settings.prime_stocks_test_symbol_override.upper(),
        force_candidate_action=settings.prime_stocks_force_candidate_action,
        ai_validation_bypass_enabled=settings.prime_stocks_ai_validation_bypass_enabled,
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=settings.prime_stocks_execution_bar_limit,
        trend_bar_limit=settings.prime_stocks_trend_bar_limit,
        first_lot_notional=settings.prime_stocks_first_lot_notional,
        multi_notional=settings.prime_stocks_multi_notional,
        max_notional_per_order=settings.prime_stocks_max_notional_per_order,
        max_total_notional_per_symbol=settings.prime_stocks_max_total_notional_per_symbol,
        max_add_count=settings.prime_stocks_max_add_count,
        daily_order_cap=settings.prime_stocks_daily_order_cap,
        max_open_positions=settings.prime_stocks_max_open_positions,
        broker_retry_max_attempts=settings.prime_stocks_broker_retry_max_attempts,
        account_id=None,
        alpaca_account_id=None,
        entitlement={},
        safe_mode_enabled=settings.prime_stocks_safe_mode_enabled,
        safe_mode_size_pct=settings.prime_stocks_safe_mode_size_pct,
        live_cap_pct=settings.prime_stocks_live_cap_pct,
        max_total_exposure_pct=settings.prime_stocks_max_total_exposure_pct,
        per_symbol_entry_pct=settings.prime_stocks_live_cap_pct,
        total_entry_exposure_cap_pct=settings.prime_stocks_total_entry_exposure_cap_pct,
        total_add_exposure_cap_pct=settings.prime_stocks_total_add_exposure_cap_pct,
        global_kill_switch_enabled=settings.prime_stocks_global_kill_switch_enabled,
        account_kill_switch_enabled=False,
        selected_symbols=[],
        symbol_states=[],
    )


def build_prime_stocks_runtime_bootstrap_documents(
    settings: AppConfig,
    *,
    updated_at: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    now = datetime.now(tz=UTC) if updated_at is None else updated_at.astimezone(UTC)
    default_runtime_config = build_default_runtime_config(settings)
    signal_payload = {
        "base_entry_signal": False,
        "base_entry_trigger": False,
        "add_signal_raw": False,
        "add_trigger": False,
        "hit_atr_trail": False,
        "hit_regime": False,
        "pause_new_basket": False,
        "pause_adds": False,
        "regime_fail": False,
        "auto_paused": False,
    }
    state_payload = {
        "position_open": False,
        "position_size": 0.0,
        "position_avg_price": None,
        "add_count": 0,
        "add_tiers_filled": [],
        "last_add_price": None,
        "dollars_used": 0.0,
        "pos_high": None,
        "trail_stop": None,
        "last_entry_time": None,
        "last_exit_time": None,
        "last_action": "BOOTSTRAPPED",
        "candidate_action": "BOOTSTRAPPED",
        "execution_key": "BOOTSTRAPPED:none",
        "last_processed_bar_time": None,
        "latest_signal_time": None,
    }
    execution_payload = _serialize_execution(
        candidate_action="BOOTSTRAPPED",
        latest_signal_time=None,
        execution_mode="bootstrapped",
        execution_decision="not_started",
        execution_result=None,
        skipped_reason="bootstrap_seeded",
    )
    return {
        "config/current": asdict(default_runtime_config),
        "state/current": {
            "run_id": BOOTSTRAP_RUN_ID,
            "enabled": default_runtime_config.enabled,
            "dry_run": default_runtime_config.dry_run,
            "paper_execution_enabled": default_runtime_config.paper_execution_enabled,
            "live_execution_enabled": default_runtime_config.live_execution_enabled,
            "test_mode": False,
            "test_trigger": None,
            "symbol_override": None,
            "validation_only": False,
            "account_id": None,
            "alpaca_account_id": None,
            "broker_environment": None,
            "symbol": default_runtime_config.symbol,
            "position_open": state_payload["position_open"],
            "position_size": state_payload["position_size"],
            "position_avg_price": state_payload["position_avg_price"],
            "dollars_used": state_payload["dollars_used"],
            "add_count": state_payload["add_count"],
            "add_tiers_filled": state_payload["add_tiers_filled"],
            "last_add_price": state_payload["last_add_price"],
            "pos_high": state_payload["pos_high"],
            "trail_stop": state_payload["trail_stop"],
            "last_entry_time": state_payload["last_entry_time"],
            "last_exit_time": state_payload["last_exit_time"],
            "last_action": state_payload["last_action"],
            "candidate_action": state_payload["candidate_action"],
            "execution_key": state_payload["execution_key"],
            "last_processed_bar_time": None,
            "latest_signal_time": state_payload["latest_signal_time"],
            "latest_candidate_action": "BOOTSTRAPPED",
            "latest_status": "initialized",
            "runtime_target": default_runtime_config.runtime_target,
            "latest_execution_decision": "not_started",
            "latest_order_status": execution_payload["order_status"],
            "execution_allowed": execution_payload["execution_allowed"],
            "blocked_reason": execution_payload["blocked_reason"],
            "latest_retry_count": execution_payload["retry_count"],
            "latest_broker_error_code": execution_payload["broker_error_code"],
            "latest_broker_error_message": execution_payload["broker_error_message"],
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
            "trigger_type": "bootstrap",
            "trigger_source": "firestore_seed",
            "updated_at": now.isoformat(),
        },
        "snapshots/latest": {
            "run_id": BOOTSTRAP_RUN_ID,
            "product_key": default_runtime_config.product_key,
            "strategy_key": default_runtime_config.strategy_key,
            "symbol": default_runtime_config.symbol,
            "asset_type": default_runtime_config.asset_type,
            "status": "initialized",
            "runtime_target": default_runtime_config.runtime_target,
            "dry_run": default_runtime_config.dry_run,
            "paper_execution_enabled": default_runtime_config.paper_execution_enabled,
            "live_execution_enabled": default_runtime_config.live_execution_enabled,
            "test_mode": False,
            "test_trigger": None,
            "symbol_override": None,
            "validation_only": False,
            "runtime_message": "Prime Stocks Firestore runtime bootstrap seeded default documents.",
            "trigger_type": "bootstrap",
            "trigger_source": "firestore_seed",
            "candidate_action": "BOOTSTRAPPED",
            "execution_timeframe": default_runtime_config.execution_timeframe,
            "trend_timeframe": default_runtime_config.trend_timeframe,
            "pullback_window": default_runtime_config.pullback_window,
            "latest_signal_time": None,
            "updated_at": now.isoformat(),
            "execution_allowed": execution_payload["execution_allowed"],
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
            "strategy_message": "Prime Stocks runtime is initialized and awaiting the first closed 1H bar.",
            "state": state_payload,
            "signal": signal_payload,
            "execution": execution_payload,
        },
        "signals/latest": {
            "run_id": BOOTSTRAP_RUN_ID,
            "symbol": default_runtime_config.symbol,
            "candidate_action": "BOOTSTRAPPED",
            "latest_signal_time": None,
            "signal": signal_payload,
            "trigger_type": "bootstrap",
            "trigger_source": "firestore_seed",
            "updated_at": now.isoformat(),
            "execution_allowed": execution_payload["execution_allowed"],
            "test_mode": False,
            "test_trigger": None,
            "symbol_override": None,
            "validation_only": False,
            "retry_count": execution_payload["retry_count"],
            "broker_error_code": execution_payload["broker_error_code"],
            "broker_error_message": execution_payload["broker_error_message"],
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
        },
        "execution/current": {
            "run_id": BOOTSTRAP_RUN_ID,
            "candidate_action": "BOOTSTRAPPED",
            "latest_signal_time": None,
            "execution_key": execution_payload["execution_key"],
            "execution_mode": execution_payload["execution_mode"],
            "execution_decision": execution_payload["execution_decision"],
            "order_status": execution_payload["order_status"],
            "order_id": execution_payload["order_id"],
            "client_order_id": execution_payload["client_order_id"],
            "submitted": execution_payload["submitted"],
            "skipped_reason": execution_payload["skipped_reason"],
            "blocked_reason": execution_payload["blocked_reason"],
            "add_tier": execution_payload["add_tier"],
            "execution_allowed": execution_payload["execution_allowed"],
            "retry_count": execution_payload["retry_count"],
            "broker_error_code": execution_payload["broker_error_code"],
            "broker_error_message": execution_payload["broker_error_message"],
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
            "test_mode": False,
            "test_trigger": None,
            "symbol_override": None,
            "validation_only": False,
            "trigger_type": "bootstrap",
            "trigger_source": "firestore_seed",
            "updated_at": now.isoformat(),
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
        },
        "actions/latest": {
            "run_id": BOOTSTRAP_RUN_ID,
            "candidate_action": "BOOTSTRAPPED",
            "execution_decision": execution_payload["execution_decision"],
            "execution_mode": execution_payload["execution_mode"],
            "execution": execution_payload,
            "blocked_reason": execution_payload["blocked_reason"],
            "execution_allowed": execution_payload["execution_allowed"],
            "test_mode": False,
            "test_trigger": None,
            "symbol_override": None,
            "validation_only": False,
            "trigger_type": "bootstrap",
            "trigger_source": "firestore_seed",
            "updated_at": now.isoformat(),
            "last_error_code": None,
            "last_error_message": None,
            "recovery_action": None,
            "failed_at": None,
            "recovered_at": None,
        },
    }


def _serialize_state(state: Any) -> dict[str, Any]:
    return {
        "add_count": state.add_count,
        "last_add_price": state.last_add_price,
        "dollars_used": state.dollars_used,
        "pos_high": state.pos_high,
        "trail_stop": state.trail_stop,
        "position_avg_price": state.position_avg_price,
        "position_size": state.position_size,
    }


def _serialize_runtime_state(state_record: PrimeStocksRuntimeStateRecord) -> dict[str, Any]:
    return {
        "position_open": state_record.position_open,
        "position_size": state_record.position_size,
        "position_avg_price": state_record.position_avg_price,
        "dollars_used": state_record.dollars_used,
        "add_count": state_record.add_count,
        "add_tiers_filled": state_record.add_tiers_filled,
        "last_add_price": state_record.last_add_price,
        "pos_high": state_record.pos_high,
        "trail_stop": state_record.trail_stop,
        "last_entry_time": state_record.last_entry_time,
        "last_exit_time": state_record.last_exit_time,
        "last_action": state_record.last_action,
        "candidate_action": state_record.candidate_action,
        "execution_key": state_record.execution_key,
        "last_processed_bar_time": state_record.last_processed_bar_time,
        "latest_signal_time": state_record.latest_signal_time,
        "current_total_exposure_pct": state_record.current_total_exposure_pct,
    }


def _serialize_signal(strategy_result: PrimeStocksStrategyResult) -> dict[str, Any]:
    latest_signal = strategy_result.latest_signal
    latest_bar = strategy_result.latest_bar
    return {
        "base_entry_signal": latest_signal.base_entry_signal,
        "base_entry_trigger": latest_signal.base_entry_trigger,
        "add_signal_raw": latest_signal.add_signal_raw,
        "add_trigger": latest_signal.add_trigger,
        "hit_atr_trail": latest_signal.hit_atr_trail,
        "hit_regime": latest_signal.hit_regime,
        "pause_new_basket": latest_bar.pause_new_basket if latest_bar is not None else False,
        "pause_adds": latest_bar.pause_adds if latest_bar is not None else False,
        "regime_fail": latest_bar.regime_fail if latest_bar is not None else False,
        "auto_paused": latest_bar.auto_paused if latest_bar is not None else False,
    }


def _isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _serialize_execution(
    *,
    candidate_action: str,
    latest_signal_time: datetime | None,
    execution_mode: str,
    execution_decision: str,
    execution_result: AlpacaPaperExecutionResult | None,
    skipped_reason: str | None,
    ai_decision: PrimeStocksAiDecision | None = None,
    retry_count: int = 0,
    broker_error_code: str | None = None,
    broker_error_message: str | None = None,
) -> dict[str, Any]:
    execution_key = f"{candidate_action}:{_isoformat_or_none(latest_signal_time) or 'none'}"
    blocked_reason = (
        skipped_reason
        if execution_decision not in {
            "submitted_buy",
            "submitted_exit",
            "no_op",
            "dry_run_only",
            "skipped_no_new_bar",
            "skipped_duplicate",
            "validation_ping_ok",
            "validation_ping_duplicate",
        }
        else None
    )
    if ai_decision is not None and ai_decision.Ai_blocked_reason is not None and blocked_reason is None:
        blocked_reason = ai_decision.Ai_blocked_reason
    return {
        "execution_key": execution_key,
        "execution_mode": execution_mode,
        "execution_decision": execution_decision,
        "submitted": execution_result.submitted if execution_result is not None else False,
        "order_status": execution_result.order_status if execution_result is not None else "not_submitted",
        "order_id": execution_result.order_id if execution_result is not None else None,
        "client_order_id": execution_result.client_order_id if execution_result is not None else None,
        "side": execution_result.side if execution_result is not None else None,
        "notional": execution_result.notional if execution_result is not None else None,
        "qty": None,
        "add_tier": execution_result.add_tier if execution_result is not None else _parse_add_tier(candidate_action),
        "execution_allowed": execution_result.submitted if execution_result is not None else execution_decision in {"submitted_buy", "submitted_exit"},
        "skipped_reason": skipped_reason if execution_result is None else execution_result.skipped_reason,
        "blocked_reason": blocked_reason,
        "retry_count": execution_result.retry_count if execution_result is not None else retry_count,
        "broker_error_code": execution_result.broker_error_code if execution_result is not None else broker_error_code,
        "broker_error_message": execution_result.broker_error_message if execution_result is not None else broker_error_message,
        "exit_reason": _resolve_exit_reason(candidate_action=candidate_action, execution_decision=execution_decision),
    }


def _resolve_failure_markers(
    *,
    execution_decision: str,
    blocked_reason: str | None,
    broker_error_code: str | None,
    broker_error_message: str | None,
    runtime_message: str,
    now: datetime,
) -> dict[str, Any]:
    last_error_code = broker_error_code or blocked_reason
    last_error_message = broker_error_message
    if last_error_code is not None and last_error_message is None:
        last_error_message = runtime_message

    recovery_action = None
    if execution_decision in {"runtime_store_unavailable", "market_data_unavailable"}:
        recovery_action = "retry_next_scheduler_run"
    elif execution_decision in {"linked_account_unavailable", "laravel_bridge_unavailable"}:
        recovery_action = "refresh_account_context"
    elif broker_error_code is not None:
        recovery_action = "review_broker_state"
    elif blocked_reason is not None:
        recovery_action = "review_runtime_guard"

    recovered_at = None
    failed_at = None
    if last_error_code is not None:
        failed_at = now.isoformat()
    elif execution_decision in {"submitted_buy", "submitted_exit", "no_op", "skipped_no_new_bar", "validation_ping_ok", "validation_ping_duplicate"}:
        recovered_at = now.isoformat()

    return {
        "last_error_code": last_error_code,
        "last_error_message": last_error_message,
        "recovery_action": recovery_action,
        "failed_at": failed_at,
        "recovered_at": recovered_at,
    }


def _serialize_ai_cache_record(record: AiCacheRecord) -> dict[str, Any]:
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


def _resolve_exit_reason(*, candidate_action: str, execution_decision: str) -> str | None:
    if execution_decision != "submitted_exit":
        return None
    if candidate_action == "EXIT_REGIME":
        return "d1_structure_confirmed"
    if candidate_action == "EXIT_ATR":
        return "atr_trail_hit"
    return "manual_exit"


def _build_notification_payload(
    *,
    enabled: bool,
    run_id: str,
    runtime_config: PrimeStocksRuntimeConfigRecord,
    account_context: ResolvedAlpacaAccountContext,
    candidate_action: str,
    execution_decision: str,
    latest_signal_time: datetime | None,
    serialized_execution: dict[str, Any],
    serialized_ai: dict[str, Any] | None,
    trigger_type: str,
    trigger_source: str,
    now: datetime,
    status: str,
    runtime_message: str,
    service_name: str | None,
    service_revision: str | None,
) -> dict[str, Any] | None:
    if not enabled:
        return None
    event_type = _resolve_notification_event_type(
        candidate_action=candidate_action,
        execution_decision=execution_decision,
        blocked_reason=_maybe_string(serialized_execution.get("blocked_reason")),
        broker_error_code=_maybe_string(serialized_execution.get("broker_error_code")),
    )
    if event_type is None:
        return None
    latest_signal_time_iso = _isoformat_or_none(latest_signal_time)
    order_marker = _maybe_string(serialized_execution.get("order_id")) or _maybe_string(serialized_execution.get("client_order_id"))
    notification_key = ":".join(
        [
            event_type,
            runtime_config.symbol,
            execution_decision,
            _maybe_string(serialized_execution.get("blocked_reason")) or "none",
            latest_signal_time_iso or "none",
            order_marker or "none",
        ]
    )
    return {
        "run_id": run_id,
        "uid": account_context.uid,
        "account_id": runtime_config.account_id,
        "alpaca_account_id": runtime_config.alpaca_account_id,
        "symbol": runtime_config.symbol,
        "candidate_action": candidate_action,
        "execution_decision": execution_decision,
        "blocked_reason": serialized_execution.get("blocked_reason"),
        "skipped_reason": serialized_execution.get("skipped_reason"),
        "order_id": serialized_execution.get("order_id"),
        "client_order_id": serialized_execution.get("client_order_id"),
        "order_status": serialized_execution.get("order_status"),
        "broker_environment": account_context.environment,
        "trigger_type": trigger_type,
        "trigger_source": trigger_source,
        "event_type": event_type,
        "notification_key": notification_key,
        "status": status,
        "message": runtime_message,
        "Ai_regime_label": None if serialized_ai is None else serialized_ai.get("Ai_regime_label"),
        "Ai_sentiment_label": None if serialized_ai is None else serialized_ai.get("Ai_sentiment_label"),
        "Ai_safety_label": None if serialized_ai is None else serialized_ai.get("Ai_safety_label"),
        "Ai_execution_allowed": None if serialized_ai is None else serialized_ai.get("Ai_execution_allowed"),
        "Ai_blocked_reason": None if serialized_ai is None else serialized_ai.get("Ai_blocked_reason"),
        "service_name": service_name,
        "service_revision": service_revision,
        "webhook_ready": {
            "event_type": event_type,
            "run_id": run_id,
            "symbol": runtime_config.symbol,
            "execution_decision": execution_decision,
            "order_id": serialized_execution.get("order_id"),
        },
        "created_at": now.isoformat(),
    }


def _resolve_notification_event_type(
    *,
    candidate_action: str,
    execution_decision: str,
    blocked_reason: str | None,
    broker_error_code: str | None,
) -> str | None:
    if execution_decision == "submitted_buy":
        return "add_submitted" if candidate_action.startswith("MULTI-") else "submitted_buy"
    if execution_decision == "submitted_exit":
        return "submitted_exit"
    if execution_decision in {"global_kill_switch_enabled", "account_kill_switch_enabled"}:
        return "kill_switch_active"
    if execution_decision.startswith("entitlement_"):
        return "entitlement_blocked"
    if broker_error_code is not None:
        return "broker_rejection"
    if execution_decision in {
        "market_data_unavailable",
        "runtime_store_unavailable",
        "linked_account_unavailable",
        "laravel_bridge_unavailable",
        "unexpected_exception",
        "broker_retry_exhausted",
    }:
        return "runtime_failure"
    if blocked_reason in {
        "ai_cache_unavailable",
        "ai_cache_stale",
        "stale_data",
        "live_cap_pct_exceeded",
        "max_total_exposure_pct_exceeded",
        "per_symbol_entry_cap_exceeded",
        "total_entry_exposure_cap_exceeded",
        "total_add_exposure_cap_exceeded",
        "max_notional_per_order_exceeded",
    }:
        return "critical_blocked"
    return None


def _deserialize_ai_cache_record(payload: dict[str, Any]) -> AiCacheRecord:
    return AiCacheRecord(
        scope=str(payload.get("scope", "symbol")),
        symbol=_maybe_string(payload.get("symbol")),
        Ai_regime_label=str(payload.get("Ai_regime_label", "neutral")).strip().lower(),
        Ai_sentiment_label=str(payload.get("Ai_sentiment_label", "neutral")).strip().lower(),
        Ai_safety_label=str(payload.get("Ai_safety_label", "caution")).strip().lower(),
        Ai_confidence=float(payload.get("Ai_confidence", 0.0)),
        Ai_reason=str(payload.get("Ai_reason", "")).strip(),
        Ai_updated_at=_maybe_string(payload.get("Ai_updated_at")),
        Ai_source=str(payload.get("Ai_source", "cached_gemini")).strip(),
        Ai_execution_allowed=bool(payload.get("Ai_execution_allowed", False)),
        Ai_block_new_entries=bool(payload.get("Ai_block_new_entries", False)),
        Ai_block_adds=bool(payload.get("Ai_block_adds", False)),
        Ai_blocked_reason=_maybe_string(payload.get("Ai_blocked_reason")),
    )


def _normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip().lower()
    return "stock" if normalized in SUPPORTED_STOCK_ASSET_TYPES else normalized


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _maybe_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _maybe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _clamp_pct(value: object, *, default: float) -> float:
    resolved = _maybe_float(value)
    if resolved is None:
        return default
    return max(0.0, min(100.0, resolved))


def _maybe_int_list(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    resolved: list[int] = []
    for item in value:
        try:
            resolved.append(int(item))
        except (TypeError, ValueError):
            continue
    return resolved


def _normalize_runtime_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().upper()
    aliases = {
        "1H": "1H",
        "1HOUR": "1H",
        "4H": "4H",
        "4HOUR": "4H",
        "1D": "1D",
        "1DAY": "1D",
        "DAY": "1D",
        "D": "1D",
    }
    return aliases.get(normalized, normalized)


def _normalize_ping_mode(value: object) -> str:
    normalized = str(value or "off").strip().lower()
    if normalized in {"on", "gauge"}:
        return normalized
    return "off"


def _normalize_entitlement_payload(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_add_tier(candidate_action: str) -> int | None:
    if not candidate_action.startswith("MULTI-"):
        return None
    try:
        return int(candidate_action.split("-", maxsplit=1)[1])
    except (IndexError, ValueError):
        return None
