from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import logging
from typing import Any, Callable, Protocol, TypeVar
from uuid import uuid4

from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter
from app.brokers.alpaca_paper_trading import (
    AlpacaPaperExecutionResult,
    AlpacaPaperSubmissionState,
    AlpacaPaperTradingAdapter,
    AlpacaPaperTradingError,
)
from app.runtime.execution.strategies import (
    AdxTrendStrategyConfig,
    BollingerReversionStrategyConfig,
    BreakoutStrategyConfig,
    DonchianBreakoutStrategyConfig,
    EmaStrategyConfig,
    MomentumStrategyConfig,
    OpeningRangeBreakoutStrategyConfig,
    PullbackStrategyConfig,
    RelativeStrengthStrategyConfig,
    RsiReversionStrategyConfig,
    VwapStrategyConfig,
    evaluate_adx_trend_strategy,
    evaluate_bollinger_reversion_strategy,
    evaluate_breakout_strategy,
    evaluate_donchian_breakout_strategy,
    evaluate_ema_strategy,
    evaluate_momentum_strategy,
    evaluate_opening_range_breakout_strategy,
    evaluate_pullback_strategy,
    evaluate_relative_strength_strategy,
    evaluate_rsi_reversion_strategy,
    evaluate_vwap_strategy,
)
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
)
from app.shared.config import AppConfig


logger = logging.getLogger(__name__)
T = TypeVar("T")
SUPPORTED_EXECUTION_ACTIONS = frozenset({"buy", "sell", "close", "cancel", "modify"})


class ExecutionRuntimeStoreError(RuntimeError):
    """Raised when Firestore-backed execution runtime reads or writes fail."""


@dataclass(frozen=True)
class ExecutionRuntimeRequest:
    user_id: str
    account_id: int
    slot: int
    symbol: str = ""
    action: str = ""
    qty: float | None = None
    notional: float | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    alpaca_account_id: int | None = None
    broker_environment: str | None = None
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class ExecutionRuntimeConfig:
    product_id: str
    enabled: bool
    execution_mode: str
    uid: str
    account_id: int
    slot_number: int
    symbol: str
    action: str
    qty: float | None = None
    notional: float | None = None
    alpaca_account_id: int | None = None
    strategy_key: str | None = None
    strategy_settings: dict[str, Any] | None = None
    risk_settings: dict[str, Any] | None = None
    selected_symbols: tuple[str, ...] = ()
    symbol_assignments: dict[str, dict[str, Any]] | None = None
    symbol_states: dict[str, Any] | None = None
    automation_enabled: bool = False
    auto_disable_enabled: bool = True
    auto_disable_min_trades: int = 5
    auto_disable_max_drawdown_percent: float = 12.0
    auto_disable_min_win_rate: float = 35.0
    auto_disable_scope: str = "symbol_assignment"
    global_guardrails_enabled: bool = True
    max_daily_loss_dollars: float | None = None
    max_daily_loss_percent: float | None = 5.0
    max_daily_trades: int | None = 10
    max_open_positions_total: int | None = 5
    max_new_entries_per_run: int | None = 2
    emergency_kill_switch: bool = False


@dataclass(frozen=True)
class ExecutionRuntimePaths:
    root: str
    config_document: str
    state_document: str
    execution_document: str
    signal_document: str
    action_document: str
    heartbeat_document: str
    logs_collection: str
    symbol_state_document: str


@dataclass(frozen=True)
class ExecutionRuntimeResult:
    ok: bool
    run_id: str
    product_id: str
    uid: str
    account_id: int
    slot: int
    symbol: str
    action: str
    execution_status: str
    message: str
    firestore_paths: dict[str, str]
    broker_environment: str | None = None
    alpaca_account_id: int | None = None
    order_id: str | None = None
    replacement_order_id: str | None = None
    client_order_id: str | None = None
    side: str | None = None
    qty: float | None = None
    notional: float | None = None
    broker_error_code: str | None = None
    broker_error_message: str | None = None
    enforcement_reason: str | None = None
    projected_exposure_percent: float | None = None
    open_positions_count: int | None = None
    cancel_status: str | None = None
    modify_status: str | None = None
    manually_disabled: bool | None = None
    auto_disabled: bool | None = None
    disabled_source: str | None = None
    disabled_reason: str | None = None
    auto_disabled_at: str | None = None
    re_enabled_at: str | None = None
    re_enabled_by: str | None = None
    last_runtime_decision_at: str | None = None
    enforcement_metric_snapshot: dict[str, Any] | None = None
    guardrail_status: str | None = None
    guardrail_reason: str | None = None
    guardrail_metric_snapshot: dict[str, Any] | None = None
    last_guardrail_check_at: str | None = None
    last_processed_bar_at: str | None = None
    raw_response: dict[str, Any] | None = None


@dataclass(frozen=True)
class ExecutionSchedulerTarget:
    uid: str
    account_id: int
    slot_number: int
    product_id: str
    runtime_path: str


@dataclass(frozen=True)
class ExecutionSchedulerDiscovery:
    total_slots_seen: int
    runnable_slots: int
    skipped_disabled: int
    skipped_no_symbols: int
    skipped_invalid_config: int
    targets: tuple[ExecutionSchedulerTarget, ...]


class FirestoreClientProtocol(Protocol):
    def collection(self, name: str) -> Any:
        raise NotImplementedError


class ExecutionRuntimeStore:
    PRODUCT_ID = "execution"

    def __init__(
        self,
        settings: AppConfig,
        client: FirestoreClientProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._client = client

    def get_paths(
        self,
        *,
        uid: str,
        account_id: int,
        slot_number: int,
        symbol: str,
    ) -> ExecutionRuntimePaths:
        resolved_uid = uid.strip() or "unknown"
        resolved_slot = max(1, int(slot_number))
        resolved_symbol = symbol.strip().upper() or "UNKNOWN"
        root = (
            f"users/{resolved_uid}/accounts/{int(account_id)}/{self.PRODUCT_ID}/current"
            f"/slots/slot_{resolved_slot}"
        )
        return ExecutionRuntimePaths(
            root=root,
            config_document=f"{root}/config/current",
            state_document=f"{root}/state/current",
            execution_document=f"{root}/execution/current",
            signal_document=f"{root}/signals/latest",
            action_document=f"{root}/actions/latest",
            heartbeat_document=f"{root}/heartbeat/current",
            logs_collection=f"{root}/logs",
            symbol_state_document=f"{root}/symbols/{resolved_symbol}/state/current",
        )

    def load_runtime_config(self, runtime_request: ExecutionRuntimeRequest) -> ExecutionRuntimeConfig:
        default_config = ExecutionRuntimeConfig(
            product_id=self.PRODUCT_ID,
            enabled=True,
            execution_mode="manual_signal" if runtime_request.symbol and runtime_request.action else "strategy_cycle",
            uid=runtime_request.user_id.strip() or "unknown",
            account_id=int(runtime_request.account_id),
            slot_number=max(1, int(runtime_request.slot)),
            symbol=runtime_request.symbol.strip().upper() or "SLOT",
            action=runtime_request.action.strip().lower() or "evaluate",
            qty=runtime_request.qty,
            notional=runtime_request.notional,
            alpaca_account_id=runtime_request.alpaca_account_id,
            strategy_key=None,
            strategy_settings=None,
            risk_settings=None,
            selected_symbols=(),
            symbol_assignments=None,
            symbol_states=None,
            automation_enabled=False,
            auto_disable_enabled=True,
            auto_disable_min_trades=5,
            auto_disable_max_drawdown_percent=12.0,
            auto_disable_min_win_rate=35.0,
            auto_disable_scope="symbol_assignment",
            global_guardrails_enabled=True,
            max_daily_loss_dollars=None,
            max_daily_loss_percent=5.0,
            max_daily_trades=10,
            max_open_positions_total=5,
            max_new_entries_per_run=2,
            emergency_kill_switch=False,
        )
        paths = self.get_paths(
            uid=default_config.uid,
            account_id=default_config.account_id,
            slot_number=default_config.slot_number,
            symbol=default_config.symbol,
        )
        snapshot = self._firestore_call(
            action="load execution runtime config",
            path=paths.config_document,
            fn=lambda: self._document_ref(paths.config_document).get(),
        )
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        if not isinstance(payload, dict):
            return default_config
        matches_linked_account = _payload_matches_expected_linked_account(
            payload,
            expected_alpaca_account_id=runtime_request.alpaca_account_id,
        )
        strategy_settings = payload.get("strategy_settings")
        risk_settings = payload.get("risk_settings")
        symbol_states = payload.get("symbol_states") if matches_linked_account else None
        selected_symbols = _normalize_symbol_list(payload.get("selected_symbols")) if matches_linked_account else ()
        symbol_assignments = _normalize_symbol_assignments(
            payload.get("symbol_assignments") if matches_linked_account else None,
            selected_symbols=selected_symbols,
            fallback_strategy_key=_maybe_string(payload.get("strategy_key")) or "ema",
            fallback_strategy_settings=strategy_settings if isinstance(strategy_settings, dict) else {},
            fallback_risk_settings=risk_settings if isinstance(risk_settings, dict) else {},
            fallback_auto_disable_settings={
                "auto_disable_enabled": payload.get("auto_disable_enabled", True),
                "auto_disable_min_trades": payload.get("auto_disable_min_trades", 5),
                "auto_disable_max_drawdown_percent": payload.get("auto_disable_max_drawdown_percent", 12.0),
                "auto_disable_min_win_rate": payload.get("auto_disable_min_win_rate", 35.0),
            },
        )
        automation_enabled = bool(payload.get("automation_enabled", False))
        enabled = bool(payload.get("enabled", default_config.enabled))
        return ExecutionRuntimeConfig(
            product_id=self.PRODUCT_ID,
            enabled=bool(enabled and (automation_enabled or payload.get("strategy_key") is None or runtime_request.symbol)),
            execution_mode=str(payload.get("execution_mode", default_config.execution_mode)).strip() or default_config.execution_mode,
            uid=str(payload.get("uid", default_config.uid)).strip() or default_config.uid,
            account_id=int(payload.get("account_id", default_config.account_id)),
            slot_number=max(1, int(payload.get("slot_number", default_config.slot_number))),
            symbol=str(payload.get("symbol", default_config.symbol)).strip().upper() or default_config.symbol,
            action=str(payload.get("action", default_config.action)).strip().lower() or default_config.action,
            qty=_maybe_float(payload.get("qty", default_config.qty)),
            notional=_maybe_float(payload.get("notional", default_config.notional)),
            alpaca_account_id=_maybe_int(payload.get("alpaca_account_id", default_config.alpaca_account_id)),
            strategy_key=_maybe_string(payload.get("strategy_key")),
            strategy_settings=strategy_settings if isinstance(strategy_settings, dict) else None,
            risk_settings=risk_settings if isinstance(risk_settings, dict) else None,
            selected_symbols=tuple(symbol_assignments.keys()) if symbol_assignments else selected_symbols,
            symbol_assignments=symbol_assignments,
            symbol_states=symbol_states if isinstance(symbol_states, dict) else None,
            automation_enabled=automation_enabled,
            auto_disable_enabled=bool(payload.get("auto_disable_enabled", True)),
            auto_disable_min_trades=max(1, _maybe_int(payload.get("auto_disable_min_trades")) or 5),
            auto_disable_max_drawdown_percent=max(0.0, _maybe_float(payload.get("auto_disable_max_drawdown_percent")) or 12.0),
            auto_disable_min_win_rate=max(0.0, min(100.0, _maybe_float(payload.get("auto_disable_min_win_rate")) or 35.0)),
            auto_disable_scope=_normalize_auto_disable_scope(payload.get("auto_disable_scope")),
            global_guardrails_enabled=bool(payload.get("global_guardrails_enabled", True)),
            max_daily_loss_dollars=_normalize_guardrail_limit(payload.get("max_daily_loss_dollars")),
            max_daily_loss_percent=_normalize_guardrail_limit(payload.get("max_daily_loss_percent"), default=5.0),
            max_daily_trades=_normalize_guardrail_int(payload.get("max_daily_trades"), default=10),
            max_open_positions_total=_normalize_guardrail_int(payload.get("max_open_positions_total"), default=5),
            max_new_entries_per_run=_normalize_guardrail_int(payload.get("max_new_entries_per_run"), default=2),
            emergency_kill_switch=bool(payload.get("emergency_kill_switch", False)),
        )

    def discover_scheduler_targets(self) -> ExecutionSchedulerDiscovery:
        client = self._client_or_default()
        total_slots_seen = 0
        skipped_disabled = 0
        skipped_no_symbols = 0
        skipped_invalid_config = 0
        targets: list[ExecutionSchedulerTarget] = []

        users_collection = self._firestore_call(
            action="list execution runtime users collection",
            path="users",
            fn=lambda: client.collection("users"),
        )

        for user_snapshot in self._firestore_call(
            action="stream execution runtime users",
            path="users",
            fn=lambda: users_collection.stream(),
        ):
            uid = _snapshot_id(user_snapshot)
            user_reference = _snapshot_reference(user_snapshot)
            if uid is None or user_reference is None:
                continue

            accounts_collection = self._firestore_call(
                action="list execution runtime accounts collection",
                path=f"users/{uid}/accounts",
                fn=lambda user_reference=user_reference: user_reference.collection("accounts"),
            )

            for account_snapshot in self._firestore_call(
                action="stream execution runtime accounts",
                path=f"users/{uid}/accounts",
                fn=lambda accounts_collection=accounts_collection: accounts_collection.stream(),
            ):
                account_reference = _snapshot_reference(account_snapshot)
                account_id_text = _snapshot_id(account_snapshot)
                if account_reference is None or account_id_text is None:
                    continue
                try:
                    account_id = int(account_id_text)
                except (TypeError, ValueError):
                    continue

                current_document = self._firestore_call(
                    action="resolve execution runtime current document",
                    path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current",
                    fn=lambda account_reference=account_reference: account_reference.collection(self.PRODUCT_ID).document("current"),
                )
                slots_collection = self._firestore_call(
                    action="list execution runtime slots collection",
                    path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current/slots",
                    fn=lambda current_document=current_document: current_document.collection("slots"),
                )

                for slot_snapshot in self._firestore_call(
                    action="stream execution runtime slots",
                    path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current/slots",
                    fn=lambda slots_collection=slots_collection: slots_collection.stream(),
                ):
                    total_slots_seen += 1
                    slot_reference = _snapshot_reference(slot_snapshot)
                    slot_id = _snapshot_id(slot_snapshot)
                    slot_number = _parse_slot_number(slot_id)
                    if slot_reference is None or slot_number is None:
                        skipped_invalid_config += 1
                        continue

                    config_document = self._firestore_call(
                        action="resolve execution runtime slot config document",
                        path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current/slots/slot_{slot_number}/config/current",
                        fn=lambda slot_reference=slot_reference: slot_reference.collection("config").document("current"),
                    )
                    config_snapshot = self._firestore_call(
                        action="load execution runtime slot config",
                        path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current/slots/slot_{slot_number}/config/current",
                        fn=lambda config_document=config_document: config_document.get(),
                    )
                    if not getattr(config_snapshot, "exists", False):
                        skipped_invalid_config += 1
                        continue

                    payload = config_snapshot.to_dict()
                    if not isinstance(payload, dict):
                        skipped_invalid_config += 1
                        continue

                    if not bool(payload.get("automation_enabled", False)):
                        skipped_disabled += 1
                        continue

                    selected_symbols = _normalize_symbol_list(payload.get("selected_symbols"))
                    symbol_assignments = _normalize_symbol_assignments(
                        payload.get("symbol_assignments"),
                        selected_symbols=selected_symbols,
                        fallback_strategy_key=_maybe_string(payload.get("strategy_key")) or "ema",
                        fallback_strategy_settings=payload.get("strategy_settings") if isinstance(payload.get("strategy_settings"), dict) else {},
                        fallback_risk_settings=payload.get("risk_settings") if isinstance(payload.get("risk_settings"), dict) else {},
                        fallback_auto_disable_settings={
                            "auto_disable_enabled": payload.get("auto_disable_enabled", True),
                            "auto_disable_min_trades": payload.get("auto_disable_min_trades", 5),
                            "auto_disable_max_drawdown_percent": payload.get("auto_disable_max_drawdown_percent", 12.0),
                            "auto_disable_min_win_rate": payload.get("auto_disable_min_win_rate", 35.0),
                        },
                    )
                    enabled_assignments = [
                        symbol
                        for symbol, assignment in symbol_assignments.items()
                        if isinstance(assignment, dict) and _assignment_is_runnable(assignment)
                    ]
                    if not enabled_assignments:
                        skipped_no_symbols += 1
                        continue

                    targets.append(
                        ExecutionSchedulerTarget(
                            uid=uid,
                            account_id=account_id,
                            slot_number=slot_number,
                            product_id=self.PRODUCT_ID,
                            runtime_path=f"users/{uid}/accounts/{account_id}/{self.PRODUCT_ID}/current/slots/slot_{slot_number}",
                        )
                    )

        return ExecutionSchedulerDiscovery(
            total_slots_seen=total_slots_seen,
            runnable_slots=len(targets),
            skipped_disabled=skipped_disabled,
            skipped_no_symbols=skipped_no_symbols,
            skipped_invalid_config=skipped_invalid_config,
            targets=tuple(targets),
        )

    def execution_performance_base_path(self, *, uid: str, account_id: int) -> str:
        resolved_uid = uid.strip()
        if resolved_uid == "":
            raise ExecutionRuntimeStoreError("Execution performance tracking requires a non-empty uid.")
        return f"users/{resolved_uid}/accounts/{account_id}/performance/{self.PRODUCT_ID}"

    def execution_performance_trade_path(self, *, uid: str, account_id: int, trade_id: str) -> str:
        resolved_trade_id = trade_id.strip()
        if resolved_trade_id == "":
            raise ExecutionRuntimeStoreError("Execution performance tracking requires a non-empty trade_id.")
        return f"{self.execution_performance_base_path(uid=uid, account_id=account_id)}/trades/{resolved_trade_id}"

    def execution_performance_summary_path(self, *, uid: str, account_id: int) -> str:
        return f"{self.execution_performance_base_path(uid=uid, account_id=account_id)}/summary/current"

    def load_execution_trade_performance_documents(self, *, uid: str, account_id: int) -> list[dict[str, Any]]:
        trade_collection_path = f"{self.execution_performance_base_path(uid=uid, account_id=account_id)}/trades"
        client = self._client_or_default()

        if hasattr(client, "storage"):
            storage = getattr(client, "storage", {})
            cursor = storage
            for part in trade_collection_path.split("/"):
                if part not in cursor:
                    return []
                cursor = cursor[part]
            if not isinstance(cursor, dict):
                return []
            return [payload for payload in cursor.values() if isinstance(payload, dict)]

        snapshots = self._firestore_call(
            action="load execution trade performance documents",
            path=trade_collection_path,
            fn=lambda: list(self._collection_ref(trade_collection_path).stream()),
        )
        documents: list[dict[str, Any]] = []
        for snapshot in snapshots:
            if getattr(snapshot, "exists", False):
                payload = snapshot.to_dict()
                if isinstance(payload, dict):
                    documents.append(payload)
        return documents

    def load_execution_performance_summary(self, *, uid: str, account_id: int) -> dict[str, Any]:
        summary_path = self.execution_performance_summary_path(uid=uid, account_id=account_id)
        snapshot = self._firestore_call(
            action="load execution performance summary",
            path=summary_path,
            fn=lambda: self._document_ref(summary_path).get(),
        )
        if getattr(snapshot, "exists", False):
            payload = snapshot.to_dict()
            if isinstance(payload, dict):
                return payload
        return {}

    def write_execution_trade_performance_batch(
        self,
        *,
        uid: str,
        account_id: int,
        trade_payloads: dict[str, dict[str, Any]],
    ) -> None:
        if not trade_payloads:
            return

        now = datetime.now(tz=UTC).isoformat()
        for trade_id, trade_payload in trade_payloads.items():
            trade_path = self.execution_performance_trade_path(uid=uid, account_id=account_id, trade_id=trade_id)
            payload = dict(trade_payload)
            payload.setdefault("trade_id", trade_id)
            payload.setdefault("updated_at", now)
            self._firestore_call(
                action="write execution trade performance",
                path=trade_path,
                fn=lambda trade_path=trade_path, payload=payload: self._document_ref(trade_path).set(payload, merge=True),
            )

        summary = self._build_execution_trade_performance_summary(uid=uid, account_id=account_id)
        summary_payload = {
            **summary,
            "product_id": self.PRODUCT_ID,
            "updated_at": now,
        }
        summary_path = self.execution_performance_summary_path(uid=uid, account_id=account_id)
        self._firestore_call(
            action="write execution trade performance summary",
            path=summary_path,
            fn=lambda: self._document_ref(summary_path).set(summary_payload, merge=True),
        )

    def write_execution_slot_config(
        self,
        *,
        uid: str,
        account_id: int,
        slot_number: int,
        config_payload: dict[str, Any],
    ) -> None:
        config_path = self.get_paths(uid=uid, account_id=account_id, slot_number=slot_number, symbol="SLOT").config_document
        self._firestore_call(
            action="write execution slot config",
            path=config_path,
            fn=lambda: self._document_ref(config_path).set(config_payload, merge=True),
        )

    def _build_execution_trade_performance_summary(self, *, uid: str, account_id: int) -> dict[str, Any]:
        trades = self.load_execution_trade_performance_documents(uid=uid, account_id=account_id)
        closed_trades = [
            payload
            for payload in trades
            if str(payload.get("trade_state", "")).strip().lower() == "closed"
            and _maybe_float(payload.get("realized_pnl_dollars")) is not None
        ]
        if not closed_trades:
            return {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "realized_pnl": 0.0,
                "realized_pnl_dollars": 0.0,
                "realized_pnl_percent": 0.0,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "net_pnl": 0.0,
                "avg_win": 0.0,
                "avg_win_dollars": 0.0,
                "avg_loss": 0.0,
                "avg_loss_dollars": 0.0,
                "last_trade_at": None,
                "slot_summaries": {},
                "symbol_summaries": [],
                "strategy_summaries": [],
                "recent_trades": [],
            }

        def _update_bucket(bucket: dict[str, Any], payload: dict[str, Any]) -> None:
            realized_pnl = _maybe_float(payload.get("realized_pnl_dollars")) or 0.0
            entry_notional = abs(_maybe_float(payload.get("entry_notional")) or 0.0)
            trade_at = _iso_to_datetime(payload.get("exit_filled_at")) or _iso_to_datetime(payload.get("updated_at"))

            bucket["total_trades"] += 1
            bucket["realized_pnl_dollars"] += realized_pnl
            bucket["entry_notional"] += entry_notional
            if realized_pnl > 0:
                bucket["wins"] += 1
                bucket["gross_profit"] += realized_pnl
                bucket["win_values"].append(realized_pnl)
            elif realized_pnl < 0:
                bucket["losses"] += 1
                bucket["gross_loss"] += abs(realized_pnl)
                bucket["loss_values"].append(realized_pnl)
            if trade_at is not None and (bucket["last_trade_at"] is None or trade_at > bucket["last_trade_at"]):
                bucket["last_trade_at"] = trade_at

        def _finalize_bucket(bucket: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
            total_trades = bucket["total_trades"]
            realized_pnl = round(bucket["realized_pnl_dollars"], 2)
            entry_notional = bucket["entry_notional"]
            wins = bucket["wins"]
            losses = bucket["losses"]
            payload = {
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "win_rate": round((wins / total_trades) * 100.0, 2) if total_trades > 0 else 0.0,
                "realized_pnl": realized_pnl,
                "realized_pnl_dollars": realized_pnl,
                "realized_pnl_percent": round((realized_pnl / entry_notional) * 100.0, 2) if entry_notional > 0 else 0.0,
                "gross_profit": round(bucket["gross_profit"], 2),
                "gross_loss": round(bucket["gross_loss"], 2),
                "net_pnl": realized_pnl,
                "avg_win": round(sum(bucket["win_values"]) / len(bucket["win_values"]), 2) if bucket["win_values"] else 0.0,
                "avg_win_dollars": round(sum(bucket["win_values"]) / len(bucket["win_values"]), 2) if bucket["win_values"] else 0.0,
                "avg_loss": round(sum(bucket["loss_values"]) / len(bucket["loss_values"]), 2) if bucket["loss_values"] else 0.0,
                "avg_loss_dollars": round(sum(bucket["loss_values"]) / len(bucket["loss_values"]), 2) if bucket["loss_values"] else 0.0,
                "last_trade_at": bucket["last_trade_at"].isoformat() if isinstance(bucket["last_trade_at"], datetime) else None,
                "unrealized_pnl": None,
            }
            if extra:
                payload.update(extra)
            return payload

        overall = {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "realized_pnl_dollars": 0.0,
            "entry_notional": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "win_values": [],
            "loss_values": [],
            "last_trade_at": None,
        }
        slot_buckets: dict[int, dict[str, Any]] = defaultdict(lambda: {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "realized_pnl_dollars": 0.0,
            "entry_notional": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "win_values": [],
            "loss_values": [],
            "last_trade_at": None,
        })
        symbol_buckets: dict[tuple[int, str], dict[str, Any]] = defaultdict(lambda: {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "realized_pnl_dollars": 0.0,
            "entry_notional": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "win_values": [],
            "loss_values": [],
            "last_trade_at": None,
        })
        strategy_buckets: dict[tuple[int, str], dict[str, Any]] = defaultdict(lambda: {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "realized_pnl_dollars": 0.0,
            "entry_notional": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "win_values": [],
            "loss_values": [],
            "last_trade_at": None,
        })

        normalized_recent_trades: list[dict[str, Any]] = []
        for payload in closed_trades:
            slot_number = max(1, _maybe_int(payload.get("slot_number")) or 1)
            symbol = (_maybe_string(payload.get("symbol")) or "UNKNOWN").upper()
            strategy_key = (_maybe_string(payload.get("strategy_key")) or "unknown").lower()
            _update_bucket(overall, payload)
            _update_bucket(slot_buckets[slot_number], payload)
            _update_bucket(symbol_buckets[(slot_number, symbol)], payload)
            _update_bucket(strategy_buckets[(slot_number, strategy_key)], payload)
            normalized_recent_trades.append({
                "trade_id": _maybe_string(payload.get("trade_id")),
                "slot_number": slot_number,
                "symbol": symbol,
                "strategy_key": strategy_key,
                "trade_outcome": _maybe_string(payload.get("trade_outcome")) or "breakeven",
                "realized_pnl_dollars": round(_maybe_float(payload.get("realized_pnl_dollars")) or 0.0, 2),
                "realized_pnl_percent": round(_maybe_float(payload.get("realized_pnl_percent")) or 0.0, 2),
                "entry_price": _maybe_float(payload.get("entry_price")),
                "exit_price": _maybe_float(payload.get("exit_price")),
                "qty": _maybe_float(payload.get("qty")),
                "entry_filled_at": _maybe_string(payload.get("entry_filled_at")),
                "exit_filled_at": _maybe_string(payload.get("exit_filled_at")),
                "updated_at": _maybe_string(payload.get("updated_at")),
            })

        slot_summaries = {
            str(slot_number): _finalize_bucket(bucket, {"slot_number": slot_number})
            for slot_number, bucket in sorted(slot_buckets.items(), key=lambda item: item[0])
        }
        symbol_summaries = [
            _finalize_bucket(bucket, {"slot_number": slot_number, "symbol": symbol})
            for (slot_number, symbol), bucket in sorted(symbol_buckets.items(), key=lambda item: (item[0][0], item[0][1]))
        ]
        strategy_summaries = [
            _finalize_bucket(bucket, {"slot_number": slot_number, "strategy_key": strategy_key})
            for (slot_number, strategy_key), bucket in sorted(strategy_buckets.items(), key=lambda item: (item[0][0], item[0][1]))
        ]

        overall_summary = _finalize_bucket(overall)
        overall_summary["slot_summaries"] = slot_summaries
        overall_summary["symbol_summaries"] = symbol_summaries
        overall_summary["strategy_summaries"] = strategy_summaries
        overall_summary["recent_trades"] = sorted(
            normalized_recent_trades,
            key=lambda item: item.get("exit_filled_at") or item.get("updated_at") or "",
            reverse=True,
        )[:20]
        return overall_summary

    def write_runtime_result(
        self,
        *,
        runtime_request: ExecutionRuntimeRequest,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext | None,
        result: ExecutionRuntimeResult,
        write_symbol_state: bool = True,
    ) -> None:
        now = datetime.now(tz=UTC).isoformat()
        last_processed_bar_at = _maybe_string(result.last_processed_bar_at)
        latest_action = runtime_request.action or runtime_config.action or result.action
        latest_strategy_key = _maybe_string(runtime_config.strategy_key)
        paths = self.get_paths(
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
            slot_number=runtime_config.slot_number,
            symbol=runtime_config.symbol,
        )
        state_payload = {
            "run_id": result.run_id,
            "uid": runtime_config.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": result.alpaca_account_id,
            "product_id": self.PRODUCT_ID,
            "slot_number": runtime_config.slot_number,
            "symbol": runtime_config.symbol,
            "action": runtime_config.action,
            "execution_status": result.execution_status,
            "latest_result": result.execution_status,
            "message": result.message,
            "runtime_message": result.message,
            "broker_environment": result.broker_environment,
            "strategy_key": runtime_config.strategy_key,
            "latest_strategy_key": latest_strategy_key,
            "latest_action": latest_action,
            "included_in_latest_cycle": True,
            "included_in_last_cycle": True,
            "last_checked_at": result.last_runtime_decision_at or now,
            "checked_at": result.last_runtime_decision_at or now,
            "last_processed_bar_time": last_processed_bar_at,
            "last_processed_bar_at": last_processed_bar_at,
            "order_id": result.order_id,
            "replacement_order_id": result.replacement_order_id,
            "client_order_id": result.client_order_id,
            "side": result.side,
            "qty": result.qty,
            "notional": result.notional,
            "broker_error_code": result.broker_error_code,
            "broker_error_message": result.broker_error_message,
            "enforcement_reason": result.enforcement_reason,
            "projected_exposure_percent": result.projected_exposure_percent,
            "open_positions_count": result.open_positions_count,
            "cancel_status": result.cancel_status,
            "modify_status": result.modify_status,
            "manually_disabled": result.manually_disabled,
            "auto_disabled": result.auto_disabled,
            "disabled_source": result.disabled_source,
            "disabled_reason": result.disabled_reason,
            "auto_disabled_at": result.auto_disabled_at,
            "re_enabled_at": result.re_enabled_at,
            "re_enabled_by": result.re_enabled_by,
            "last_runtime_decision_at": result.last_runtime_decision_at,
            "enforcement_metric_snapshot": result.enforcement_metric_snapshot,
            "guardrail_status": result.guardrail_status,
            "guardrail_reason": result.guardrail_reason,
            "guardrail_metric_snapshot": result.guardrail_metric_snapshot,
            "last_guardrail_check_at": result.last_guardrail_check_at,
            "updated_at": now,
        }
        execution_payload = {
            "run_id": result.run_id,
            "uid": runtime_config.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": result.alpaca_account_id,
            "slot_number": runtime_config.slot_number,
            "product_id": self.PRODUCT_ID,
            "symbol": runtime_config.symbol,
            "action": runtime_config.action,
            "execution_status": result.execution_status,
            "latest_result": result.execution_status,
            "message": result.message,
            "runtime_message": result.message,
            "broker_environment": result.broker_environment,
            "submitted": result.execution_status in {"submitted", "buy_submitted", "sell_submitted", "close_submitted"},
            "strategy_key": runtime_config.strategy_key,
            "latest_strategy_key": latest_strategy_key,
            "latest_action": latest_action,
            "included_in_latest_cycle": True,
            "included_in_last_cycle": True,
            "last_checked_at": result.last_runtime_decision_at or now,
            "checked_at": result.last_runtime_decision_at or now,
            "last_processed_bar_time": last_processed_bar_at,
            "last_processed_bar_at": last_processed_bar_at,
            "order_id": result.order_id,
            "replacement_order_id": result.replacement_order_id,
            "client_order_id": result.client_order_id,
            "side": result.side,
            "qty": result.qty,
            "notional": result.notional,
            "broker_error_code": result.broker_error_code,
            "broker_error_message": result.broker_error_message,
            "enforcement_reason": result.enforcement_reason,
            "projected_exposure_percent": result.projected_exposure_percent,
            "open_positions_count": result.open_positions_count,
            "cancel_status": result.cancel_status,
            "modify_status": result.modify_status,
            "manually_disabled": result.manually_disabled,
            "auto_disabled": result.auto_disabled,
            "disabled_source": result.disabled_source,
            "disabled_reason": result.disabled_reason,
            "auto_disabled_at": result.auto_disabled_at,
            "re_enabled_at": result.re_enabled_at,
            "re_enabled_by": result.re_enabled_by,
            "last_runtime_decision_at": result.last_runtime_decision_at,
            "enforcement_metric_snapshot": result.enforcement_metric_snapshot,
            "guardrail_status": result.guardrail_status,
            "guardrail_reason": result.guardrail_reason,
            "guardrail_metric_snapshot": result.guardrail_metric_snapshot,
            "last_guardrail_check_at": result.last_guardrail_check_at,
            "updated_at": now,
        }
        signal_payload = {
            "run_id": result.run_id,
            "uid": runtime_config.uid,
            "account_id": runtime_config.account_id,
            "slot_number": runtime_config.slot_number,
            "product_id": self.PRODUCT_ID,
            "symbol": runtime_config.symbol,
            "action": runtime_config.action,
            "received_at": now,
            "payload": runtime_request.payload or {},
            "strategy_key": runtime_config.strategy_key,
        }
        log_payload = {
            "kind": "execution_runtime",
            "created_at": now,
            "run_id": result.run_id,
            "uid": runtime_config.uid,
            "account_id": runtime_config.account_id,
            "alpaca_account_id": result.alpaca_account_id,
            "slot_number": runtime_config.slot_number,
            "product_id": self.PRODUCT_ID,
            "symbol": runtime_config.symbol,
            "action": runtime_config.action,
            "execution_status": result.execution_status,
            "message": result.message,
            "broker_environment": result.broker_environment,
            "strategy_key": runtime_config.strategy_key,
            "order_id": result.order_id,
            "replacement_order_id": result.replacement_order_id,
            "client_order_id": result.client_order_id,
            "side": result.side,
            "qty": result.qty,
            "notional": result.notional,
            "broker_error_code": result.broker_error_code,
            "broker_error_message": result.broker_error_message,
            "enforcement_reason": result.enforcement_reason,
            "projected_exposure_percent": result.projected_exposure_percent,
            "open_positions_count": result.open_positions_count,
            "cancel_status": result.cancel_status,
            "modify_status": result.modify_status,
            "manually_disabled": result.manually_disabled,
            "auto_disabled": result.auto_disabled,
            "disabled_source": result.disabled_source,
            "disabled_reason": result.disabled_reason,
            "auto_disabled_at": result.auto_disabled_at,
            "re_enabled_at": result.re_enabled_at,
            "re_enabled_by": result.re_enabled_by,
            "last_runtime_decision_at": result.last_runtime_decision_at,
            "enforcement_metric_snapshot": result.enforcement_metric_snapshot,
            "guardrail_status": result.guardrail_status,
            "guardrail_reason": result.guardrail_reason,
            "guardrail_metric_snapshot": result.guardrail_metric_snapshot,
            "last_guardrail_check_at": result.last_guardrail_check_at,
            "raw_response": result.raw_response,
            "resolved_account": None if account_context is None else {
                "broker_connection_id": account_context.broker_connection_id,
                "broker_credential_id": account_context.broker_credential_id,
                "trade_enabled": account_context.trade_enabled,
                "environment": account_context.environment,
            },
        }

        self._firestore_call(
            action="write execution runtime state",
            path=paths.state_document,
            fn=lambda: self._document_ref(paths.state_document).set(state_payload, merge=True),
        )
        self._firestore_call(
            action="write execution runtime result",
            path=paths.execution_document,
            fn=lambda: self._document_ref(paths.execution_document).set(execution_payload, merge=True),
        )
        self._firestore_call(
            action="write execution runtime signal",
            path=paths.signal_document,
            fn=lambda: self._document_ref(paths.signal_document).set(signal_payload, merge=True),
        )
        self._firestore_call(
            action="write execution runtime action",
            path=paths.action_document,
            fn=lambda: self._document_ref(paths.action_document).set(execution_payload, merge=True),
        )
        if write_symbol_state:
            symbol_state_payload = dict(state_payload)
            self._firestore_call(
                action="write execution symbol state",
                path=paths.symbol_state_document,
                fn=lambda: self._document_ref(paths.symbol_state_document).set(symbol_state_payload, merge=True),
            )
        self._firestore_call(
            action="write execution heartbeat",
            path=paths.heartbeat_document,
            fn=lambda: self._document_ref(paths.heartbeat_document).set(
                {
                    "product_id": self.PRODUCT_ID,
                    "account_id": runtime_config.account_id,
                    "slot_number": runtime_config.slot_number,
                    "updated_at": now,
                    "last_run_id": result.run_id,
                    "last_execution_status": result.execution_status,
                    "last_checked_at": result.last_runtime_decision_at or now,
                    "last_processed_bar_time": last_processed_bar_at,
                },
                merge=True,
            ),
        )
        self._firestore_call(
            action="write execution runtime log",
            path=f"{paths.logs_collection}/{result.run_id}",
            fn=lambda: self._document_ref(f"{paths.logs_collection}/{result.run_id}").set(log_payload, merge=True),
        )

    def create_run_id(self) -> str:
        return f"run-{uuid4().hex[:15]}"

    def _document_ref(self, path: str) -> Any:
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) % 2 != 0:
            raise ExecutionRuntimeStoreError(f"Invalid Firestore document path '{path}'.")
        ref = self._client_or_default()
        for index, segment in enumerate(segments):
            ref = ref.collection(segment) if index % 2 == 0 else ref.document(segment)
        return ref

    def _collection_ref(self, path: str) -> Any:
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) % 2 == 0:
            raise ExecutionRuntimeStoreError(f"Invalid Firestore collection path '{path}'.")
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
                "google-cloud-firestore is required for execution runtime state persistence."
            ) from exc
        try:
            self._client = firestore.Client(
                project=self._settings.firestore_project_id,
                database=self._settings.firestore_database_id,
            )
        except Exception as exc:
            raise ExecutionRuntimeStoreError(
                "Failed to initialize Firestore client for execution runtime state."
            ) from exc
        return self._client

    def _firestore_call(self, *, action: str, path: str, fn: Callable[[], T]) -> T:
        try:
            return fn()
        except ExecutionRuntimeStoreError:
            raise
        except Exception as exc:
            raise ExecutionRuntimeStoreError(
                f"Failed to {action} at Firestore path '{path}'."
            ) from exc


class ExecutionRuntimeService:
    PRODUCT_ID = "execution"

    def __init__(
        self,
        settings: AppConfig,
        runtime_store: ExecutionRuntimeStore | None = None,
        account_resolver: LaravelAlpacaAccountResolver | None = None,
        paper_trading: AlpacaPaperTradingAdapter | None = None,
        market_data: AlpacaMarketDataAdapter | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._runtime_store = runtime_store or ExecutionRuntimeStore(settings=settings)
        self._account_resolver = account_resolver or LaravelAlpacaAccountResolver(settings=settings)
        self._paper_trading = paper_trading or AlpacaPaperTradingAdapter(settings=settings)
        self._market_data = market_data or AlpacaMarketDataAdapter(settings=settings)
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))
        self._slot_cycle_bar_cache: dict[tuple[Any, ...], list[Any]] = {}
        self._slot_cycle_trade_docs_cache: dict[tuple[str, int], list[dict[str, Any]]] = {}
        self._slot_cycle_entry_count = 0

    def discover_scheduler_targets(self) -> ExecutionSchedulerDiscovery:
        total_slots_seen = 0
        skipped_disabled = 0
        skipped_no_symbols = 0
        skipped_invalid_config = 0
        targets: list[ExecutionSchedulerTarget] = []

        try:
            runtime_targets = self._account_resolver.list_runtime_targets(product_id=self.PRODUCT_ID)
        except Exception:
            return self._runtime_store.discover_scheduler_targets()

        for runtime_target in runtime_targets:
            total_slots_seen += 1
            try:
                runtime_config = self._runtime_store.load_runtime_config(
                    ExecutionRuntimeRequest(
                        user_id=runtime_target.uid,
                        account_id=runtime_target.account_id,
                        slot=runtime_target.slot_number,
                        alpaca_account_id=runtime_target.alpaca_account_id,
                    )
                )
            except Exception:
                skipped_invalid_config += 1
                continue

            if not runtime_config.automation_enabled:
                skipped_disabled += 1
                continue

            symbol_assignments = runtime_config.symbol_assignments or {}
            has_enabled_symbols = any(
                isinstance(assignment, dict) and bool(assignment.get("enabled", True))
                for assignment in symbol_assignments.values()
            )
            if not has_enabled_symbols:
                skipped_no_symbols += 1
                continue

            targets.append(
                ExecutionSchedulerTarget(
                    uid=runtime_target.uid,
                    account_id=runtime_target.account_id,
                    slot_number=runtime_target.slot_number,
                    product_id=self.PRODUCT_ID,
                    runtime_path=runtime_target.runtime_path
                    or f"users/{runtime_target.uid}/accounts/{runtime_target.account_id}/{self.PRODUCT_ID}/current/slots/slot_{runtime_target.slot_number}",
                )
            )

        return ExecutionSchedulerDiscovery(
            total_slots_seen=total_slots_seen,
            runnable_slots=len(targets),
            skipped_disabled=skipped_disabled,
            skipped_no_symbols=skipped_no_symbols,
            skipped_invalid_config=skipped_invalid_config,
            targets=tuple(targets),
        )

    def run_once(self, payload: dict[str, Any]) -> ExecutionRuntimeResult:
        self._reset_slot_cycle_caches()
        runtime_request = self._validate_payload(payload)
        runtime_config = self._runtime_store.load_runtime_config(runtime_request)
        run_id = self._runtime_store.create_run_id()

        try:
            if not runtime_config.enabled:
                result = ExecutionRuntimeResult(
                    ok=False,
                    run_id=run_id,
                    product_id=self.PRODUCT_ID,
                    uid=runtime_config.uid,
                    account_id=runtime_config.account_id,
                    slot=runtime_config.slot_number,
                    symbol=runtime_config.symbol,
                    action=runtime_config.action,
                    execution_status="disabled",
                    message="Execution runtime is disabled for this slot.",
                    firestore_paths=asdict(
                        self._runtime_store.get_paths(
                            uid=runtime_config.uid,
                            account_id=runtime_config.account_id,
                            slot_number=runtime_config.slot_number,
                            symbol=runtime_config.symbol,
                        )
                    ),
                    broker_environment=runtime_request.broker_environment,
                    alpaca_account_id=runtime_config.alpaca_account_id,
                )
                self._runtime_store.write_runtime_result(
                    runtime_request=runtime_request,
                    runtime_config=runtime_config,
                    account_context=None,
                    result=result,
                )
                return result

            account_context: ResolvedAlpacaAccountContext | None = None
            execution_status = "skipped"
            message = "Execution runtime skipped order submission."
            execution_result: AlpacaPaperExecutionResult | None = None

            try:
                account_context = self._account_resolver.resolve_runtime_account_for_slot(
                    account_id=runtime_config.account_id,
                    slot_number=runtime_config.slot_number,
                    product_id=self.PRODUCT_ID,
                )
                if runtime_request.action:
                    execution_result = self._execute_order(
                        runtime_request=runtime_request,
                        runtime_config=runtime_config,
                        account_context=account_context,
                    )
                    execution_status = _manual_execution_status(action=runtime_config.action, execution_result=execution_result)
                    message = _result_message(runtime_config, execution_result)
                else:
                    return self._run_strategy_cycle(
                        runtime_request=runtime_request,
                        runtime_config=runtime_config,
                        account_context=account_context,
                        run_id=run_id,
                    )
            except AlpacaAccountResolutionError as exc:
                execution_status = "skipped"
                message = str(exc)
                logger.warning(
                    "Execution runtime skipped account resolution account_id=%s slot=%s symbol=%s action=%s reason=%s",
                    runtime_config.account_id,
                    runtime_config.slot_number,
                    runtime_config.symbol,
                    runtime_config.action,
                    exc,
                )
            except AlpacaPaperTradingError as exc:
                execution_status = "failed"
                message = _normalize_execution_error_message(exc.message)
                execution_result = AlpacaPaperExecutionResult(
                    action=runtime_config.action,
                    submitted=False,
                    order_status="rejected",
                    order_id=None,
                    client_order_id=None,
                    side=_result_side(runtime_config.action),
                    notional=runtime_config.notional,
                    skipped_reason=None,
                    broker_error_code=exc.code,
                    broker_error_message=_normalize_execution_error_message(exc.message),
                    raw_response=exc.raw_response,
                )
                logger.warning(
                    "Execution runtime order submission failed account_id=%s slot=%s symbol=%s action=%s code=%s message=%s",
                    runtime_config.account_id,
                    runtime_config.slot_number,
                    runtime_config.symbol,
                    runtime_config.action,
                    exc.code,
                    exc.message,
                )
            except ValueError as exc:
                execution_status = "skipped"
                message = str(exc)
                logger.warning(
                    "Execution runtime rejected payload account_id=%s slot=%s symbol=%s action=%s reason=%s",
                    runtime_config.account_id,
                    runtime_config.slot_number,
                    runtime_config.symbol,
                    runtime_config.action,
                    exc,
                )

            result = ExecutionRuntimeResult(
                ok=execution_status in {"submitted", "buy_submitted", "sell_submitted", "close_submitted", "cancel_submitted", "modify_submitted"},
                run_id=run_id,
                product_id=self.PRODUCT_ID,
                uid=runtime_config.uid,
                account_id=runtime_config.account_id,
                slot=runtime_config.slot_number,
                symbol=runtime_config.symbol,
                action=runtime_config.action,
                execution_status=execution_status,
                message=message,
                firestore_paths=asdict(
                    self._runtime_store.get_paths(
                        uid=runtime_config.uid,
                        account_id=runtime_config.account_id,
                        slot_number=runtime_config.slot_number,
                        symbol=runtime_config.symbol,
                    )
                ),
                broker_environment=(
                    account_context.environment if account_context is not None else runtime_request.broker_environment
                ),
                alpaca_account_id=(
                    account_context.alpaca_account_id if account_context is not None else runtime_config.alpaca_account_id
                ),
                order_id=execution_result.order_id if execution_result is not None else None,
                replacement_order_id=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "replacement_order_id",
                ),
                client_order_id=execution_result.client_order_id if execution_result is not None else None,
                side=execution_result.side if execution_result is not None else _result_side(runtime_config.action),
                qty=runtime_config.qty,
                notional=execution_result.notional if execution_result is not None else runtime_config.notional,
                broker_error_code=execution_result.broker_error_code if execution_result is not None else None,
                broker_error_message=execution_result.broker_error_message if execution_result is not None else None,
                enforcement_reason=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "enforcement_reason",
                ),
                projected_exposure_percent=_raw_response_float(
                    execution_result.raw_response if execution_result is not None else None,
                    "projected_exposure_percent",
                ),
                open_positions_count=_raw_response_int(
                    execution_result.raw_response if execution_result is not None else None,
                    "open_positions_count",
                ),
                cancel_status=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "cancel_status",
                ),
                modify_status=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "modify_status",
                ),
                guardrail_status=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "guardrail_status",
                ),
                guardrail_reason=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "guardrail_reason",
                ),
                guardrail_metric_snapshot=_raw_response_dict(
                    execution_result.raw_response if execution_result is not None else None,
                    "guardrail_metric_snapshot",
                ),
                last_guardrail_check_at=_raw_response_string(
                    execution_result.raw_response if execution_result is not None else None,
                    "last_guardrail_check_at",
                ),
                raw_response=execution_result.raw_response if execution_result is not None else None,
            )
            self._sync_execution_performance(
                runtime_config=runtime_config,
                account_context=account_context,
                results=[result],
            )
            self._runtime_store.write_runtime_result(
                runtime_request=runtime_request,
                runtime_config=runtime_config,
                account_context=account_context,
                result=result,
            )
            logger.info(
                "Execution runtime completed product_id=%s account_id=%s slot=%s symbol=%s action=%s execution_status=%s run_id=%s",
                result.product_id,
                result.account_id,
                result.slot,
                result.symbol,
                result.action,
                result.execution_status,
                result.run_id,
            )
            return result
        finally:
            self._reset_slot_cycle_caches()

    def _reset_slot_cycle_caches(self) -> None:
        self._slot_cycle_bar_cache = {}
        self._slot_cycle_trade_docs_cache = {}
        self._slot_cycle_entry_count = 0

    def _fetch_stock_bars_cached(
        self,
        *,
        symbol: str,
        timeframe: str,
        limit: int,
        credential_context: ResolvedAlpacaAccountContext,
    ) -> list[Any]:
        cache_key = (
            credential_context.account_id,
            credential_context.alpaca_account_id,
            symbol.upper(),
            timeframe,
            int(limit),
        )
        if cache_key not in self._slot_cycle_bar_cache:
            self._slot_cycle_bar_cache[cache_key] = list(
                self._market_data.fetch_stock_bars(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    credential_context=credential_context,
                )
            )
        return list(self._slot_cycle_bar_cache[cache_key])

    def _load_execution_trade_documents_cached(
        self,
        *,
        uid: str,
        account_id: int,
        force_refresh: bool = False,
    ) -> list[dict[str, Any]]:
        cache_key = (uid, int(account_id))
        if force_refresh or cache_key not in self._slot_cycle_trade_docs_cache:
            self._slot_cycle_trade_docs_cache[cache_key] = list(
                self._runtime_store.load_execution_trade_performance_documents(
                    uid=uid,
                    account_id=account_id,
                )
            )
        return list(self._slot_cycle_trade_docs_cache[cache_key])

    def _execute_order(
        self,
        *,
        runtime_request: ExecutionRuntimeRequest,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
    ) -> AlpacaPaperExecutionResult:
        action = runtime_config.action
        client_order_id = f"execution-{action}-{uuid4().hex[:15]}"
        assignment = self._resolve_assignment_for_symbol(
            runtime_config=runtime_config,
            symbol=runtime_config.symbol,
        )
        submission_state = self._paper_trading.get_submission_state(
            symbol=runtime_config.symbol,
            credential_context=account_context,
        )

        if action == "buy":
            guardrail_decision = self._enforce_global_buy_guardrails(
                runtime_config=runtime_config,
                account_context=account_context,
                submission_state=submission_state,
            )
            if guardrail_decision is not None:
                return guardrail_decision
            risk_decision = self._enforce_buy_risk(
                runtime_config=runtime_config,
                submission_state=submission_state,
                assignment=assignment,
            )
            if risk_decision is not None:
                return risk_decision
            if runtime_config.qty is not None:
                result = self._paper_trading.submit_market_order_qty(
                    symbol=runtime_config.symbol,
                    side="buy",
                    qty=runtime_config.qty,
                    client_order_id=client_order_id,
                    action=action,
                    credential_context=account_context,
                )
                if result.submitted:
                    self._slot_cycle_entry_count += 1
                return result
            if runtime_config.notional is not None:
                result = self._paper_trading.submit_market_order_notional(
                    symbol=runtime_config.symbol,
                    side="buy",
                    notional=runtime_config.notional,
                    client_order_id=client_order_id,
                    action=action,
                    credential_context=account_context,
                )
                if result.submitted:
                    self._slot_cycle_entry_count += 1
                return result
            raise ValueError("Execution runtime buy orders require qty or notional.")

        if action == "sell":
            position = submission_state.position
            if position is None or position.qty <= 0:
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=None,
                    client_order_id=client_order_id,
                    side="sell",
                    notional=None,
                    skipped_reason="no_position_to_sell",
                    broker_error_code="no_position_to_sell",
                    broker_error_message="Execution runtime sell skipped because the slot has no open position.",
                )
            if runtime_config.qty is None:
                raise ValueError("Execution runtime sell orders require qty.")
            if runtime_config.qty > position.qty:
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=None,
                    client_order_id=client_order_id,
                    side="sell",
                    notional=None,
                    skipped_reason="insufficient_position_qty",
                    broker_error_code="insufficient_position_qty",
                    broker_error_message="Execution runtime sell skipped because qty exceeds the open position.",
                )
            return self._paper_trading.submit_market_order_qty(
                symbol=runtime_config.symbol,
                side="sell",
                qty=runtime_config.qty,
                client_order_id=client_order_id,
                action=action,
                credential_context=account_context,
            )

        if action == "close":
            position = submission_state.position
            if position is None or position.qty <= 0:
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=None,
                    client_order_id=client_order_id,
                    side="sell",
                    notional=None,
                    skipped_reason="no_position_to_close",
                    broker_error_code="no_position_to_close",
                    broker_error_message="Execution runtime close skipped because the slot has no open position.",
                )
            return self._paper_trading.close_position_symbol(
                symbol=runtime_config.symbol,
                action=action,
                client_order_id=client_order_id,
                credential_context=account_context,
            )

        if action == "cancel":
            order_payload = self._resolve_target_order(
                runtime_request=runtime_request,
                account_context=account_context,
            )
            order_symbol = _maybe_string(order_payload.get("symbol")) if isinstance(order_payload, dict) else None
            assignment = self._resolve_assignment_for_symbol(
                runtime_config=runtime_config,
                symbol=order_symbol or runtime_config.symbol,
            )
            order_management_decision = self._enforce_order_management(
                runtime_config=runtime_config,
                assignment=assignment,
                action=action,
                order_payload=order_payload,
            )
            if order_management_decision is not None:
                return order_management_decision
            if order_payload is None:
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=runtime_request.order_id,
                    client_order_id=runtime_request.client_order_id,
                    side=None,
                    notional=None,
                    skipped_reason="order_not_open",
                    broker_error_code="order_not_open",
                    broker_error_message="Execution runtime cancel skipped because the target order is not open.",
                )
            return self._paper_trading.cancel_order(
                order_id=str(order_payload.get("id", runtime_request.order_id or "")),
                client_order_id=_maybe_string(order_payload.get("client_order_id")) or runtime_request.client_order_id,
                action=action,
                credential_context=account_context,
            )

        if action == "modify":
            order_payload = self._resolve_target_order(
                runtime_request=runtime_request,
                account_context=account_context,
            )
            order_symbol = _maybe_string(order_payload.get("symbol")) if isinstance(order_payload, dict) else None
            assignment = self._resolve_assignment_for_symbol(
                runtime_config=runtime_config,
                symbol=order_symbol or runtime_config.symbol,
            )
            order_management_decision = self._enforce_order_management(
                runtime_config=runtime_config,
                assignment=assignment,
                action=action,
                order_payload=order_payload,
            )
            if order_management_decision is not None:
                return order_management_decision
            if order_payload is None:
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=runtime_request.order_id,
                    client_order_id=runtime_request.client_order_id,
                    side=None,
                    notional=None,
                    skipped_reason="order_not_open",
                    broker_error_code="order_not_open",
                    broker_error_message="Execution runtime modify skipped because the target order is not open.",
                )
            order_side = _maybe_string(order_payload.get("side")) or "buy"
            if order_side != "buy":
                return AlpacaPaperExecutionResult(
                    action=action,
                    submitted=False,
                    order_status="skipped",
                    order_id=_maybe_string(order_payload.get("id")),
                    client_order_id=_maybe_string(order_payload.get("client_order_id")),
                    side=order_side,
                    notional=_maybe_float(order_payload.get("notional")),
                    skipped_reason="modify_not_supported",
                    broker_error_code="modify_not_supported",
                    broker_error_message="Execution runtime modify is currently supported for open buy orders only.",
                )
            cancel_result = self._paper_trading.cancel_order(
                order_id=str(order_payload.get("id", runtime_request.order_id or "")),
                client_order_id=_maybe_string(order_payload.get("client_order_id")) or runtime_request.client_order_id,
                action="cancel",
                credential_context=account_context,
            )
            replacement_client_order_id = f"execution-modify-{uuid4().hex[:15]}"
            symbol = _maybe_string(order_payload.get("symbol")) or runtime_config.symbol
            if runtime_request.qty is not None:
                replacement = self._paper_trading.submit_market_order_qty(
                    symbol=symbol,
                    side="buy",
                    qty=runtime_request.qty,
                    client_order_id=replacement_client_order_id,
                    action="modify",
                    credential_context=account_context,
                )
            elif runtime_request.notional is not None:
                replacement = self._paper_trading.submit_market_order_notional(
                    symbol=symbol,
                    side="buy",
                    notional=runtime_request.notional,
                    client_order_id=replacement_client_order_id,
                    action="modify",
                    credential_context=account_context,
                )
            else:
                raise ValueError("Execution runtime modify orders require qty or notional.")
            return AlpacaPaperExecutionResult(
                action=action,
                submitted=replacement.submitted,
                order_status=replacement.order_status,
                order_id=replacement.order_id,
                client_order_id=replacement.client_order_id,
                side=replacement.side,
                notional=replacement.notional,
                broker_error_code=replacement.broker_error_code,
                broker_error_message=replacement.broker_error_message,
                raw_response={
                    "cancel_status": cancel_result.order_status,
                    "modify_status": "submitted" if replacement.submitted else "rejected",
                    "canceled_order_id": _maybe_string(order_payload.get("id")),
                    "replacement_order_id": replacement.order_id,
                    "raw_cancel_response": cancel_result.raw_response,
                    "raw_replacement_response": replacement.raw_response,
                },
            )

        raise ValueError(f"Execution runtime does not support action={action!r}.")

    def _resolve_target_order(
        self,
        *,
        runtime_request: ExecutionRuntimeRequest,
        account_context: ResolvedAlpacaAccountContext,
    ) -> dict[str, Any] | None:
        recent_orders = self._paper_trading.list_recent_orders(
            credential_context=account_context,
            limit=100,
        )
        for item in recent_orders:
            if not isinstance(item, dict):
                continue
            if runtime_request.order_id and _maybe_string(item.get("id")) == runtime_request.order_id:
                return item if _is_order_open(item) else None
            if runtime_request.client_order_id and _maybe_string(item.get("client_order_id")) == runtime_request.client_order_id:
                return item if _is_order_open(item) else None
        return None

    def _enforce_buy_risk(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        submission_state: AlpacaPaperSubmissionState,
        assignment: dict[str, Any] | None = None,
    ) -> AlpacaPaperExecutionResult | None:
        normalized_assignment = _normalize_assignment_control_state(assignment or {})
        risk_caps_enabled = bool(normalized_assignment.get("risk_caps_enabled", True))
        risk_settings = dict(runtime_config.risk_settings or {})
        if normalized_assignment:
            risk_settings.update({
                "max_positions": normalized_assignment.get("max_positions", risk_settings.get("max_positions")),
                "max_total_exposure_percent": normalized_assignment.get("max_total_exposure_percent", risk_settings.get("max_total_exposure_percent")),
            })
        if not risk_caps_enabled:
            return None

        position = submission_state.position
        if position is not None and position.qty > 0:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="risk_position_already_open",
                broker_error_code="risk_position_already_open",
                broker_error_message="Execution runtime buy skipped because this symbol already has an open broker position.",
                raw_response={
                    "enforcement_reason": "position_already_open",
                    "open_position_qty": position.qty,
                    "open_positions_count": submission_state.account.open_positions_count,
                },
            )

        max_positions = _maybe_int(risk_settings.get("max_positions"))
        open_positions_count = submission_state.account.open_positions_count
        if max_positions is not None and max_positions > 0 and open_positions_count >= max_positions:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="risk_max_positions",
                broker_error_code="risk_max_positions",
                broker_error_message=(
                    "Execution runtime buy skipped because max_positions was reached for the slot risk settings."
                ),
                raw_response={
                    "enforcement_reason": "max_positions",
                    "open_positions_count": open_positions_count,
                    "max_positions": max_positions,
                },
            )

        max_total_exposure_percent = _maybe_float(risk_settings.get("max_total_exposure_percent"))
        account_equity = submission_state.account.equity
        total_exposure = submission_state.account.total_exposure or 0.0
        projected_exposure_percent: float | None = None
        if (
            max_total_exposure_percent is not None
            and max_total_exposure_percent > 0
            and account_equity is not None
            and account_equity > 0
        ):
            projected_notional = self._project_buy_notional(
                runtime_config=runtime_config,
                submission_state=submission_state,
            )
            if projected_notional is None or projected_notional <= 0:
                return AlpacaPaperExecutionResult(
                    action=runtime_config.action,
                    submitted=False,
                    order_status="skipped",
                    order_id=None,
                    client_order_id=None,
                    side="buy",
                    notional=runtime_config.notional,
                    skipped_reason="risk_invalid_size",
                    broker_error_code="risk_invalid_size",
                    broker_error_message=(
                        "Execution runtime buy skipped because the projected order size could not be resolved safely."
                    ),
                    raw_response={
                        "enforcement_reason": "invalid_size",
                        "position_size_mode": _maybe_string(risk_settings.get("position_size_mode")),
                        "qty": runtime_config.qty,
                        "notional": runtime_config.notional,
                        "latest_price": _maybe_float((runtime_config.strategy_settings or {}).get("_latest_price")),
                    },
                )
            projected_exposure_percent = ((total_exposure + projected_notional) / account_equity) * 100.0
            if projected_exposure_percent > max_total_exposure_percent:
                return AlpacaPaperExecutionResult(
                    action=runtime_config.action,
                    submitted=False,
                    order_status="skipped",
                    order_id=None,
                    client_order_id=None,
                    side="buy",
                    notional=projected_notional,
                    skipped_reason="risk_max_exposure",
                    broker_error_code="risk_max_exposure",
                    broker_error_message=(
                        "Execution runtime buy skipped because projected exposure exceeds max_total_exposure_percent."
                    ),
                    raw_response={
                        "enforcement_reason": "max_total_exposure_percent",
                        "projected_exposure_percent": round(projected_exposure_percent, 4),
                        "max_total_exposure_percent": max_total_exposure_percent,
                        "current_total_exposure": total_exposure,
                        "projected_order_notional": projected_notional,
                        "account_equity": account_equity,
                        "open_positions_count": open_positions_count,
                    },
                )
        return None

    def _enforce_global_buy_guardrails(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        submission_state: AlpacaPaperSubmissionState,
    ) -> AlpacaPaperExecutionResult | None:
        if not runtime_config.global_guardrails_enabled:
            return None

        snapshot = self._build_global_guardrail_snapshot(
            runtime_config=runtime_config,
            submission_state=submission_state,
        )
        if runtime_config.emergency_kill_switch:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="guardrail_kill_switch",
                broker_error_code="guardrail_kill_switch",
                broker_error_message="Execution runtime buy skipped because the slot emergency kill switch is active.",
                raw_response={
                    "guardrail_status": "blocked",
                    "guardrail_reason": "kill_switch",
                    "guardrail_metric_snapshot": snapshot,
                    "last_guardrail_check_at": self._now_provider().isoformat(),
                },
            )

        max_daily_loss_dollars = runtime_config.max_daily_loss_dollars
        max_daily_loss_percent = runtime_config.max_daily_loss_percent
        if (
            (max_daily_loss_dollars is not None and max_daily_loss_dollars > 0 and snapshot["daily_loss_dollars"] >= max_daily_loss_dollars)
            or (max_daily_loss_percent is not None and max_daily_loss_percent > 0 and snapshot["daily_loss_percent"] >= max_daily_loss_percent)
        ):
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="guardrail_daily_loss",
                broker_error_code="guardrail_daily_loss",
                broker_error_message="Execution runtime buy skipped because the slot daily loss guardrail was breached.",
                raw_response={
                    "guardrail_status": "blocked",
                    "guardrail_reason": "daily_loss",
                    "guardrail_metric_snapshot": snapshot,
                    "last_guardrail_check_at": self._now_provider().isoformat(),
                },
            )

        max_daily_trades = runtime_config.max_daily_trades
        if max_daily_trades is not None and max_daily_trades > 0 and snapshot["daily_trades"] >= max_daily_trades:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="guardrail_daily_trades",
                broker_error_code="guardrail_daily_trades",
                broker_error_message="Execution runtime buy skipped because the slot daily trades guardrail was reached.",
                raw_response={
                    "guardrail_status": "blocked",
                    "guardrail_reason": "daily_trades",
                    "guardrail_metric_snapshot": snapshot,
                    "last_guardrail_check_at": self._now_provider().isoformat(),
                },
            )

        max_open_positions_total = runtime_config.max_open_positions_total
        if max_open_positions_total is not None and max_open_positions_total > 0 and submission_state.account.open_positions_count >= max_open_positions_total:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="guardrail_open_positions",
                broker_error_code="guardrail_open_positions",
                broker_error_message="Execution runtime buy skipped because the slot open positions guardrail was reached.",
                raw_response={
                    "guardrail_status": "blocked",
                    "guardrail_reason": "open_positions",
                    "guardrail_metric_snapshot": snapshot,
                    "open_positions_count": submission_state.account.open_positions_count,
                    "last_guardrail_check_at": self._now_provider().isoformat(),
                },
            )

        max_new_entries_per_run = runtime_config.max_new_entries_per_run
        if max_new_entries_per_run is not None and max_new_entries_per_run > 0 and self._slot_cycle_entry_count >= max_new_entries_per_run:
            return AlpacaPaperExecutionResult(
                action=runtime_config.action,
                submitted=False,
                order_status="skipped",
                order_id=None,
                client_order_id=None,
                side="buy",
                notional=runtime_config.notional,
                skipped_reason="guardrail_run_entry_limit",
                broker_error_code="guardrail_run_entry_limit",
                broker_error_message="Execution runtime buy skipped because the slot per-run entry limit was reached.",
                raw_response={
                    "guardrail_status": "blocked",
                    "guardrail_reason": "run_entry_limit",
                    "guardrail_metric_snapshot": snapshot,
                    "last_guardrail_check_at": self._now_provider().isoformat(),
                },
            )

        return None

    def _enforce_global_guardrails(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        symbol: str,
        assignment: dict[str, Any],
        run_id: str,
        submission_state: AlpacaPaperSubmissionState,
    ) -> ExecutionRuntimeResult | None:
        check_at = self._now_provider().isoformat()
        if not runtime_config.global_guardrails_enabled:
            return None

        metric_snapshot = self._build_global_guardrail_snapshot(
            runtime_config=runtime_config,
            submission_state=submission_state,
        )
        if runtime_config.emergency_kill_switch:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_guardrail_kill_switch",
                message=f"Execution runtime skipped {symbol} because the slot emergency kill switch is active.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                guardrail_status="blocked",
                guardrail_reason="kill_switch",
                guardrail_metric_snapshot=metric_snapshot,
                last_guardrail_check_at=check_at,
            )

        max_daily_trades = runtime_config.max_daily_trades
        if max_daily_trades is not None and max_daily_trades > 0 and metric_snapshot["daily_trades"] >= max_daily_trades:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_guardrail_daily_trades",
                message=f"Execution runtime skipped {symbol} because the slot daily trades guardrail was reached.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                guardrail_status="blocked",
                guardrail_reason="daily_trades",
                guardrail_metric_snapshot=metric_snapshot,
                last_guardrail_check_at=check_at,
            )

        max_open_positions_total = runtime_config.max_open_positions_total
        if (
            max_open_positions_total is not None
            and max_open_positions_total > 0
            and submission_state.account.open_positions_count >= max_open_positions_total
        ):
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_guardrail_open_positions",
                message=f"Execution runtime skipped {symbol} because the slot open positions guardrail was reached.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                guardrail_status="blocked",
                guardrail_reason="open_positions",
                open_positions_count=submission_state.account.open_positions_count,
                guardrail_metric_snapshot=metric_snapshot,
                last_guardrail_check_at=check_at,
            )

        max_new_entries_per_run = runtime_config.max_new_entries_per_run
        if max_new_entries_per_run is not None and max_new_entries_per_run > 0 and self._slot_cycle_entry_count >= max_new_entries_per_run:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_guardrail_run_entry_limit",
                message=f"Execution runtime skipped {symbol} because the slot per-run entry cap was reached.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                guardrail_status="blocked",
                guardrail_reason="run_entry_limit",
                guardrail_metric_snapshot=metric_snapshot,
                last_guardrail_check_at=check_at,
            )

        max_daily_loss_dollars = runtime_config.max_daily_loss_dollars
        max_daily_loss_percent = runtime_config.max_daily_loss_percent
        if (
            (max_daily_loss_dollars is not None and max_daily_loss_dollars > 0 and metric_snapshot["daily_loss_dollars"] >= max_daily_loss_dollars)
            or (max_daily_loss_percent is not None and max_daily_loss_percent > 0 and metric_snapshot["daily_loss_percent"] >= max_daily_loss_percent)
        ):
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_guardrail_daily_loss",
                message=f"Execution runtime skipped {symbol} because the slot daily loss guardrail was breached.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                guardrail_status="blocked",
                guardrail_reason="daily_loss",
                guardrail_metric_snapshot=metric_snapshot,
                last_guardrail_check_at=check_at,
            )

        return None

    def _build_global_guardrail_snapshot(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        submission_state: AlpacaPaperSubmissionState,
    ) -> dict[str, Any]:
        trades = self._load_execution_trade_documents_cached(
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
        )
        today = self._now_provider().date()
        daily_trades = 0
        daily_loss_dollars = 0.0
        daily_loss_percent = 0.0
        for payload in trades:
            if not isinstance(payload, dict):
                continue
            if max(1, _maybe_int(payload.get("slot_number")) or 1) != runtime_config.slot_number:
                continue
            entry_at = _iso_to_datetime(payload.get("entry_filled_at")) or _iso_to_datetime(payload.get("entry_submitted_at"))
            if entry_at is not None and entry_at.date() == today:
                daily_trades += 1
            exit_at = _iso_to_datetime(payload.get("exit_filled_at")) or _iso_to_datetime(payload.get("updated_at"))
            if exit_at is None or exit_at.date() != today:
                continue
            realized_dollars = _maybe_float(payload.get("realized_pnl_dollars")) or 0.0
            realized_percent = _maybe_float(payload.get("realized_pnl_percent")) or 0.0
            if realized_dollars < 0:
                daily_loss_dollars += abs(realized_dollars)
            if realized_percent < 0:
                daily_loss_percent += abs(realized_percent)

        return {
            "daily_trades": daily_trades,
            "daily_loss_dollars": round(daily_loss_dollars, 2),
            "daily_loss_percent": round(daily_loss_percent, 2),
            "open_positions_count": submission_state.account.open_positions_count,
            "max_daily_trades": runtime_config.max_daily_trades,
            "max_daily_loss_dollars": runtime_config.max_daily_loss_dollars,
            "max_daily_loss_percent": runtime_config.max_daily_loss_percent,
            "max_open_positions_total": runtime_config.max_open_positions_total,
            "max_new_entries_per_run": runtime_config.max_new_entries_per_run,
            "new_entries_this_run": self._slot_cycle_entry_count,
            "kill_switch": runtime_config.emergency_kill_switch,
        }

    def _project_buy_notional(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        submission_state: AlpacaPaperSubmissionState,
    ) -> float | None:
        if runtime_config.notional is not None:
            return runtime_config.notional if runtime_config.notional > 0 else None
        if runtime_config.qty is None or runtime_config.qty <= 0:
            return None
        latest_price = _maybe_float((runtime_config.strategy_settings or {}).get("_latest_price"))
        if latest_price is not None and latest_price > 0:
            return runtime_config.qty * latest_price
        market_value = submission_state.position.market_value if submission_state.position is not None else None
        position_qty = submission_state.position.qty if submission_state.position is not None else None
        if market_value is not None and position_qty is not None and position_qty > 0:
            reference_price = market_value / position_qty
            if reference_price > 0:
                return runtime_config.qty * reference_price
        return None

    def _run_strategy_cycle(
        self,
        *,
        runtime_request: ExecutionRuntimeRequest,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        run_id: str,
    ) -> ExecutionRuntimeResult:
        symbol_assignments = runtime_config.symbol_assignments or {}
        if symbol_assignments == {}:
            summary = self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_no_symbols",
                message="Execution runtime skipped because the selected slot has no symbol assignments.",
                account_context=account_context,
            )
            self._runtime_store.write_runtime_result(
                runtime_request=runtime_request,
                runtime_config=runtime_config,
                account_context=account_context,
                result=summary,
                write_symbol_state=False,
            )
            return summary

        symbol_results: list[ExecutionRuntimeResult] = []
        for symbol, assignment in symbol_assignments.items():
            symbol_result = self._run_assigned_symbol(
                runtime_request=runtime_request,
                runtime_config=runtime_config,
                account_context=account_context,
                run_id=run_id,
                symbol=symbol,
                assignment=assignment,
            )
            symbol_results.append(symbol_result)
            symbol_request = ExecutionRuntimeRequest(
                user_id=runtime_request.user_id,
                account_id=runtime_request.account_id,
                slot=runtime_request.slot,
                symbol=symbol,
                action=symbol_result.action,
                qty=symbol_result.qty,
                notional=symbol_result.notional,
                alpaca_account_id=runtime_request.alpaca_account_id,
                broker_environment=runtime_request.broker_environment,
                payload=runtime_request.payload,
            )
            symbol_config = ExecutionRuntimeConfig(
                product_id=runtime_config.product_id,
                enabled=runtime_config.enabled,
                execution_mode=runtime_config.execution_mode,
                uid=runtime_config.uid,
                account_id=runtime_config.account_id,
                slot_number=runtime_config.slot_number,
                symbol=symbol,
                action=symbol_result.action,
                qty=symbol_result.qty,
                notional=symbol_result.notional,
                alpaca_account_id=runtime_config.alpaca_account_id,
                strategy_key=runtime_config.strategy_key,
                strategy_settings=runtime_config.strategy_settings,
                risk_settings=runtime_config.risk_settings,
                selected_symbols=runtime_config.selected_symbols,
                symbol_assignments=runtime_config.symbol_assignments,
                symbol_states=runtime_config.symbol_states,
                automation_enabled=runtime_config.automation_enabled,
                auto_disable_enabled=runtime_config.auto_disable_enabled,
                auto_disable_min_trades=runtime_config.auto_disable_min_trades,
                auto_disable_max_drawdown_percent=runtime_config.auto_disable_max_drawdown_percent,
                auto_disable_min_win_rate=runtime_config.auto_disable_min_win_rate,
                auto_disable_scope=runtime_config.auto_disable_scope,
                global_guardrails_enabled=runtime_config.global_guardrails_enabled,
                max_daily_loss_dollars=runtime_config.max_daily_loss_dollars,
                max_daily_loss_percent=runtime_config.max_daily_loss_percent,
                max_daily_trades=runtime_config.max_daily_trades,
                max_open_positions_total=runtime_config.max_open_positions_total,
                max_new_entries_per_run=runtime_config.max_new_entries_per_run,
                emergency_kill_switch=runtime_config.emergency_kill_switch,
            )
            self._runtime_store.write_runtime_result(
                runtime_request=symbol_request,
                runtime_config=symbol_config,
                account_context=account_context,
                result=symbol_result,
            )

        summary = self._summarize_strategy_results(
            runtime_config=runtime_config,
            account_context=account_context,
            run_id=run_id,
            symbol_results=symbol_results,
        )
        self._sync_execution_performance(
            runtime_config=runtime_config,
            account_context=account_context,
            results=symbol_results,
        )
        self._runtime_store.write_runtime_result(
            runtime_request=runtime_request,
            runtime_config=runtime_config,
            account_context=account_context,
            result=summary,
            write_symbol_state=False,
        )
        return summary

    def _enforce_assignment_auto_disable(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        symbol: str,
        assignment: dict[str, Any],
        run_id: str,
    ) -> ExecutionRuntimeResult | None:
        assignment = _normalize_assignment_control_state(assignment)
        if not bool(assignment.get("auto_disable_enabled", runtime_config.auto_disable_enabled)):
            return None

        strategy_key = str(assignment.get("strategy_key", runtime_config.strategy_key or "ema")).strip().lower() or "ema"
        performance_snapshot = self._build_assignment_performance_snapshot(
            runtime_config=runtime_config,
            symbol=symbol,
            strategy_key=strategy_key,
        )
        total_trades = int(performance_snapshot.get("total_trades", 0) or 0)
        assignment_min_trades = max(1, _maybe_int(assignment.get("auto_disable_min_trades")) or runtime_config.auto_disable_min_trades)
        assignment_min_win_rate = max(0.0, min(100.0, _maybe_float(assignment.get("auto_disable_min_win_rate")) or runtime_config.auto_disable_min_win_rate))
        assignment_max_drawdown = max(0.0, _maybe_float(assignment.get("auto_disable_max_drawdown_percent")) or runtime_config.auto_disable_max_drawdown_percent)
        if total_trades < assignment_min_trades:
            return None

        win_rate = _maybe_float(performance_snapshot.get("win_rate")) or 0.0
        max_drawdown_percent = _maybe_float(performance_snapshot.get("max_drawdown_percent")) or 0.0
        auto_disabled_reason: str | None = None
        if win_rate < assignment_min_win_rate:
            auto_disabled_reason = "auto_disabled_win_rate"
        elif max_drawdown_percent > assignment_max_drawdown:
            auto_disabled_reason = "auto_disabled_drawdown"

        if auto_disabled_reason is None:
            return None

        now = self._now_provider().isoformat()
        updated_assignment = _normalize_assignment_control_state({
            **assignment,
            "enabled": False,
            "manually_disabled": False,
            "auto_disabled": True,
            "disabled_source": "auto",
            "disabled_reason": auto_disabled_reason,
            "auto_disabled_reason": auto_disabled_reason,
            "auto_disabled_at": now,
            "last_runtime_decision_at": now,
        })
        self._persist_assignment_control_state(
            runtime_config=runtime_config,
            symbol=symbol,
            updated_assignment=updated_assignment,
        )

        return self._build_result(
            runtime_config=runtime_config,
            run_id=run_id,
            execution_status="skipped_auto_disabled",
            message=f"Execution runtime auto-disabled {symbol} because {auto_disabled_reason.replace('_', ' ')} thresholds were breached.",
            account_context=account_context,
            symbol=symbol,
            action="evaluate",
            enforcement_reason=auto_disabled_reason,
            manually_disabled=False,
            auto_disabled=True,
            disabled_source="auto",
            disabled_reason=auto_disabled_reason,
            auto_disabled_at=now,
            last_runtime_decision_at=now,
            enforcement_metric_snapshot=performance_snapshot,
            raw_response={
                "strategy_key": strategy_key,
                "auto_disable": True,
                "performance_snapshot": performance_snapshot,
            },
        )

    def _build_assignment_performance_snapshot(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        symbol: str,
        strategy_key: str,
    ) -> dict[str, Any]:
        trades = self._load_execution_trade_documents_cached(
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
        )
        relevant_trades = [
            payload for payload in trades
            if isinstance(payload, dict)
            and str(payload.get("trade_state", "")).strip().lower() == "closed"
            and max(1, _maybe_int(payload.get("slot_number")) or 1) == runtime_config.slot_number
            and (_maybe_string(payload.get("symbol")) or "").upper() == symbol
            and (_maybe_string(payload.get("strategy_key")) or "").lower() == strategy_key
        ]
        if not relevant_trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "current_drawdown_percent": 0.0,
                "max_drawdown_percent": 0.0,
            }

        total_trades = 0
        wins = 0
        losses = 0
        cumulative_value = 100.0
        peak_value = 100.0
        current_drawdown_percent = 0.0
        max_drawdown_percent = 0.0

        sorted_trades = sorted(
            relevant_trades,
            key=lambda item: item.get("exit_filled_at") or item.get("updated_at") or "",
        )
        for payload in sorted_trades:
            total_trades += 1
            realized_percent = _maybe_float(payload.get("realized_pnl_percent")) or 0.0
            realized_dollars = _maybe_float(payload.get("realized_pnl_dollars")) or 0.0
            if realized_dollars > 0:
                wins += 1
            elif realized_dollars < 0:
                losses += 1
            cumulative_value += realized_percent
            peak_value = max(peak_value, cumulative_value)
            if peak_value > 0:
                current_drawdown_percent = max(0.0, ((peak_value - cumulative_value) / peak_value) * 100.0)
                max_drawdown_percent = max(max_drawdown_percent, current_drawdown_percent)

        return {
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / total_trades) * 100.0, 2) if total_trades > 0 else 0.0,
            "current_drawdown_percent": round(current_drawdown_percent, 2),
            "max_drawdown_percent": round(max_drawdown_percent, 2),
        }

    def _persist_assignment_control_state(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        symbol: str,
        updated_assignment: dict[str, Any],
    ) -> None:
        symbol_assignments = {
            key: _normalize_assignment_control_state(value)
            for key, value in (runtime_config.symbol_assignments or {}).items()
            if isinstance(value, dict)
        }
        symbol_assignments[symbol] = _normalize_assignment_control_state(updated_assignment)
        config_payload = {
            "auto_disable_enabled": runtime_config.auto_disable_enabled,
            "auto_disable_min_trades": runtime_config.auto_disable_min_trades,
            "auto_disable_max_drawdown_percent": runtime_config.auto_disable_max_drawdown_percent,
            "auto_disable_min_win_rate": runtime_config.auto_disable_min_win_rate,
            "auto_disable_scope": runtime_config.auto_disable_scope,
            "global_guardrails_enabled": runtime_config.global_guardrails_enabled,
            "max_daily_loss_dollars": runtime_config.max_daily_loss_dollars,
            "max_daily_loss_percent": runtime_config.max_daily_loss_percent,
            "max_daily_trades": runtime_config.max_daily_trades,
            "max_open_positions_total": runtime_config.max_open_positions_total,
            "max_new_entries_per_run": runtime_config.max_new_entries_per_run,
            "emergency_kill_switch": runtime_config.emergency_kill_switch,
            "symbol_assignments": symbol_assignments,
            "symbol_states": _symbol_assignments_to_symbol_states(symbol_assignments),
            "selected_symbols": list(symbol_assignments.keys()),
            "settings_updated_at": self._now_provider().isoformat(),
        }
        self._runtime_store.write_execution_slot_config(
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
            slot_number=runtime_config.slot_number,
            config_payload=config_payload,
        )

    def _resolve_assignment_for_symbol(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        symbol: str | None,
    ) -> dict[str, Any] | None:
        resolved_symbol = (symbol or "").strip().upper()
        assignments = runtime_config.symbol_assignments or {}
        if resolved_symbol and isinstance(assignments.get(resolved_symbol), dict):
            return _normalize_assignment_control_state(assignments[resolved_symbol])
        return None

    def _enforce_order_management(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        assignment: dict[str, Any] | None,
        action: str,
        order_payload: dict[str, Any] | None,
    ) -> AlpacaPaperExecutionResult | None:
        normalized_assignment = _normalize_assignment_control_state(assignment or {})
        if not normalized_assignment:
            return None
        if not bool(normalized_assignment.get("order_management_enabled", True)):
            return AlpacaPaperExecutionResult(
                action=action,
                submitted=False,
                order_status="skipped",
                order_id=_maybe_string(order_payload.get("id")) if isinstance(order_payload, dict) else None,
                client_order_id=_maybe_string(order_payload.get("client_order_id")) if isinstance(order_payload, dict) else None,
                side=_maybe_string(order_payload.get("side")) if isinstance(order_payload, dict) else None,
                notional=_maybe_float(order_payload.get("notional")) if isinstance(order_payload, dict) else None,
                skipped_reason="order_management_disabled",
                broker_error_code="order_management_disabled",
                broker_error_message="Execution runtime skipped because order management is disabled for this symbol assignment.",
            )
        if action == "cancel" and not bool(normalized_assignment.get("allow_cancel", True)):
            return AlpacaPaperExecutionResult(
                action=action,
                submitted=False,
                order_status="skipped",
                order_id=_maybe_string(order_payload.get("id")) if isinstance(order_payload, dict) else None,
                client_order_id=_maybe_string(order_payload.get("client_order_id")) if isinstance(order_payload, dict) else None,
                side=_maybe_string(order_payload.get("side")) if isinstance(order_payload, dict) else None,
                notional=_maybe_float(order_payload.get("notional")) if isinstance(order_payload, dict) else None,
                skipped_reason="cancel_disabled",
                broker_error_code="cancel_disabled",
                broker_error_message="Execution runtime skipped because cancel is disabled for this symbol assignment.",
            )
        if action == "modify" and not bool(normalized_assignment.get("allow_modify", True)):
            return AlpacaPaperExecutionResult(
                action=action,
                submitted=False,
                order_status="skipped",
                order_id=_maybe_string(order_payload.get("id")) if isinstance(order_payload, dict) else None,
                client_order_id=_maybe_string(order_payload.get("client_order_id")) if isinstance(order_payload, dict) else None,
                side=_maybe_string(order_payload.get("side")) if isinstance(order_payload, dict) else None,
                notional=_maybe_float(order_payload.get("notional")) if isinstance(order_payload, dict) else None,
                skipped_reason="modify_disabled",
                broker_error_code="modify_disabled",
                broker_error_message="Execution runtime skipped because modify is disabled for this symbol assignment.",
            )
        return None

    def _sync_execution_performance(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext | None,
        results: list[ExecutionRuntimeResult],
    ) -> None:
        if account_context is None:
            return

        tracked_results = [
            result
            for result in results
            if result.symbol not in {"", "SLOT"}
            and result.execution_status in {"buy_submitted", "close_submitted"}
        ]

        try:
            trade_documents = {
                str(payload.get("trade_id", "")).strip(): dict(payload)
                for payload in self._load_execution_trade_documents_cached(
                    uid=runtime_config.uid,
                    account_id=runtime_config.account_id,
                )
                if isinstance(payload, dict) and str(payload.get("trade_id", "")).strip() != ""
            }
            recent_orders = self._paper_trading.list_recent_orders(
                credential_context=account_context,
                limit=200,
            )
        except Exception as exc:
            logger.warning(
                "Execution performance sync skipped account_id=%s slot=%s reason=%s",
                runtime_config.account_id,
                runtime_config.slot_number,
                exc,
            )
            return

        order_index = _index_orders_by_identity(recent_orders)
        now = self._now_provider().isoformat()

        for result in tracked_results:
            if result.execution_status == "buy_submitted":
                trade_id = _build_execution_trade_id(result)
                existing = trade_documents.get(trade_id, {})
                trade_documents[trade_id] = {
                    **existing,
                    "trade_id": trade_id,
                    "product_id": self.PRODUCT_ID,
                    "slot_number": result.slot,
                    "symbol": result.symbol,
                    "strategy_key": ((runtime_config.symbol_assignments or {}).get(result.symbol, {}) or {}).get("strategy_key", runtime_config.strategy_key),
                    "trade_state": existing.get("trade_state", "open_order_submitted"),
                    "entry_order_id": result.order_id or existing.get("entry_order_id"),
                    "entry_client_order_id": result.client_order_id or existing.get("entry_client_order_id"),
                    "entry_submitted_at": existing.get("entry_submitted_at") or now,
                    "updated_at": now,
                }
            elif result.execution_status == "close_submitted":
                open_trade_id = _find_latest_open_trade_id(
                    trade_documents=trade_documents,
                    slot_number=result.slot,
                    symbol=result.symbol,
                )
                if open_trade_id is None:
                    continue
                existing = trade_documents.get(open_trade_id, {})
                trade_documents[open_trade_id] = {
                    **existing,
                    "trade_state": "close_order_submitted",
                    "exit_order_id": result.order_id or existing.get("exit_order_id"),
                    "exit_client_order_id": result.client_order_id or existing.get("exit_client_order_id"),
                    "exit_submitted_at": existing.get("exit_submitted_at") or now,
                    "updated_at": now,
                }

        for trade_id, payload in list(trade_documents.items()):
            entry_order = _resolve_order_from_index(
                order_index,
                order_id=_maybe_string(payload.get("entry_order_id")),
                client_order_id=_maybe_string(payload.get("entry_client_order_id")),
            )
            exit_order = _resolve_order_from_index(
                order_index,
                order_id=_maybe_string(payload.get("exit_order_id")),
                client_order_id=_maybe_string(payload.get("exit_client_order_id")),
            )

            if payload.get("trade_state") in {"open_order_submitted", "opening"}:
                if _is_filled_order(entry_order):
                    entry_price = _maybe_float(entry_order.get("filled_avg_price"))
                    qty = _maybe_float(entry_order.get("filled_qty")) or _maybe_float(entry_order.get("qty"))
                    if entry_price is not None and qty is not None and qty > 0:
                        payload.update({
                            "trade_state": "open",
                            "qty": qty,
                            "entry_price": entry_price,
                            "entry_notional": round(entry_price * qty, 2),
                            "entry_filled_at": _maybe_string(entry_order.get("filled_at")) or _maybe_string(entry_order.get("updated_at")) or payload.get("entry_filled_at"),
                            "updated_at": now,
                        })
                elif _is_terminal_rejected_order(entry_order):
                    payload.update({
                        "trade_state": "canceled",
                        "updated_at": now,
                    })

            if payload.get("trade_state") == "close_order_submitted":
                if _is_filled_order(exit_order):
                    entry_price = _maybe_float(payload.get("entry_price"))
                    qty = _maybe_float(payload.get("qty")) or _maybe_float(exit_order.get("filled_qty")) or _maybe_float(exit_order.get("qty"))
                    exit_price = _maybe_float(exit_order.get("filled_avg_price"))
                    if entry_price is not None and qty is not None and qty > 0 and exit_price is not None:
                        realized_pnl = round((exit_price - entry_price) * qty, 2)
                        entry_notional = round(entry_price * qty, 2)
                        exit_notional = round(exit_price * qty, 2)
                        payload.update({
                            "trade_state": "closed",
                            "qty": qty,
                            "exit_price": exit_price,
                            "exit_notional": exit_notional,
                            "exit_filled_at": _maybe_string(exit_order.get("filled_at")) or _maybe_string(exit_order.get("updated_at")) or payload.get("exit_filled_at"),
                            "realized_pnl_dollars": realized_pnl,
                            "realized_pnl": realized_pnl,
                            "realized_pnl_percent": round((realized_pnl / entry_notional) * 100.0, 2) if entry_notional > 0 else 0.0,
                            "trade_outcome": "win" if realized_pnl > 0 else "loss" if realized_pnl < 0 else "breakeven",
                            "updated_at": now,
                        })
                elif _is_terminal_rejected_order(exit_order):
                    payload.update({
                        "trade_state": "open",
                        "updated_at": now,
                    })

            trade_documents[trade_id] = payload

        if trade_documents:
            self._runtime_store.write_execution_trade_performance_batch(
                uid=runtime_config.uid,
                account_id=runtime_config.account_id,
                trade_payloads=trade_documents,
            )
            self._slot_cycle_trade_docs_cache[(runtime_config.uid, int(runtime_config.account_id))] = [
                dict(payload) for payload in trade_documents.values()
            ]

    def _run_assigned_symbol(
        self,
        *,
        runtime_request: ExecutionRuntimeRequest,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        run_id: str,
        symbol: str,
        assignment: dict[str, Any],
    ) -> ExecutionRuntimeResult:
        symbol = symbol.strip().upper()
        assignment = _normalize_assignment_control_state(assignment)
        strategy_key = str(assignment.get("strategy_key", "ema")).strip().lower() or "ema"
        if bool(assignment.get("manually_disabled")) or str(assignment.get("disabled_source") or "").strip().lower() == "manual":
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_manually_disabled",
                message=f"Execution runtime skipped {symbol} because the symbol assignment is manually disabled.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                manually_disabled=True,
                auto_disabled=False,
                disabled_source="manual",
                disabled_reason=_maybe_string(assignment.get("disabled_reason")) or "manually_disabled",
                last_runtime_decision_at=self._now_provider().isoformat(),
                raw_response={"strategy_key": strategy_key, "assignment_enabled": False, "disabled_source": "manual"},
            )
        if bool(assignment.get("auto_disabled")) or str(assignment.get("disabled_source") or "").strip().lower() == "auto":
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_auto_disabled",
                message=f"Execution runtime skipped {symbol} because the symbol assignment is auto-disabled.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                manually_disabled=False,
                auto_disabled=True,
                disabled_source="auto",
                disabled_reason=_maybe_string(assignment.get("auto_disabled_reason")) or _maybe_string(assignment.get("disabled_reason")) or "auto_disabled",
                auto_disabled_at=_maybe_string(assignment.get("auto_disabled_at")),
                last_runtime_decision_at=self._now_provider().isoformat(),
                raw_response={"strategy_key": strategy_key, "assignment_enabled": False, "disabled_source": "auto"},
            )
        auto_disable_result = self._enforce_assignment_auto_disable(
            runtime_config=runtime_config,
            account_context=account_context,
            symbol=symbol,
            assignment=assignment,
            run_id=run_id,
        )
        if auto_disable_result is not None:
            return auto_disable_result
        if strategy_key not in {"ema", "pullback", "breakout", "rsi_reversion", "momentum", "vwap", "bollinger_reversion", "adx_trend", "donchian_breakout", "relative_strength", "opening_range_breakout"}:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_strategy_not_implemented",
                message=f"Execution runtime skipped {symbol} because strategy_key={strategy_key!r} is not implemented yet.",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                raw_response={"strategy_key": strategy_key},
            )
        try:
            if strategy_key == "breakout":
                strategy_config = BreakoutStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "vwap":
                strategy_config = VwapStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "bollinger_reversion":
                strategy_config = BollingerReversionStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "adx_trend":
                strategy_config = AdxTrendStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "donchian_breakout":
                strategy_config = DonchianBreakoutStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "relative_strength":
                strategy_config = RelativeStrengthStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "opening_range_breakout":
                strategy_config = OpeningRangeBreakoutStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "momentum":
                strategy_config = MomentumStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "rsi_reversion":
                strategy_config = RsiReversionStrategyConfig.from_payload(assignment.get("strategy_settings"))
            elif strategy_key == "pullback":
                strategy_config = PullbackStrategyConfig.from_payload(assignment.get("strategy_settings"))
            else:
                strategy_config = EmaStrategyConfig.from_payload(assignment.get("strategy_settings"))
        except ValueError as exc:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_invalid_config",
                message=str(exc),
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                raw_response={"strategy_key": strategy_key},
            )
        try:
            timeframe = _map_execution_timeframe(strategy_config.timeframe)
            requested_bar_limit = _execution_strategy_bar_fetch_limit(
                timeframe=strategy_config.timeframe,
                required_bar_count=strategy_config.required_bar_count,
            )
            bars = self._fetch_stock_bars_cached(
                symbol=symbol,
                timeframe=timeframe,
                limit=requested_bar_limit,
                credential_context=account_context,
            )
        except Exception as exc:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status="skipped_market_data_unavailable",
                message=f"Execution runtime skipped {symbol} because market data was unavailable: {exc}",
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
            )

        benchmark_bars: list[Any] | None = None
        if strategy_key == "relative_strength":
            try:
                benchmark_bars = self._fetch_stock_bars_cached(
                    symbol=strategy_config.benchmark_symbol,
                    timeframe=timeframe,
                    limit=requested_bar_limit,
                    credential_context=account_context,
                )
            except Exception as exc:
                return self._build_result(
                    runtime_config=runtime_config,
                    run_id=run_id,
                    execution_status="skipped_market_data_unavailable",
                    message=(
                        f"Execution runtime skipped {symbol} because benchmark market data for "
                        f"{strategy_config.benchmark_symbol} was unavailable: {exc}"
                    ),
                    account_context=account_context,
                    symbol=symbol,
                    action="evaluate",
                )

        if strategy_key == "breakout":
            evaluation = evaluate_breakout_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "vwap":
            evaluation = evaluate_vwap_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "bollinger_reversion":
            evaluation = evaluate_bollinger_reversion_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "adx_trend":
            evaluation = evaluate_adx_trend_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "donchian_breakout":
            evaluation = evaluate_donchian_breakout_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "relative_strength":
            evaluation = evaluate_relative_strength_strategy(
                symbol=symbol,
                bars=bars,
                benchmark_bars=benchmark_bars or [],
                config=strategy_config,
            )
        elif strategy_key == "opening_range_breakout":
            evaluation = evaluate_opening_range_breakout_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "momentum":
            evaluation = evaluate_momentum_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "rsi_reversion":
            evaluation = evaluate_rsi_reversion_strategy(symbol=symbol, bars=bars, config=strategy_config)
        elif strategy_key == "pullback":
            evaluation = evaluate_pullback_strategy(symbol=symbol, bars=bars, config=strategy_config)
        else:
            evaluation = evaluate_ema_strategy(symbol=symbol, bars=bars, config=strategy_config)
        if evaluation.status != "signal_ready" or evaluation.action is None:
            return self._build_result(
                runtime_config=runtime_config,
                run_id=run_id,
                execution_status=evaluation.status,
                message=evaluation.message,
                account_context=account_context,
                symbol=symbol,
                action="evaluate",
                manually_disabled=bool(assignment.get("manually_disabled")),
                auto_disabled=bool(assignment.get("auto_disabled")),
                disabled_source=_maybe_string(assignment.get("disabled_source")),
                disabled_reason=_maybe_string(assignment.get("disabled_reason")),
                auto_disabled_at=_maybe_string(assignment.get("auto_disabled_at")),
                last_runtime_decision_at=self._now_provider().isoformat(),
                last_processed_bar_at=_maybe_string(_evaluation_payload(evaluation).get("latest_bar_ended_at")),
                raw_response=_evaluation_payload(evaluation),
            )

        action = evaluation.action
        trade_config = self._build_trade_config(
            runtime_config=runtime_config,
            symbol=symbol,
            action=action,
            assignment=assignment,
            latest_price=bars[-1].close if bars else None,
        )
        trade_request = ExecutionRuntimeRequest(
            user_id=runtime_request.user_id,
            account_id=runtime_request.account_id,
            slot=runtime_request.slot,
            symbol=symbol,
            action=action,
            qty=trade_config.qty,
            notional=trade_config.notional,
            alpaca_account_id=runtime_request.alpaca_account_id,
            broker_environment=runtime_request.broker_environment,
            payload=runtime_request.payload,
        )
        try:
            execution_result = self._execute_order(
                runtime_request=trade_request,
                runtime_config=trade_config,
                account_context=account_context,
            )
        except AlpacaPaperTradingError as exc:
            return self._build_result(
                runtime_config=trade_config,
                run_id=run_id,
                execution_status="failed",
                message=_normalize_execution_error_message(exc.message),
                account_context=account_context,
                symbol=symbol,
                action=action,
                broker_error_code=exc.code,
                broker_error_message=_normalize_execution_error_message(exc.message),
                manually_disabled=bool(assignment.get("manually_disabled")),
                auto_disabled=bool(assignment.get("auto_disabled")),
                disabled_source=_maybe_string(assignment.get("disabled_source")),
                disabled_reason=_maybe_string(assignment.get("disabled_reason")),
                auto_disabled_at=_maybe_string(assignment.get("auto_disabled_at")),
                last_runtime_decision_at=self._now_provider().isoformat(),
                last_processed_bar_at=_maybe_string(_evaluation_payload(evaluation).get("latest_bar_ended_at")),
                raw_response=exc.raw_response,
            )
        except ValueError as exc:
            return self._build_result(
                runtime_config=trade_config,
                run_id=run_id,
                execution_status="skipped_invalid_config",
                message=str(exc),
                account_context=account_context,
                symbol=symbol,
                action=action,
                manually_disabled=bool(assignment.get("manually_disabled")),
                auto_disabled=bool(assignment.get("auto_disabled")),
                disabled_source=_maybe_string(assignment.get("disabled_source")),
                disabled_reason=_maybe_string(assignment.get("disabled_reason")),
                auto_disabled_at=_maybe_string(assignment.get("auto_disabled_at")),
                last_runtime_decision_at=self._now_provider().isoformat(),
                last_processed_bar_at=_maybe_string(_evaluation_payload(evaluation).get("latest_bar_ended_at")),
                raw_response=_evaluation_payload(evaluation),
            )

        execution_status = _strategy_execution_status(action=action, execution_result=execution_result)
        message = _strategy_result_message(symbol=symbol, action=action, execution_result=execution_result)
        return self._build_result(
            runtime_config=trade_config,
            run_id=run_id,
            execution_status=execution_status,
            message=message,
            account_context=account_context,
            symbol=symbol,
            action=action,
            order_id=execution_result.order_id,
            client_order_id=execution_result.client_order_id,
            side=execution_result.side,
            qty=trade_config.qty,
            notional=execution_result.notional if execution_result.notional is not None else trade_config.notional,
            broker_error_code=execution_result.broker_error_code,
            broker_error_message=execution_result.broker_error_message,
            enforcement_reason=_raw_response_string(execution_result.raw_response, "enforcement_reason"),
            projected_exposure_percent=_raw_response_float(execution_result.raw_response, "projected_exposure_percent"),
            open_positions_count=_raw_response_int(execution_result.raw_response, "open_positions_count"),
            guardrail_status=_raw_response_string(execution_result.raw_response, "guardrail_status"),
            guardrail_reason=_raw_response_string(execution_result.raw_response, "guardrail_reason"),
            guardrail_metric_snapshot=_raw_response_dict(execution_result.raw_response, "guardrail_metric_snapshot"),
            last_guardrail_check_at=_raw_response_string(execution_result.raw_response, "last_guardrail_check_at"),
            manually_disabled=bool(assignment.get("manually_disabled")),
            auto_disabled=bool(assignment.get("auto_disabled")),
            disabled_source=_maybe_string(assignment.get("disabled_source")),
            disabled_reason=_maybe_string(assignment.get("disabled_reason")),
            auto_disabled_at=_maybe_string(assignment.get("auto_disabled_at")),
            last_runtime_decision_at=self._now_provider().isoformat(),
            last_processed_bar_at=_maybe_string(_evaluation_payload(evaluation).get("latest_bar_ended_at")),
            raw_response={
                "evaluation": _evaluation_payload(evaluation),
                "broker": execution_result.raw_response,
            },
        )

    def _build_trade_config(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        symbol: str,
        action: str,
        assignment: dict[str, Any],
        latest_price: float | None = None,
    ) -> ExecutionRuntimeConfig:
        risk_settings = assignment.get("risk_settings") if isinstance(assignment.get("risk_settings"), dict) else (runtime_config.risk_settings or {})
        position_size_mode = str(risk_settings.get("position_size_mode", "qty")).strip().lower() or "qty"
        strategy_settings = (
            dict(assignment.get("strategy_settings"))
            if isinstance(assignment.get("strategy_settings"), dict)
            else dict(runtime_config.strategy_settings or {})
        )
        if latest_price is not None:
            strategy_settings["_latest_price"] = latest_price
        qty = runtime_config.qty
        notional = runtime_config.notional
        if action == "buy":
            if position_size_mode == "notional":
                notional = _maybe_float(risk_settings.get("default_notional"))
                qty = None
            else:
                qty = _maybe_float(risk_settings.get("default_qty"))
                notional = None
        else:
            qty = None
            notional = None
        return ExecutionRuntimeConfig(
            product_id=runtime_config.product_id,
            enabled=runtime_config.enabled,
            execution_mode=runtime_config.execution_mode,
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
            slot_number=runtime_config.slot_number,
            symbol=symbol,
            action=action,
            qty=qty,
            notional=notional,
            alpaca_account_id=runtime_config.alpaca_account_id,
            strategy_key=str(assignment.get("strategy_key", runtime_config.strategy_key or "ema")).strip().lower() or "ema",
            strategy_settings=strategy_settings,
            risk_settings=risk_settings,
            selected_symbols=runtime_config.selected_symbols,
            symbol_assignments=runtime_config.symbol_assignments,
            symbol_states=runtime_config.symbol_states,
            automation_enabled=runtime_config.automation_enabled,
            auto_disable_enabled=runtime_config.auto_disable_enabled,
            auto_disable_min_trades=runtime_config.auto_disable_min_trades,
            auto_disable_max_drawdown_percent=runtime_config.auto_disable_max_drawdown_percent,
            auto_disable_min_win_rate=runtime_config.auto_disable_min_win_rate,
            auto_disable_scope=runtime_config.auto_disable_scope,
            global_guardrails_enabled=runtime_config.global_guardrails_enabled,
            max_daily_loss_dollars=runtime_config.max_daily_loss_dollars,
            max_daily_loss_percent=runtime_config.max_daily_loss_percent,
            max_daily_trades=runtime_config.max_daily_trades,
            max_open_positions_total=runtime_config.max_open_positions_total,
            max_new_entries_per_run=runtime_config.max_new_entries_per_run,
            emergency_kill_switch=runtime_config.emergency_kill_switch,
        )

    def _summarize_strategy_results(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        account_context: ResolvedAlpacaAccountContext,
        run_id: str,
        symbol_results: list[ExecutionRuntimeResult],
    ) -> ExecutionRuntimeResult:
        counts: dict[str, int] = {}
        for item in symbol_results:
            counts[item.execution_status] = counts.get(item.execution_status, 0) + 1
        ordered_statuses = [
            "buy_submitted",
            "close_submitted",
            "sell_submitted",
            "failed",
            "skipped_auto_disabled",
            "skipped_manually_disabled",
            "skipped_order_management_disabled",
            "skipped_cancel_disabled",
            "skipped_modify_disabled",
            "skipped_guardrail_kill_switch",
            "skipped_guardrail_daily_loss",
            "skipped_guardrail_daily_trades",
            "skipped_guardrail_open_positions",
            "skipped_guardrail_run_entry_limit",
            "skipped_risk_max_exposure",
            "skipped_risk_max_positions",
            "skipped_risk_invalid_size",
            "skipped_invalid_config",
            "skipped_strategy_not_implemented",
            "skipped_market_data_unavailable",
            "skipped_direction_filter",
            "skipped_no_open_position",
            "no_signal",
        ]
        summary_status = "no_signal"
        primary_result: ExecutionRuntimeResult | None = None
        for status in ordered_statuses:
            if counts.get(status):
                summary_status = status
                primary_result = next((item for item in symbol_results if item.execution_status == status), None)
                break
        summary_message = "; ".join(
            f"{status}={counts[status]}" for status in ordered_statuses if counts.get(status)
        ) or "no_signal=0"
        return self._build_result(
            runtime_config=runtime_config,
            run_id=run_id,
            execution_status=summary_status,
            message=f"Execution slot cycle completed: {summary_message}.",
            account_context=account_context,
            symbol="SLOT",
            action="evaluate",
            enforcement_reason=primary_result.enforcement_reason if primary_result is not None else None,
            projected_exposure_percent=primary_result.projected_exposure_percent if primary_result is not None else None,
            open_positions_count=primary_result.open_positions_count if primary_result is not None else None,
            guardrail_status=primary_result.guardrail_status if primary_result is not None else None,
            guardrail_reason=primary_result.guardrail_reason if primary_result is not None else None,
            guardrail_metric_snapshot=primary_result.guardrail_metric_snapshot if primary_result is not None else None,
            last_guardrail_check_at=primary_result.last_guardrail_check_at if primary_result is not None else None,
            raw_response={
                "summary_counts": counts,
                "symbols": [
                    {
                        "symbol": item.symbol,
                        "action": item.action,
                        "execution_status": item.execution_status,
                        "message": item.message,
                    }
                    for item in symbol_results
                ],
            },
        )

    def _build_result(
        self,
        *,
        runtime_config: ExecutionRuntimeConfig,
        run_id: str,
        execution_status: str,
        message: str,
        account_context: ResolvedAlpacaAccountContext | None,
        symbol: str | None = None,
        action: str | None = None,
        order_id: str | None = None,
        replacement_order_id: str | None = None,
        client_order_id: str | None = None,
        side: str | None = None,
        qty: float | None = None,
        notional: float | None = None,
        broker_error_code: str | None = None,
        broker_error_message: str | None = None,
        enforcement_reason: str | None = None,
        projected_exposure_percent: float | None = None,
        open_positions_count: int | None = None,
        cancel_status: str | None = None,
        modify_status: str | None = None,
        manually_disabled: bool | None = None,
        auto_disabled: bool | None = None,
        disabled_source: str | None = None,
        disabled_reason: str | None = None,
        auto_disabled_at: str | None = None,
        re_enabled_at: str | None = None,
        re_enabled_by: str | None = None,
        last_runtime_decision_at: str | None = None,
        enforcement_metric_snapshot: dict[str, Any] | None = None,
        guardrail_status: str | None = None,
        guardrail_reason: str | None = None,
        guardrail_metric_snapshot: dict[str, Any] | None = None,
        last_guardrail_check_at: str | None = None,
        last_processed_bar_at: str | None = None,
        raw_response: dict[str, Any] | None = None,
    ) -> ExecutionRuntimeResult:
        resolved_symbol = (symbol or runtime_config.symbol or "SLOT").strip().upper()
        resolved_action = (action or runtime_config.action or "evaluate").strip().lower()
        return ExecutionRuntimeResult(
            ok=execution_status in {"submitted", "buy_submitted", "sell_submitted", "close_submitted"},
            run_id=run_id,
            product_id=self.PRODUCT_ID,
            uid=runtime_config.uid,
            account_id=runtime_config.account_id,
            slot=runtime_config.slot_number,
            symbol=resolved_symbol,
            action=resolved_action,
            execution_status=execution_status,
            message=message,
            firestore_paths=asdict(
                self._runtime_store.get_paths(
                    uid=runtime_config.uid,
                    account_id=runtime_config.account_id,
                    slot_number=runtime_config.slot_number,
                    symbol=resolved_symbol,
                )
            ),
            broker_environment=account_context.environment if account_context is not None else None,
            alpaca_account_id=account_context.alpaca_account_id if account_context is not None else runtime_config.alpaca_account_id,
            order_id=order_id,
            replacement_order_id=replacement_order_id,
            client_order_id=client_order_id,
            side=side,
            qty=qty,
            notional=notional,
            broker_error_code=broker_error_code,
            broker_error_message=broker_error_message,
            enforcement_reason=enforcement_reason,
            projected_exposure_percent=projected_exposure_percent,
            open_positions_count=open_positions_count,
            cancel_status=cancel_status,
            modify_status=modify_status,
            manually_disabled=manually_disabled,
            auto_disabled=auto_disabled,
            disabled_source=disabled_source,
            disabled_reason=disabled_reason,
            auto_disabled_at=auto_disabled_at,
            re_enabled_at=re_enabled_at,
            re_enabled_by=re_enabled_by,
            last_runtime_decision_at=last_runtime_decision_at,
            enforcement_metric_snapshot=enforcement_metric_snapshot,
            guardrail_status=guardrail_status,
            guardrail_reason=guardrail_reason,
            guardrail_metric_snapshot=guardrail_metric_snapshot,
            last_guardrail_check_at=last_guardrail_check_at,
            last_processed_bar_at=last_processed_bar_at,
            raw_response=raw_response,
        )

    def _validate_payload(self, payload: dict[str, Any]) -> ExecutionRuntimeRequest:
        if not isinstance(payload, dict):
            raise ValueError("Execution runtime payload must be a JSON object.")
        user_id = str(payload.get("user_id", "")).strip()
        symbol = str(payload.get("symbol", "")).strip().upper()
        action = str(payload.get("action", "")).strip().lower()
        order_id = _maybe_string(payload.get("order_id"))
        client_order_id = _maybe_string(payload.get("client_order_id"))
        if user_id == "":
            raise ValueError("Execution runtime payload requires user_id.")
        if action and action not in SUPPORTED_EXECUTION_ACTIONS:
            raise ValueError(
                f"Execution runtime payload action must be one of {sorted(SUPPORTED_EXECUTION_ACTIONS)}."
            )
        if action in {"buy", "sell", "close"} and bool(symbol) != bool(action):
            raise ValueError("Execution runtime payload requires symbol and action together for manual execution.")
        if action in {"cancel", "modify"} and order_id is None and client_order_id is None:
            raise ValueError("Execution runtime payload requires order_id or client_order_id for cancel and modify actions.")
        if action == "modify" and _maybe_float(payload.get("qty")) is None and _maybe_float(payload.get("notional")) is None:
            raise ValueError("Execution runtime payload requires qty or notional for modify actions.")
        account_id = _require_int(payload.get("account_id"), field_name="account_id")
        slot_number = max(1, _require_int(payload.get("slot", 1), field_name="slot"))
        alpaca_account_id = _maybe_int(payload.get("alpaca_account_id"))
        broker_environment = _maybe_string(payload.get("broker_environment"))
        qty = _maybe_float(payload.get("qty"))
        notional = _maybe_float(payload.get("notional"))
        return ExecutionRuntimeRequest(
            user_id=user_id,
            account_id=account_id,
            slot=slot_number,
            symbol=symbol,
            action=action,
            qty=qty,
            notional=notional,
            order_id=order_id,
            client_order_id=client_order_id,
            alpaca_account_id=alpaca_account_id,
            broker_environment=broker_environment,
            payload=payload,
        )


def build_execution_runtime_service(settings: AppConfig) -> ExecutionRuntimeService:
    return ExecutionRuntimeService(settings=settings)


def _require_int(value: Any, *, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Execution runtime payload requires integer {field_name}.") from exc


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_guardrail_limit(value: Any, *, default: float | None = None) -> float | None:
    resolved = _maybe_float(value)
    if resolved is None:
        return default
    if resolved <= 0:
        return None
    return float(resolved)


def _normalize_guardrail_int(value: Any, *, default: int | None = None) -> int | None:
    resolved = _maybe_int(value)
    if resolved is None:
        return default
    if resolved <= 0:
        return None
    return int(resolved)


def _snapshot_id(snapshot: Any) -> str | None:
    snapshot_id = getattr(snapshot, "id", None)
    if snapshot_id is None:
        return None
    text = str(snapshot_id).strip()
    return text or None


def _snapshot_reference(snapshot: Any) -> Any | None:
    return getattr(snapshot, "reference", None)


def _parse_slot_number(slot_id: Any) -> int | None:
    slot_text = _maybe_string(slot_id)
    if slot_text is None:
        return None
    if slot_text.startswith("slot_"):
        slot_text = slot_text[5:]
    try:
        return max(1, int(slot_text))
    except (TypeError, ValueError):
        return None


def _normalize_symbol_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    resolved: list[str] = []
    for item in value:
        text = str(item).strip().upper()
        if text:
            resolved.append(text)
    return tuple(resolved)


def _normalize_symbol_assignments(
    value: Any,
    *,
    selected_symbols: tuple[str, ...],
    fallback_strategy_key: str,
    fallback_strategy_settings: dict[str, Any],
    fallback_risk_settings: dict[str, Any],
    fallback_auto_disable_settings: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    fallback_auto_disable_settings = fallback_auto_disable_settings or {}
    if isinstance(value, dict):
        for symbol, assignment in value.items():
            resolved_symbol = str(symbol).strip().upper()
            if resolved_symbol == "" or not isinstance(assignment, dict):
                continue
            normalized[resolved_symbol] = {
                "symbol": resolved_symbol,
                "enabled": bool(assignment.get("enabled", True)),
                "manually_disabled": bool(assignment.get("manually_disabled", False)),
                "auto_disabled": bool(assignment.get("auto_disabled", False)),
                "disabled_source": _maybe_string(assignment.get("disabled_source")),
                "disabled_reason": _maybe_string(assignment.get("disabled_reason")),
                "auto_disabled_at": _maybe_string(assignment.get("auto_disabled_at")),
                "auto_disabled_reason": _maybe_string(assignment.get("auto_disabled_reason")),
                "last_runtime_decision_at": _maybe_string(assignment.get("last_runtime_decision_at")),
                "re_enabled_at": _maybe_string(assignment.get("re_enabled_at")),
                "re_enabled_by": _maybe_string(assignment.get("re_enabled_by")),
                "auto_disable_enabled": bool(assignment.get("auto_disable_enabled", fallback_auto_disable_settings.get("auto_disable_enabled", True))),
                "auto_disable_min_trades": max(1, _maybe_int(assignment.get("auto_disable_min_trades")) or _maybe_int(fallback_auto_disable_settings.get("auto_disable_min_trades")) or 5),
                "auto_disable_max_drawdown_percent": max(0.0, _maybe_float(assignment.get("auto_disable_max_drawdown_percent")) or _maybe_float(fallback_auto_disable_settings.get("auto_disable_max_drawdown_percent")) or 12.0),
                "auto_disable_min_win_rate": max(0.0, min(100.0, _maybe_float(assignment.get("auto_disable_min_win_rate")) or _maybe_float(fallback_auto_disable_settings.get("auto_disable_min_win_rate")) or 35.0)),
                "risk_caps_enabled": bool(assignment.get("risk_caps_enabled", True)),
                "max_total_exposure_percent": _maybe_float(assignment.get("max_total_exposure_percent")),
                "max_positions": _maybe_int(assignment.get("max_positions")),
                "order_management_enabled": bool(assignment.get("order_management_enabled", True)),
                "allow_cancel": bool(assignment.get("allow_cancel", True)),
                "allow_modify": bool(assignment.get("allow_modify", True)),
                "strategy_key": _maybe_string(assignment.get("strategy_key")) or fallback_strategy_key,
                "strategy_settings": assignment.get("strategy_settings") if isinstance(assignment.get("strategy_settings"), dict) else dict(fallback_strategy_settings),
                "risk_settings": assignment.get("risk_settings") if isinstance(assignment.get("risk_settings"), dict) else dict(fallback_risk_settings),
                "updated_at": _maybe_string(assignment.get("updated_at")),
                "name": _maybe_string(assignment.get("name")),
                "tradable": assignment.get("tradable"),
            }
            normalized[resolved_symbol] = _normalize_assignment_control_state(normalized[resolved_symbol])
    for symbol in selected_symbols:
        if symbol in normalized:
            continue
        normalized[symbol] = {
            "symbol": symbol,
            "enabled": True,
            "manually_disabled": False,
            "auto_disabled": False,
            "disabled_source": None,
            "disabled_reason": None,
            "auto_disabled_at": None,
            "auto_disabled_reason": None,
            "last_runtime_decision_at": None,
            "re_enabled_at": None,
            "re_enabled_by": None,
            "auto_disable_enabled": bool(fallback_auto_disable_settings.get("auto_disable_enabled", True)),
            "auto_disable_min_trades": max(1, _maybe_int(fallback_auto_disable_settings.get("auto_disable_min_trades")) or 5),
            "auto_disable_max_drawdown_percent": max(0.0, _maybe_float(fallback_auto_disable_settings.get("auto_disable_max_drawdown_percent")) or 12.0),
            "auto_disable_min_win_rate": max(0.0, min(100.0, _maybe_float(fallback_auto_disable_settings.get("auto_disable_min_win_rate")) or 35.0)),
            "risk_caps_enabled": True,
            "max_total_exposure_percent": _maybe_float(fallback_risk_settings.get("max_total_exposure_percent")) or 20.0,
            "max_positions": _maybe_int(fallback_risk_settings.get("max_positions")) or 5,
            "order_management_enabled": True,
            "allow_cancel": True,
            "allow_modify": True,
            "strategy_key": fallback_strategy_key,
            "strategy_settings": dict(fallback_strategy_settings),
            "risk_settings": dict(fallback_risk_settings),
            "updated_at": None,
            "name": None,
            "tradable": None,
        }
        normalized[symbol] = _normalize_assignment_control_state(normalized[symbol])
    return normalized


def _payload_matches_expected_linked_account(
    payload: dict[str, Any] | None,
    *,
    expected_alpaca_account_id: int | None,
) -> bool:
    if expected_alpaca_account_id is None:
        return True
    if not isinstance(payload, dict):
        return False
    payload_alpaca_account_id = _maybe_int(payload.get("alpaca_account_id"))
    return payload_alpaca_account_id == expected_alpaca_account_id


def _normalize_auto_disable_scope(value: Any) -> str:
    scope = (_maybe_string(value) or "symbol_assignment").lower()
    return scope if scope in {"strategy", "symbol_assignment"} else "symbol_assignment"


def _normalize_assignment_control_state(assignment: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(assignment.get("enabled", True))
    manually_disabled = bool(assignment.get("manually_disabled", False))
    auto_disabled = bool(assignment.get("auto_disabled", False))
    disabled_source = (_maybe_string(assignment.get("disabled_source")) or "").lower() or None
    disabled_reason = _maybe_string(assignment.get("disabled_reason"))
    auto_disabled_reason = _maybe_string(assignment.get("auto_disabled_reason"))

    if enabled:
        manually_disabled = False
        auto_disabled = False
        disabled_source = None
        disabled_reason = None
        auto_disabled_reason = None
    elif auto_disabled or disabled_source == "auto":
        enabled = False
        manually_disabled = False
        auto_disabled = True
        disabled_source = "auto"
        disabled_reason = disabled_reason or auto_disabled_reason or "auto_disabled"
        auto_disabled_reason = auto_disabled_reason or disabled_reason
    else:
        enabled = False
        manually_disabled = True
        auto_disabled = False
        disabled_source = "manual"
        disabled_reason = disabled_reason or "manually_disabled"
        auto_disabled_reason = None

    return {
        **assignment,
        "enabled": enabled,
        "manually_disabled": manually_disabled,
        "auto_disabled": auto_disabled,
        "disabled_source": disabled_source,
        "disabled_reason": disabled_reason,
        "auto_disabled_reason": auto_disabled_reason,
        "auto_disable_enabled": bool(assignment.get("auto_disable_enabled", True)),
        "auto_disable_min_trades": max(1, _maybe_int(assignment.get("auto_disable_min_trades")) or 5),
        "auto_disable_max_drawdown_percent": max(0.0, _maybe_float(assignment.get("auto_disable_max_drawdown_percent")) or 12.0),
        "auto_disable_min_win_rate": max(0.0, min(100.0, _maybe_float(assignment.get("auto_disable_min_win_rate")) or 35.0)),
        "risk_caps_enabled": bool(assignment.get("risk_caps_enabled", True)),
        "max_total_exposure_percent": _maybe_float(assignment.get("max_total_exposure_percent")),
        "max_positions": _maybe_int(assignment.get("max_positions")),
        "order_management_enabled": bool(assignment.get("order_management_enabled", True)),
        "allow_cancel": bool(assignment.get("allow_cancel", True)),
        "allow_modify": bool(assignment.get("allow_modify", True)),
    }


def _assignment_is_runnable(assignment: dict[str, Any]) -> bool:
    normalized = _normalize_assignment_control_state(assignment)
    return bool(normalized.get("enabled", True)) and not bool(normalized.get("manually_disabled")) and not bool(normalized.get("auto_disabled"))


def _symbol_assignments_to_symbol_states(symbol_assignments: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for symbol, assignment in symbol_assignments.items():
        normalized = _normalize_assignment_control_state(assignment)
        states.append({
            "symbol": symbol,
            "name": _maybe_string(normalized.get("name")),
            "tradable": normalized.get("tradable"),
            "mode": "active" if _assignment_is_runnable(normalized) else "paused",
            "updated_at": _maybe_string(normalized.get("updated_at")),
        })
    return states


def _result_side(action: str) -> str | None:
    normalized = (action or "").strip().lower()
    if normalized == "buy":
        return "buy"
    if normalized in {"sell", "close"}:
        return "sell"
    return None


def _result_message(runtime_config: ExecutionRuntimeConfig, execution_result: AlpacaPaperExecutionResult) -> str:
    if execution_result.submitted:
        if runtime_config.action == "cancel":
            return f"Execution runtime submitted cancel request for order {execution_result.order_id or execution_result.client_order_id or 'unknown'}."
        if runtime_config.action == "modify":
            replacement_order_id = _raw_response_string(execution_result.raw_response, "replacement_order_id")
            return f"Execution runtime submitted modify replacement order {replacement_order_id or execution_result.order_id or 'unknown'}."
        return (
            f"Execution runtime submitted {runtime_config.action} order for "
            f"{runtime_config.symbol} on slot {runtime_config.slot_number}."
        )
    if execution_result.broker_error_message:
        return execution_result.broker_error_message
    if execution_result.skipped_reason:
        return f"Execution runtime skipped {runtime_config.action}: {execution_result.skipped_reason}."
    return f"Execution runtime skipped {runtime_config.action} for {runtime_config.symbol}."


def _normalize_execution_error_message(message: str) -> str:
    return (message or "").replace("Prime Stocks execution", "execution runtime")


def _strategy_execution_status(*, action: str, execution_result: AlpacaPaperExecutionResult) -> str:
    if execution_result.submitted:
        if action == "buy":
            return "buy_submitted"
        if action == "sell":
            return "sell_submitted"
        if action == "close":
            return "close_submitted"
        return "submitted"
    code = execution_result.broker_error_code or execution_result.skipped_reason or "skipped"
    mapping = {
        "risk_position_already_open": "skipped_existing_position",
        "no_position_to_sell": "skipped_no_open_position",
        "no_position_to_close": "skipped_no_open_position",
        "insufficient_position_qty": "skipped_no_open_position",
        "guardrail_kill_switch": "skipped_guardrail_kill_switch",
        "guardrail_daily_loss": "skipped_guardrail_daily_loss",
        "guardrail_daily_trades": "skipped_guardrail_daily_trades",
        "guardrail_open_positions": "skipped_guardrail_open_positions",
        "guardrail_run_entry_limit": "skipped_guardrail_run_entry_limit",
        "risk_max_exposure": "skipped_risk_max_exposure",
        "risk_max_positions": "skipped_risk_max_positions",
        "risk_invalid_size": "skipped_risk_invalid_size",
    }
    return mapping.get(code, "failed" if execution_result.order_status == "rejected" else "skipped")


def _manual_execution_status(*, action: str, execution_result: AlpacaPaperExecutionResult) -> str:
    if execution_result.submitted:
        if action == "buy":
            return "buy_submitted"
        if action == "sell":
            return "sell_submitted"
        if action == "close":
            return "close_submitted"
        if action == "cancel":
            return "cancel_submitted"
        if action == "modify":
            return "modify_submitted"
        return "submitted"
    code = execution_result.broker_error_code or execution_result.skipped_reason or "skipped"
    mapping = {
        "order_not_open": "skipped_order_not_open",
        "modify_not_supported": "modify_rejected",
        "order_management_disabled": "skipped_order_management_disabled",
        "cancel_disabled": "skipped_cancel_disabled",
        "modify_disabled": "skipped_modify_disabled",
        "risk_position_already_open": "skipped_existing_position",
        "guardrail_kill_switch": "skipped_guardrail_kill_switch",
        "guardrail_daily_loss": "skipped_guardrail_daily_loss",
        "guardrail_daily_trades": "skipped_guardrail_daily_trades",
        "guardrail_open_positions": "skipped_guardrail_open_positions",
        "guardrail_run_entry_limit": "skipped_guardrail_run_entry_limit",
        "risk_max_exposure": "skipped_risk_max_exposure",
        "risk_max_positions": "skipped_risk_max_positions",
        "risk_invalid_size": "skipped_risk_invalid_size",
        "no_position_to_close": "skipped_no_open_position",
        "no_position_to_sell": "skipped_no_open_position",
        "insufficient_position_qty": "skipped_no_open_position",
    }
    if action == "cancel" and execution_result.order_status == "rejected":
        return "cancel_rejected"
    if action == "modify" and execution_result.order_status == "rejected":
        return "modify_rejected"
    return mapping.get(code, "failed" if execution_result.order_status == "rejected" else "skipped")


def _strategy_result_message(*, symbol: str, action: str, execution_result: AlpacaPaperExecutionResult) -> str:
    if execution_result.submitted:
        verb = "buy" if action == "buy" else "close" if action == "close" else action
        return f"Execution runtime submitted {verb} order for {symbol}."
    if execution_result.broker_error_message:
        return execution_result.broker_error_message
    if execution_result.skipped_reason:
        return f"Execution runtime skipped {action} for {symbol}: {execution_result.skipped_reason}."
    return f"Execution runtime skipped {action} for {symbol}."


def _evaluation_payload(evaluation: Any) -> dict[str, Any]:
    return {
        "status": getattr(evaluation, "status", None),
        "message": getattr(evaluation, "message", None),
        "action": getattr(evaluation, "action", None),
        "signal_name": getattr(evaluation, "signal_name", None),
        "latest_close": getattr(evaluation, "latest_close", None),
        "latest_bar_ended_at": getattr(evaluation, "latest_bar_ended_at", None),
        "fast_ema": getattr(evaluation, "fast_ema", None),
        "slow_ema": getattr(evaluation, "slow_ema", None),
    }


def _raw_response_string(value: Any, key: str) -> str | None:
    if not isinstance(value, dict):
        return None
    return _maybe_string(value.get(key))


def _raw_response_float(value: Any, key: str) -> float | None:
    if not isinstance(value, dict):
        return None
    return _maybe_float(value.get(key))


def _raw_response_int(value: Any, key: str) -> int | None:
    if not isinstance(value, dict):
        return None
    return _maybe_int(value.get(key))


def _raw_response_dict(value: Any, key: str) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    nested = value.get(key)
    return dict(nested) if isinstance(nested, dict) else None


def _iso_to_datetime(value: Any) -> datetime | None:
    text = _maybe_string(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _build_execution_trade_id(result: ExecutionRuntimeResult) -> str:
    order_identity = result.order_id or result.client_order_id or result.run_id
    return f"slot_{result.slot}__{result.symbol}__{order_identity}"


def _is_filled_order(order_payload: dict[str, Any] | None) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = (_maybe_string(order_payload.get("status")) or "").lower()
    return status in {"filled", "partially_filled"}


def _is_terminal_rejected_order(order_payload: dict[str, Any] | None) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = (_maybe_string(order_payload.get("status")) or "").lower()
    return status in {"canceled", "cancelled", "rejected", "expired"}


def _index_orders_by_identity(orders: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for payload in orders:
        if not isinstance(payload, dict):
            continue
        order_id = _maybe_string(payload.get("id"))
        client_order_id = _maybe_string(payload.get("client_order_id"))
        if order_id is not None:
            indexed[f"id:{order_id}"] = payload
        if client_order_id is not None:
            indexed[f"client:{client_order_id}"] = payload
    return indexed


def _resolve_order_from_index(
    order_index: dict[str, dict[str, Any]],
    *,
    order_id: str | None,
    client_order_id: str | None,
) -> dict[str, Any] | None:
    if order_id is not None and f"id:{order_id}" in order_index:
        return order_index[f"id:{order_id}"]
    if client_order_id is not None and f"client:{client_order_id}" in order_index:
        return order_index[f"client:{client_order_id}"]
    return None


def _find_latest_open_trade_id(
    *,
    trade_documents: dict[str, dict[str, Any]],
    slot_number: int,
    symbol: str,
) -> str | None:
    symbol_key = symbol.strip().upper()
    candidates: list[tuple[str, str]] = []
    for trade_id, payload in trade_documents.items():
        if not isinstance(payload, dict):
            continue
        if int(payload.get("slot_number", 0) or 0) != slot_number:
            continue
        if (_maybe_string(payload.get("symbol")) or "").upper() != symbol_key:
            continue
        if (_maybe_string(payload.get("trade_state")) or "").lower() not in {"open", "open_order_submitted", "opening"}:
            continue
        sort_key = _maybe_string(payload.get("entry_filled_at")) or _maybe_string(payload.get("entry_submitted_at")) or trade_id
        candidates.append((sort_key, trade_id))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _map_execution_timeframe(timeframe: str) -> str:
    normalized = (timeframe or "").strip().lower()
    mapping = {
        "15m": "15Min",
        "1h": "1Hour",
    }
    if normalized not in mapping:
        raise ValueError("Execution runtime supports EMA timeframes 15m and 1h only.")
    return mapping[normalized]


def _execution_strategy_bar_fetch_limit(*, timeframe: str, required_bar_count: int) -> int:
    normalized = (timeframe or "").strip().lower()
    baseline = 160 if normalized == "15m" else 120
    return max(int(required_bar_count) + 25, baseline)


def _is_order_open(order_payload: dict[str, Any]) -> bool:
    status = (_maybe_string(order_payload.get("status")) or "").lower()
    return status in {"new", "accepted", "pending_new", "partially_filled", "accepted_for_bidding"}
