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

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from app.brokers.alpaca_paper_trading import AlpacaPaperExecutionResult
from app.products.stocks.bismel1.models import PrimeStocksStrategyResult
from app.shared.config import AppConfig

SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})


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
    execution_timeframe: str
    trend_timeframe: str
    pullback_window: int
    execution_bar_limit: int
    trend_bar_limit: int
    first_lot_notional: float
    multi_notional: float
    runtime_target: str = "cloud_run"


@dataclass(frozen=True)
class PrimeStocksRuntimeStorePaths:
    config_document: str
    state_document: str
    snapshot_document: str
    signal_document: str
    execution_document: str
    action_document: str
    logs_collection: str


@dataclass(frozen=True)
class PrimeStocksLatestExecutionRecord:
    execution_key: str | None = None
    order_status: str | None = None
    run_id: str | None = None
    candidate_action: str | None = None
    latest_signal_time: str | None = None


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

    def get_paths(self) -> PrimeStocksRuntimeStorePaths:
        root = f"{self._settings.firestore_runtime_collection}/{self._settings.firestore_product_document}"
        return PrimeStocksRuntimeStorePaths(
            config_document=f"{root}/config/current",
            state_document=f"{root}/state/current",
            snapshot_document=f"{root}/snapshots/latest",
            signal_document=f"{root}/signals/latest",
            execution_document=f"{root}/execution/current",
            action_document=f"{root}/actions/latest",
            logs_collection=f"{root}/logs",
        )

    def load_runtime_config(self, default_config: PrimeStocksRuntimeConfigRecord) -> PrimeStocksRuntimeConfigRecord:
        snapshot = self._config_document().get()
        payload = snapshot.to_dict() if getattr(snapshot, "exists", False) else None
        if not payload:
            self._config_document().set(asdict(default_config), merge=True)
            return default_config
        return PrimeStocksRuntimeConfigRecord(
            product_key=str(payload.get("product_key", default_config.product_key)),
            strategy_key=str(payload.get("strategy_key", default_config.strategy_key)),
            strategy_title=str(payload.get("strategy_title", default_config.strategy_title)),
            symbol=str(payload.get("symbol", default_config.symbol)).upper(),
            asset_type=_normalize_asset_type(str(payload.get("asset_type", default_config.asset_type))),
            enabled=bool(payload.get("enabled", default_config.enabled)),
            dry_run=bool(payload.get("dry_run", default_config.dry_run)),
            paper_execution_enabled=bool(payload.get("paper_execution_enabled", default_config.paper_execution_enabled)),
            execution_timeframe=str(payload.get("execution_timeframe", default_config.execution_timeframe)),
            trend_timeframe=str(payload.get("trend_timeframe", default_config.trend_timeframe)),
            pullback_window=int(payload.get("pullback_window", default_config.pullback_window)),
            execution_bar_limit=int(payload.get("execution_bar_limit", default_config.execution_bar_limit)),
            trend_bar_limit=int(payload.get("trend_bar_limit", default_config.trend_bar_limit)),
            first_lot_notional=float(payload.get("first_lot_notional", default_config.first_lot_notional)),
            multi_notional=float(payload.get("multi_notional", default_config.multi_notional)),
            runtime_target=str(payload.get("runtime_target", default_config.runtime_target)),
        )

    def load_latest_execution_record(self) -> PrimeStocksLatestExecutionRecord:
        snapshot = self._execution_document().get()
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

    def write_runtime_result(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        strategy_result: PrimeStocksStrategyResult,
        candidate_action: str,
        latest_signal_time: datetime | None,
        runtime_message: str,
        execution_mode: str,
        execution_decision: str,
        execution_result: AlpacaPaperExecutionResult | None,
        skipped_reason: str | None,
    ) -> None:
        now = datetime.now(tz=UTC)
        serialized_signal = _serialize_signal(strategy_result)
        serialized_execution = _serialize_execution(
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            execution_mode=execution_mode,
            execution_decision=execution_decision,
            execution_result=execution_result,
            skipped_reason=skipped_reason,
        )
        latest_snapshot = {
            "run_id": run_id,
            "product_key": runtime_config.product_key,
            "strategy_key": runtime_config.strategy_key,
            "symbol": runtime_config.symbol,
            "asset_type": runtime_config.asset_type,
            "status": strategy_result.status,
            "runtime_target": runtime_config.runtime_target,
            "dry_run": runtime_config.dry_run,
            "paper_execution_enabled": runtime_config.paper_execution_enabled,
            "runtime_message": runtime_message,
            "candidate_action": candidate_action,
            "execution_timeframe": strategy_result.execution_timeframe,
            "trend_timeframe": strategy_result.trend_timeframe,
            "pullback_window": runtime_config.pullback_window,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "updated_at": now.isoformat(),
            "strategy_message": strategy_result.message,
            "state": _serialize_state(strategy_result.final_state),
            "signal": serialized_signal,
            "execution": serialized_execution,
        }
        latest_signal = {
            "run_id": run_id,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "signal": serialized_signal,
            "updated_at": now.isoformat(),
        }
        current_state = {
            "run_id": run_id,
            "enabled": runtime_config.enabled,
            "dry_run": runtime_config.dry_run,
            "paper_execution_enabled": runtime_config.paper_execution_enabled,
            "last_processed_bar_time": _isoformat_or_none(latest_signal_time),
            "latest_candidate_action": candidate_action,
            "latest_status": strategy_result.status,
            "runtime_target": runtime_config.runtime_target,
            "latest_execution_decision": execution_decision,
            "latest_order_status": serialized_execution["order_status"],
            "updated_at": now.isoformat(),
        }
        execution_current = {
            "run_id": run_id,
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
            "updated_at": now.isoformat(),
        }
        latest_action = {
            "run_id": run_id,
            "candidate_action": candidate_action,
            "execution_decision": execution_decision,
            "execution_mode": execution_mode,
            "execution": serialized_execution,
            "updated_at": now.isoformat(),
        }
        log_payload = {
            "run_id": run_id,
            "level": "INFO",
            "kind": "runtime_execution",
            "message": runtime_message,
            "symbol": runtime_config.symbol,
            "status": strategy_result.status,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "execution_decision": execution_decision,
            "execution_mode": execution_mode,
            "execution": serialized_execution,
            "created_at": now.isoformat(),
        }
        self._snapshot_document().set(latest_snapshot, merge=True)
        self._signal_document().set(latest_signal, merge=True)
        self._state_document().set(current_state, merge=True)
        self._execution_document().set(execution_current, merge=True)
        self._action_document().set(latest_action, merge=True)
        self._logs_collection().document(run_id).set(log_payload, merge=True)

    def create_run_id(self) -> str:
        return f"run-{uuid4().hex[:15]}"

    def _config_document(self) -> Any:
        return self._root_document().collection("config").document("current")

    def _state_document(self) -> Any:
        return self._root_document().collection("state").document("current")

    def _snapshot_document(self) -> Any:
        return self._root_document().collection("snapshots").document("latest")

    def _signal_document(self) -> Any:
        return self._root_document().collection("signals").document("latest")

    def _execution_document(self) -> Any:
        return self._root_document().collection("execution").document("current")

    def _action_document(self) -> Any:
        return self._root_document().collection("actions").document("latest")

    def _logs_collection(self) -> Any:
        return self._root_document().collection("logs")

    def _root_document(self) -> Any:
        return self._client_or_default().collection(self._settings.firestore_runtime_collection).document(
            self._settings.firestore_product_document
        )

    def _client_or_default(self) -> FirestoreClientProtocol:
        if self._client is not None:
            return self._client
        try:
            from google.cloud import firestore
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-firestore is required for Prime Stocks runtime state persistence."
            ) from exc
        self._client = firestore.Client(
            project=self._settings.firestore_project_id,
            database=self._settings.firestore_database_id,
        )
        return self._client


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
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=settings.prime_stocks_execution_bar_limit,
        trend_bar_limit=settings.prime_stocks_trend_bar_limit,
        first_lot_notional=settings.prime_stocks_first_lot_notional,
        multi_notional=settings.prime_stocks_multi_notional,
    )


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
) -> dict[str, Any]:
    execution_key = f"{candidate_action}:{_isoformat_or_none(latest_signal_time) or 'none'}"
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
        "skipped_reason": skipped_reason if execution_result is None else execution_result.skipped_reason,
    }


def _normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip().lower()
    return "stock" if normalized in SUPPORTED_STOCK_ASSET_TYPES else normalized


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)
