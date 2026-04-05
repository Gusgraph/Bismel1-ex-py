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
    execution_timeframe: str
    trend_timeframe: str
    pullback_window: int
    execution_bar_limit: int
    trend_bar_limit: int
    runtime_target: str = "cloud_run"


@dataclass(frozen=True)
class PrimeStocksRuntimeStorePaths:
    config_document: str
    state_document: str
    snapshot_document: str
    signal_document: str
    logs_collection: str


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
            execution_timeframe=str(payload.get("execution_timeframe", default_config.execution_timeframe)),
            trend_timeframe=str(payload.get("trend_timeframe", default_config.trend_timeframe)),
            pullback_window=int(payload.get("pullback_window", default_config.pullback_window)),
            execution_bar_limit=int(payload.get("execution_bar_limit", default_config.execution_bar_limit)),
            trend_bar_limit=int(payload.get("trend_bar_limit", default_config.trend_bar_limit)),
            runtime_target=str(payload.get("runtime_target", default_config.runtime_target)),
        )

    def write_dry_run_result(
        self,
        *,
        run_id: str,
        runtime_config: PrimeStocksRuntimeConfigRecord,
        strategy_result: PrimeStocksStrategyResult,
        candidate_action: str,
        latest_signal_time: datetime | None,
        dry_run_message: str,
    ) -> None:
        now = datetime.now(tz=UTC)
        latest_snapshot = {
            "run_id": run_id,
            "product_key": runtime_config.product_key,
            "strategy_key": runtime_config.strategy_key,
            "symbol": runtime_config.symbol,
            "asset_type": runtime_config.asset_type,
            "status": strategy_result.status,
            "runtime_target": runtime_config.runtime_target,
            "dry_run": runtime_config.dry_run,
            "dry_run_message": dry_run_message,
            "candidate_action": candidate_action,
            "execution_timeframe": strategy_result.execution_timeframe,
            "trend_timeframe": strategy_result.trend_timeframe,
            "pullback_window": runtime_config.pullback_window,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "updated_at": now.isoformat(),
            "strategy_message": strategy_result.message,
            "state": _serialize_state(strategy_result.final_state),
            "signal": _serialize_signal(strategy_result),
        }
        latest_signal = {
            "run_id": run_id,
            "symbol": runtime_config.symbol,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "signal": _serialize_signal(strategy_result),
            "updated_at": now.isoformat(),
        }
        current_state = {
            "run_id": run_id,
            "enabled": runtime_config.enabled,
            "dry_run": runtime_config.dry_run,
            "last_processed_bar_time": _isoformat_or_none(latest_signal_time),
            "latest_candidate_action": candidate_action,
            "latest_status": strategy_result.status,
            "runtime_target": runtime_config.runtime_target,
            "updated_at": now.isoformat(),
        }
        log_payload = {
            "run_id": run_id,
            "level": "INFO",
            "kind": "dry_run_execution",
            "message": dry_run_message,
            "symbol": runtime_config.symbol,
            "status": strategy_result.status,
            "candidate_action": candidate_action,
            "latest_signal_time": _isoformat_or_none(latest_signal_time),
            "created_at": now.isoformat(),
        }
        self._snapshot_document().set(latest_snapshot, merge=True)
        self._signal_document().set(latest_signal, merge=True)
        self._state_document().set(current_state, merge=True)
        self._logs_collection().document(run_id).set(log_payload, merge=True)

    def create_run_id(self) -> str:
        return f"dryrun-{uuid4().hex[:15]}"

    def _config_document(self) -> Any:
        return self._root_document().collection("config").document("current")

    def _state_document(self) -> Any:
        return self._root_document().collection("state").document("current")

    def _snapshot_document(self) -> Any:
        return self._root_document().collection("snapshots").document("latest")

    def _signal_document(self) -> Any:
        return self._root_document().collection("signals").document("latest")

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
                "google-cloud-firestore is required for Prime Stocks dry-run state persistence."
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
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=settings.prime_stocks_execution_bar_limit,
        trend_bar_limit=settings.prime_stocks_trend_bar_limit,
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


def _normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip().lower()
    return "stock" if normalized in SUPPORTED_STOCK_ASSET_TYPES else normalized
