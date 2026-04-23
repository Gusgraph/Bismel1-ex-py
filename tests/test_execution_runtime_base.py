from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from app.brokers.alpaca_paper_trading import (
    AlpacaPaperAccountState,
    AlpacaPaperAssetState,
    AlpacaPaperExecutionResult,
    AlpacaPaperPositionState,
    AlpacaPaperSubmissionState,
)
from app.products.stocks.bismel1.models import PriceBar
from app.runtime.execution.execution_runtime_base import (
    ExecutionSchedulerTarget,
    ExecutionRuntimeConfig,
    ExecutionRuntimePaths,
    ExecutionRuntimeRequest,
    ExecutionRuntimeResult,
    ExecutionRuntimeService,
    ExecutionRuntimeStore,
    _normalize_symbol_assignments,
)
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext, RuntimeAccountTarget
from app.shared.config import get_settings


class FakeSnapshot:
    def __init__(self, payload):
        self._payload = payload

    @property
    def exists(self):
        return self._payload is not None

    def to_dict(self):
        return self._payload


class FakeDocumentReference:
    def __init__(self, storage: dict, path: list[str]) -> None:
        self._storage = storage
        self._path = path

    def collection(self, name: str):
        return FakeCollectionReference(self._storage, [*self._path, name])

    def get(self):
        return FakeSnapshot(_resolve_payload(self._storage, self._path))

    def set(self, payload, merge: bool = False):
        _write_payload(self._storage, self._path, payload, merge=merge)


class FakeCollectionReference:
    def __init__(self, storage: dict, path: list[str]) -> None:
        self._storage = storage
        self._path = path

    def document(self, name: str):
        return FakeDocumentReference(self._storage, [*self._path, name])


class ExecutionFakeFirestoreClient:
    def __init__(self) -> None:
        self.storage: dict = {}

    def collection(self, name: str):
        return FakeCollectionReference(self.storage, [name])


def _resolve_payload(storage: dict, path: list[str]):
    cursor = storage
    for part in path:
        if part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def _write_payload(storage: dict, path: list[str], payload, merge: bool) -> None:
    cursor = storage
    for part in path[:-1]:
        cursor = cursor.setdefault(part, {})
    existing = cursor.get(path[-1], {})
    if merge and isinstance(existing, dict):
        existing.update(payload)
        cursor[path[-1]] = existing
        return
    cursor[path[-1]] = payload


class FakeExecutionRuntimeStore:
    def __init__(self) -> None:
        self.last_result: ExecutionRuntimeResult | None = None
        self.last_config: ExecutionRuntimeConfig | None = None
        self.runtime_config_payload: dict[str, object] | None = None
        self.writes: list[ExecutionRuntimeResult] = []
        self.write_calls: list[dict[str, object]] = []
        self.performance_trade_payloads: dict[str, dict[str, object]] = {}
        self.updated_slot_configs: list[dict[str, object]] = []
        self.performance_docs_load_count = 0

    def load_runtime_config(self, runtime_request):
        payload = self.runtime_config_payload or {}
        selected_symbols = tuple(payload.get("selected_symbols", [])) if isinstance(payload.get("selected_symbols"), list) else ()
        symbol_assignments = _normalize_symbol_assignments(
            payload.get("symbol_assignments"),
            selected_symbols=selected_symbols,
            fallback_strategy_key=str(payload.get("strategy_key", "ema")),
            fallback_strategy_settings=payload.get("strategy_settings") if isinstance(payload.get("strategy_settings"), dict) else {},
            fallback_risk_settings=payload.get("risk_settings") if isinstance(payload.get("risk_settings"), dict) else {},
            fallback_auto_disable_settings={
                "auto_disable_enabled": payload.get("auto_disable_enabled", True),
                "auto_disable_min_trades": payload.get("auto_disable_min_trades", 5),
                "auto_disable_max_drawdown_percent": payload.get("auto_disable_max_drawdown_percent", 12.0),
                "auto_disable_min_win_rate": payload.get("auto_disable_min_win_rate", 35.0),
            },
        )
        if not symbol_assignments:
            symbol_assignments = None
        self.last_config = ExecutionRuntimeConfig(
            product_id="execution",
            enabled=bool(payload.get("enabled", True)),
            execution_mode=str(payload.get("execution_mode", "manual_signal" if runtime_request.symbol else "strategy_cycle")),
            uid=runtime_request.user_id,
            account_id=runtime_request.account_id,
            slot_number=runtime_request.slot,
            symbol=runtime_request.symbol or str(payload.get("symbol", "SLOT")),
            action=runtime_request.action or str(payload.get("action", "evaluate")),
            qty=runtime_request.qty,
            notional=runtime_request.notional,
            alpaca_account_id=runtime_request.alpaca_account_id,
            strategy_key=(str(payload.get("strategy_key")) if payload.get("strategy_key") is not None else None),
            strategy_settings=payload.get("strategy_settings") if isinstance(payload.get("strategy_settings"), dict) else None,
            risk_settings=payload.get("risk_settings") if isinstance(payload.get("risk_settings"), dict) else None,
            selected_symbols=tuple(symbol_assignments.keys()) if isinstance(symbol_assignments, dict) else selected_symbols,
            symbol_assignments=symbol_assignments,
            symbol_states=payload.get("symbol_states") if isinstance(payload.get("symbol_states"), dict) else None,
            automation_enabled=bool(payload.get("automation_enabled", False)),
            auto_disable_enabled=bool(payload.get("auto_disable_enabled", True)),
            auto_disable_min_trades=int(payload.get("auto_disable_min_trades", 5)),
            auto_disable_max_drawdown_percent=float(payload.get("auto_disable_max_drawdown_percent", 12.0)),
            auto_disable_min_win_rate=float(payload.get("auto_disable_min_win_rate", 35.0)),
            auto_disable_scope=str(payload.get("auto_disable_scope", "symbol_assignment")),
            global_guardrails_enabled=bool(payload.get("global_guardrails_enabled", True)),
            max_daily_loss_dollars=payload.get("max_daily_loss_dollars"),
            max_daily_loss_percent=float(payload.get("max_daily_loss_percent", 5.0)) if payload.get("max_daily_loss_percent", 5.0) is not None else None,
            max_daily_trades=int(payload.get("max_daily_trades", 10)) if payload.get("max_daily_trades", 10) is not None else None,
            max_open_positions_total=int(payload.get("max_open_positions_total", 5)) if payload.get("max_open_positions_total", 5) is not None else None,
            max_new_entries_per_run=int(payload.get("max_new_entries_per_run", 2)) if payload.get("max_new_entries_per_run", 2) is not None else None,
            emergency_kill_switch=bool(payload.get("emergency_kill_switch", False)),
        )
        return self.last_config

    def create_run_id(self) -> str:
        return "run-test"

    def get_paths(self, *, uid: str, account_id: int, slot_number: int, symbol: str) -> ExecutionRuntimePaths:
        root = f"users/{uid}/accounts/{account_id}/execution/current/slots/slot_{slot_number}"
        return ExecutionRuntimePaths(
            root=root,
            config_document=f"{root}/config/current",
            state_document=f"{root}/state/current",
            execution_document=f"{root}/execution/current",
            signal_document=f"{root}/signals/latest",
            action_document=f"{root}/actions/latest",
            heartbeat_document=f"{root}/heartbeat/current",
            logs_collection=f"{root}/logs",
            symbol_state_document=f"{root}/symbols/{symbol}/state/current",
        )

    def write_runtime_result(self, *, runtime_request, runtime_config, account_context, result, write_symbol_state=True) -> None:
        self.last_result = result
        self.writes.append(result)
        self.write_calls.append({
            "symbol": runtime_config.symbol,
            "execution_status": result.execution_status,
            "write_symbol_state": write_symbol_state,
        })

    def discover_scheduler_targets(self):
        return None

    def load_execution_trade_performance_documents(self, *, uid: str, account_id: int):
        self.performance_docs_load_count += 1
        return list(self.performance_trade_payloads.values())

    def write_execution_trade_performance_batch(self, *, uid: str, account_id: int, trade_payloads):
        self.performance_trade_payloads = {trade_id: dict(payload) for trade_id, payload in trade_payloads.items()}

    def load_execution_performance_summary(self, *, uid: str, account_id: int):
        return {}

    def write_execution_slot_config(self, *, uid: str, account_id: int, slot_number: int, config_payload):
        self.updated_slot_configs.append({
            "uid": uid,
            "account_id": account_id,
            "slot_number": slot_number,
            "config_payload": dict(config_payload),
        })
        self.runtime_config_payload = {
            **(self.runtime_config_payload or {}),
            **dict(config_payload),
        }


class FakeAccountResolver:
    def list_runtime_targets(self, *, product_id: str = "prime_stocks"):
        assert product_id == "execution"
        return [
            RuntimeAccountTarget(
                uid="user-a",
                account_id=101,
                alpaca_account_id=501,
                slot_number=1,
                environment="paper",
                account_label="Account 1",
                entitlement={"runtime_allowed": True},
                product_id="execution",
                runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
            )
        ]

    def resolve_runtime_account_for_slot(self, *, account_id: int, slot_number: int, product_id: str):
        assert account_id == 101
        assert slot_number == 1
        assert product_id == "execution"
        return ResolvedAlpacaAccountContext(
            uid="user-a",
            account_id=101,
            alpaca_account_id=501,
            broker_connection_id=301,
            broker_credential_id=401,
            environment="paper",
            data_feed="iex",
            access_mode="trade",
            trade_enabled=True,
            key_id="paper-key",
            secret="paper-secret",
            slot_number=1,
            product_id="execution",
            broker_name="alpaca",
            runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
            linkage_status="connected",
        )


class FakePaperTradingAdapter:
    def get_submission_state(self, *, symbol: str, credential_context):
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(buying_power=10000.0, open_positions_count=0, equity=10000.0, total_exposure=0.0),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=None,
        )

    def submit_market_order_qty(self, *, symbol: str, side: str, qty: float, client_order_id: str, action: str, credential_context):
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status="accepted",
            order_id="order-1",
            client_order_id=client_order_id,
            side=side,
            notional=None,
            raw_response={"id": "order-1", "status": "accepted"},
        )

    def submit_market_order_notional(self, *, symbol: str, side: str, notional: float, client_order_id: str, action: str, credential_context):
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status="accepted",
            order_id="order-2",
            client_order_id=client_order_id,
            side=side,
            notional=notional,
            raw_response={"id": "order-2", "status": "accepted"},
        )

    def close_position_symbol(self, *, symbol: str, action: str, client_order_id: str, credential_context):
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status="accepted",
            order_id="order-close",
            client_order_id=client_order_id,
            side="sell",
            notional=None,
            raw_response={"id": "order-close", "status": "accepted"},
        )

    def list_recent_orders(self, *, credential_context=None, limit: int = 50):
        return []

    def cancel_order(self, *, order_id: str, client_order_id: str | None, action: str, credential_context):
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status="canceled",
            order_id=order_id,
            client_order_id=client_order_id,
            side="buy",
            notional=None,
            raw_response={"cancel_status": "canceled", "id": order_id},
        )


class FakePaperTradingAdapterNoPosition(FakePaperTradingAdapter):
    def get_submission_state(self, *, symbol: str, credential_context):
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(buying_power=10000.0, open_positions_count=0, equity=10000.0, total_exposure=0.0),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=AlpacaPaperPositionState(symbol=symbol, qty=0.0, market_value=0.0),
        )


class FakePaperTradingAdapterWithPosition(FakePaperTradingAdapter):
    def get_submission_state(self, *, symbol: str, credential_context):
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(buying_power=10000.0, open_positions_count=1, equity=10000.0, total_exposure=100.0),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=AlpacaPaperPositionState(symbol=symbol, qty=3.0, market_value=300.0),
        )


class FakePaperTradingAdapterWithFilledHistory(FakePaperTradingAdapter):
    def __init__(self) -> None:
        self.recent_orders: list[dict[str, object]] = []
        self.has_position = False

    def get_submission_state(self, *, symbol: str, credential_context):
        position = AlpacaPaperPositionState(symbol=symbol, qty=1.0, market_value=105.0) if self.has_position else None
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(buying_power=10000.0, open_positions_count=1 if self.has_position else 0, equity=10000.0, total_exposure=105.0 if self.has_position else 0.0),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=position,
        )

    def list_recent_orders(self, *, credential_context=None, limit: int = 50):
        return list(self.recent_orders)


class FakePaperTradingAdapterAtMaxPositions(FakePaperTradingAdapter):
    def get_submission_state(self, *, symbol: str, credential_context):
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=10000.0,
                open_positions_count=5,
                equity=10000.0,
                total_exposure=200.0,
            ),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=None,
        )


class FakePaperTradingAdapterAtHighExposure(FakePaperTradingAdapter):
    def get_submission_state(self, *, symbol: str, credential_context):
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=10000.0,
                open_positions_count=1,
                equity=10000.0,
                total_exposure=1950.0,
            ),
            asset=AlpacaPaperAssetState(symbol=symbol, tradable=True, status="active"),
            position=None,
        )


class FakePaperTradingAdapterWithOpenOrder(FakePaperTradingAdapter):
    def list_recent_orders(self, *, credential_context=None, limit: int = 50):
        return [
            {
                "id": "order-open",
                "client_order_id": "client-open",
                "status": "new",
                "symbol": "AAPL",
                "side": "buy",
                "qty": "1",
                "notional": "100",
            }
        ]


class FakePaperTradingAdapterWithClosedOrder(FakePaperTradingAdapter):
    def list_recent_orders(self, *, credential_context=None, limit: int = 50):
        return [
            {
                "id": "order-closed",
                "client_order_id": "client-closed",
                "status": "filled",
                "symbol": "AAPL",
                "side": "buy",
                "qty": "1",
                "notional": "100",
            }
        ]


class FakeMarketDataAdapter:
    def __init__(self, bars_by_symbol):
        self.bars_by_symbol = bars_by_symbol
        self.fetch_counts: dict[tuple[str, str, int], int] = {}

    def fetch_stock_bars(self, *, symbol: str, timeframe: str, limit: int, credential_context):
        assert timeframe in {"15Min", "1Hour"}
        key = (symbol.upper(), timeframe, int(limit))
        self.fetch_counts[key] = self.fetch_counts.get(key, 0) + 1
        bars = self.bars_by_symbol.get(symbol.upper())
        if isinstance(bars, Exception):
            raise bars
        return list(bars or [])


class FakeFirestoreDocumentSnapshot:
    def __init__(self, doc_id: str, data: dict[str, object] | None, reference) -> None:
        self.id = doc_id
        self._data = data
        self.reference = reference
        self.exists = data is not None

    def to_dict(self):
        return self._data


class FakeFirestoreDocumentReference:
    def __init__(self, doc_id: str, store: dict[str, object]) -> None:
        self.id = doc_id
        self._store = store

    def collection(self, name: str):
        collections = self._store.setdefault("_collections", {})
        collection_store = collections.setdefault(name, {})
        return FakeFirestoreCollectionReference(name, collection_store)

    def get(self):
        data = self._store.get("_data")
        return FakeFirestoreDocumentSnapshot(self.id, data if isinstance(data, dict) else None, self)


class FakeFirestoreCollectionReference:
    def __init__(self, name: str, store: dict[str, object]) -> None:
        self.id = name
        self._store = store

    def document(self, doc_id: str):
        document_store = self._store.setdefault(doc_id, {})
        return FakeFirestoreDocumentReference(doc_id, document_store)

    def stream(self):
        snapshots = []
        for doc_id, document_store in self._store.items():
            snapshots.append(FakeFirestoreDocumentSnapshot(doc_id, document_store.get("_data"), FakeFirestoreDocumentReference(doc_id, document_store)))
        return snapshots


class FakeFirestoreClient:
    def __init__(self, users_store: dict[str, object]) -> None:
        self._collections = {"users": users_store}

    def collection(self, name: str):
        return FakeFirestoreCollectionReference(name, self._collections.setdefault(name, {}))


def _make_bars(closes: list[float]) -> list[PriceBar]:
    base = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    bars: list[PriceBar] = []
    for index, close in enumerate(closes):
        stamp = base + timedelta(minutes=15 * index)
        bars.append(
            PriceBar(
                starts_at=stamp,
                ends_at=stamp,
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1000.0,
            )
        )
    return bars


def _make_ohlcv_bars(rows: list[tuple[float, float, float, float, float | None]]) -> list[PriceBar]:
    base = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    bars: list[PriceBar] = []
    for index, (open_price, high, low, close, volume) in enumerate(rows):
        stamp = base + timedelta(minutes=15 * index)
        bars.append(
            PriceBar(
                starts_at=stamp,
                ends_at=stamp,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume if volume is not None else 1000.0,
            )
        )
    return bars


def test_execution_runtime_submits_market_buy_by_slot() -> None:
    store = FakeExecutionRuntimeStore()
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
            "symbol": "AAPL",
            "action": "buy",
            "qty": 1,
        }
    )

    assert result.ok is True
    assert result.product_id == "execution"
    assert result.execution_status == "buy_submitted"
    assert result.account_id == 101
    assert result.slot == 1
    assert result.symbol == "AAPL"
    assert result.action == "buy"
    assert result.order_id == "order-1"
    assert result.side == "buy"
    assert store.last_result is not None
    assert asdict(store.get_paths(uid="user-a", account_id=101, slot_number=1, symbol="AAPL")) == result.firestore_paths


def test_execution_runtime_buy_is_blocked_by_max_positions() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
            "max_positions": 5,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterAtMaxPositions(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_risk_max_positions"
    assert result.enforcement_reason == "max_positions"
    assert result.open_positions_count == 5


def test_execution_runtime_buy_is_blocked_by_max_exposure() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 10,
            "max_total_exposure_percent": 20.0,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterAtHighExposure(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_risk_max_exposure"
    assert result.enforcement_reason == "max_total_exposure_percent"
    assert result.projected_exposure_percent is not None
    assert result.projected_exposure_percent > 20.0


def test_execution_runtime_cancel_open_order_succeeds() -> None:
    store = FakeExecutionRuntimeStore()
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithOpenOrder(),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
            "action": "cancel",
            "order_id": "order-open",
        }
    )

    assert result.ok is True
    assert result.execution_status == "cancel_submitted"
    assert result.order_id == "order-open"
    assert result.cancel_status == "canceled"


def test_execution_runtime_cancel_non_open_order_skips_cleanly() -> None:
    store = FakeExecutionRuntimeStore()
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithClosedOrder(),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
            "action": "cancel",
            "order_id": "order-closed",
        }
    )

    assert result.ok is False
    assert result.execution_status == "skipped_order_not_open"


def test_execution_runtime_modify_open_order_performs_cancel_and_replace() -> None:
    store = FakeExecutionRuntimeStore()
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithOpenOrder(),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
            "action": "modify",
            "order_id": "order-open",
            "qty": 2,
        }
    )

    assert result.ok is True
    assert result.execution_status == "modify_submitted"
    assert result.cancel_status == "canceled"
    assert result.modify_status == "submitted"
    assert result.replacement_order_id == "order-1"


def test_execution_runtime_store_discovers_only_runnable_execution_slots() -> None:
    client = FakeFirestoreClient(
        {
            "user-a": {
                "_collections": {
                    "accounts": {
                        "101": {
                            "_collections": {
                                "execution": {
                                    "current": {
                                        "_collections": {
                                            "slots": {
                                                "slot_1": {
                                                    "_collections": {
                                                        "config": {
                                                            "current": {
                                                                "_data": {
                                                                    "automation_enabled": True,
                                                                    "symbol_assignments": {
                                                                        "AAPL": {"enabled": True, "strategy_key": "ema", "strategy_settings": {}, "risk_settings": {}}
                                                                    },
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "slot_2": {
                                                    "_collections": {
                                                        "config": {
                                                            "current": {
                                                                "_data": {
                                                                    "automation_enabled": False,
                                                                    "symbol_assignments": {
                                                                        "NVDA": {"enabled": True, "strategy_key": "ema", "strategy_settings": {}, "risk_settings": {}}
                                                                    },
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "slot_3": {
                                                    "_collections": {
                                                        "config": {
                                                            "current": {
                                                                "_data": {
                                                                    "automation_enabled": True,
                                                                    "symbol_assignments": {
                                                                        "TSLA": {"enabled": False, "strategy_key": "ema", "strategy_settings": {}, "risk_settings": {}}
                                                                    },
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    )

    store = ExecutionRuntimeStore(settings=get_settings(), client=client)
    discovery = store.discover_scheduler_targets()

    assert discovery.total_slots_seen == 3
    assert discovery.runnable_slots == 1
    assert discovery.skipped_disabled == 1
    assert discovery.skipped_no_symbols == 1
    assert discovery.skipped_invalid_config == 0
    assert discovery.targets == (
        ExecutionSchedulerTarget(
            uid="user-a",
            account_id=101,
            slot_number=1,
            product_id="execution",
            runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
        ),
    )


def test_execution_runtime_service_discovers_targets_via_runtime_bridge() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {"enabled": True, "strategy_key": "ema", "strategy_settings": {}, "risk_settings": {}}
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
    )

    discovery = service.discover_scheduler_targets()

    assert discovery.total_slots_seen == 1
    assert discovery.runnable_slots == 1
    assert discovery.skipped_disabled == 0
    assert discovery.skipped_no_symbols == 0
    assert discovery.skipped_invalid_config == 0
    assert discovery.targets == (
        ExecutionSchedulerTarget(
            uid="user-a",
            account_id=101,
            slot_number=1,
            product_id="execution",
            runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
        ),
    )


def test_execution_runtime_skips_sell_when_slot_has_no_position() -> None:
    store = FakeExecutionRuntimeStore()
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterNoPosition(),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
            "symbol": "AAPL",
            "action": "sell",
            "qty": 1,
        }
    )

    assert result.ok is False
    assert result.execution_status == "skipped_no_open_position"
    assert result.broker_error_code == "no_position_to_sell"
    assert "no open position" in (result.message or "").lower()


def test_execution_runtime_ema_cross_up_submits_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
            "direction_filter": "both",
            "cross_confirmation": False,
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 2,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
        }
    )

    assert result.ok is True
    assert result.execution_status == "buy_submitted"
    assert any(item.execution_status == "buy_submitted" and item.symbol == "AAPL" for item in store.writes)


def test_execution_runtime_ema_cross_down_submits_close() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
            "direction_filter": "both",
            "cross_confirmation": False,
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([8, 9, 10, 11, 12, 11, 9, 7])}),
    )

    result = service.run_once(
        {
            "user_id": "user-a",
            "account_id": 101,
            "slot": 1,
        }
    )

    assert result.ok is True
    assert result.execution_status == "close_submitted"
    assert any(item.execution_status == "close_submitted" and item.symbol == "AAPL" for item in store.writes)


def test_execution_runtime_ema_no_cross_writes_no_signal() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
            "direction_filter": "both",
            "cross_confirmation": False,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 11, 12, 13, 14, 15, 16])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "no_signal"
    assert any(item.execution_status == "no_signal" and item.symbol == "AAPL" for item in store.writes)
    assert {
        "symbol": "AAPL",
        "execution_status": "no_signal",
        "write_symbol_state": True,
    } in store.write_calls


def test_execution_runtime_ema_invalid_config_skips_cleanly() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 10,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 11, 12, 13, 14, 15, 16])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_invalid_config"


def test_execution_runtime_ema_no_symbols_skips_cleanly() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "selected_symbols": [],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_no_symbols"


def test_execution_runtime_ema_confirmation_waits_for_next_closed_bar() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
            "cross_confirmation": True,
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([12, 11, 10, 9, 8, 9, 12, 14])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is True
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_ema_direction_filter_blocks_disallowed_signal() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
            "direction_filter": "long_only",
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([8, 9, 10, 11, 12, 11, 9, 7])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_direction_filter"


def test_execution_runtime_uses_symbol_assignments_for_multiple_symbols() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {
                    "fast_ema_length": 3,
                    "slow_ema_length": 5,
                    "timeframe": "15m",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
            "NVDA": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {
                    "fast_ema_length": 3,
                    "slow_ema_length": 5,
                    "timeframe": "15m",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "NVDA": _make_bars([10, 11, 12, 13, 14, 15, 16, 17]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "NVDA" and item.execution_status == "no_signal" for item in store.writes)


def test_execution_runtime_pullback_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "pullback",
                "strategy_settings": {
                    "trend_ema_length": 3,
                    "pullback_percent": 5.0,
                    "confirmation_bars": 1,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 12, 14, 16, 15, 14, 15])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)


def test_execution_runtime_pullback_no_signal_when_no_recovery() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "pullback",
                "strategy_settings": {
                    "trend_ema_length": 3,
                    "pullback_percent": 5.0,
                    "confirmation_bars": 1,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 12, 14, 16, 16.1, 16.3, 16.4])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "no_signal"


def test_execution_runtime_pullback_trend_break_closes_position() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "pullback",
                "strategy_settings": {
                    "trend_ema_length": 3,
                    "pullback_percent": 2.0,
                    "confirmation_bars": 1,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([15, 16, 17, 18, 15, 12, 10])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "close_submitted"


def test_execution_runtime_mixed_ema_and_pullback_assignments() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {
                    "fast_ema_length": 3,
                    "slow_ema_length": 5,
                    "timeframe": "15m",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
            "NVDA": {
                "enabled": True,
                "strategy_key": "pullback",
                "strategy_settings": {
                    "trend_ema_length": 3,
                    "pullback_percent": 5.0,
                    "confirmation_bars": 1,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "NVDA": _make_bars([10, 12, 14, 16, 16.1, 16.3, 16.4]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "NVDA" and item.execution_status == "no_signal" for item in store.writes)


def test_execution_runtime_breakout_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "breakout",
                "strategy_settings": {
                    "lookback_bars": 5,
                    "breakout_buffer_percent": 0.2,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.2, 10.1, 10.25, 10.3, 10.6])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"


def test_execution_runtime_breakout_no_signal_without_break() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "breakout",
                "strategy_settings": {
                    "lookback_bars": 5,
                    "breakout_buffer_percent": 0.5,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.2, 10.1, 10.25, 10.3, 10.31])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "no_signal"


def test_execution_runtime_breakout_breakdown_closes_position() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "breakout",
                "strategy_settings": {
                    "lookback_bars": 5,
                    "breakout_buffer_percent": 0.1,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.2, 10.1, 10.25, 10.3, 9.7])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "close_submitted"


def test_execution_runtime_rsi_reversion_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "rsi_reversion",
                "strategy_settings": {
                    "rsi_length": 3,
                    "oversold_level": 35,
                    "overbought_level": 70,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"


def test_execution_runtime_rsi_reversion_no_signal_without_recovery() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "rsi_reversion",
                "strategy_settings": {
                    "rsi_length": 3,
                    "oversold_level": 30,
                    "overbought_level": 70,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9.8, 9.6, 9.4, 9.3, 9.2])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "no_signal"


def test_execution_runtime_rsi_reversion_overbought_closes_position() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "rsi_reversion",
                "strategy_settings": {
                    "rsi_length": 3,
                    "oversold_level": 30,
                    "overbought_level": 70,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.2, 10.4, 10.1, 10.2, 10.8])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "close_submitted"


def test_execution_runtime_momentum_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "momentum",
                "strategy_settings": {
                    "momentum_window": 3,
                    "momentum_threshold_percent": 5.0,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.1, 10.15, 10.2, 10.9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"


def test_execution_runtime_momentum_no_signal_when_below_threshold() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "momentum",
                "strategy_settings": {
                    "momentum_window": 3,
                    "momentum_threshold_percent": 5.0,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.1, 10.2, 10.25, 10.3])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "no_signal"


def test_execution_runtime_momentum_fade_closes_position() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "momentum",
                "strategy_settings": {
                    "momentum_window": 3,
                    "momentum_threshold_percent": 5.0,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 10.5, 11.0, 11.5, 10.6])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "close_submitted"


def test_execution_runtime_mixed_strategies_in_one_slot() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {
                    "fast_ema_length": 3,
                    "slow_ema_length": 5,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "NVDA": {
                "enabled": True,
                "strategy_key": "breakout",
                "strategy_settings": {
                    "lookback_bars": 5,
                    "breakout_buffer_percent": 0.2,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "MSFT": {
                "enabled": True,
                "strategy_key": "rsi_reversion",
                "strategy_settings": {
                    "rsi_length": 3,
                    "oversold_level": 35,
                    "overbought_level": 70,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "AMD": {
                "enabled": True,
                "strategy_key": "momentum",
                "strategy_settings": {
                    "momentum_window": 3,
                    "momentum_threshold_percent": 5.0,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "NVDA": _make_bars([10, 10.2, 10.1, 10.25, 10.3, 10.6]),
            "MSFT": _make_bars([10, 9, 8, 7, 8, 9]),
                "AMD": _make_bars([10, 10.1, 10.15, 10.2, 10.9]),
            }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "NVDA" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "MSFT" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "AMD" and item.execution_status == "buy_submitted" for item in store.writes)


def test_execution_runtime_vwap_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "vwap",
                "strategy_settings": {
                    "vwap_mode": "trend",
                    "deviation_percent": 0.5,
                    "timeframe": "15m",
                    "direction_filter": "long_only",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_ohlcv_bars([
                (10.0, 10.1, 9.9, 10.0, 1000),
                (10.0, 10.0, 9.8, 9.9, 1000),
                (9.9, 9.95, 9.75, 9.8, 1000),
                (9.8, 9.85, 9.65, 9.7, 1000),
                (9.7, 10.4, 9.65, 10.35, 2500),
            ]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_vwap_loss_closes_position() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "vwap",
                "strategy_settings": {"vwap_mode": "trend", "deviation_percent": 0.5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_ohlcv_bars([
                (10.0, 10.4, 9.9, 10.3, 1800),
                (10.3, 10.35, 10.2, 10.25, 1800),
                (10.25, 10.3, 10.1, 10.2, 1800),
                (10.2, 10.25, 9.7, 9.75, 2200),
            ]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "close_submitted"


def test_execution_runtime_bollinger_reversion_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "bollinger_reversion",
                "strategy_settings": {
                    "bollinger_length": 5,
                    "bollinger_stddev": 1.5,
                    "reentry_mode": "inside_band",
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10.0, 10.1, 10.2, 10.15, 9.2, 9.95])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_adx_trend_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "adx_trend",
                "strategy_settings": {
                    "adx_length": 3,
                    "adx_min_strength": 20,
                    "ema_length": 3,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_ohlcv_bars([
                (10.0, 10.3, 9.9, 10.2, 1000),
                (10.2, 10.6, 10.1, 10.5, 1000),
                (10.5, 10.9, 10.4, 10.8, 1000),
                (10.8, 11.2, 10.7, 11.1, 1000),
                (11.1, 11.5, 11.0, 11.4, 1000),
                (11.4, 11.8, 11.3, 11.7, 1000),
                (11.7, 12.0, 11.6, 11.95, 1000),
                (11.95, 12.25, 11.9, 12.2, 1000),
                (12.2, 12.5, 12.1, 12.45, 1000),
            ]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_donchian_breakout_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "donchian_breakout",
                "strategy_settings": {
                    "channel_length": 5,
                    "breakout_buffer_percent": 0.1,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10.0, 10.1, 10.05, 10.2, 10.15, 10.5])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_relative_strength_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "relative_strength",
                "strategy_settings": {
                    "benchmark_symbol": "SPY",
                    "strength_window": 4,
                    "min_relative_outperformance_percent": 1.0,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10.0, 10.2, 10.4, 10.6, 10.9, 11.2]),
            "SPY": _make_bars([10.0, 10.05, 10.1, 10.12, 10.15, 10.18]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_relative_strength_missing_benchmark_skips_cleanly() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "relative_strength",
                "strategy_settings": {"benchmark_symbol": "SPY", "strength_window": 4, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10.0, 10.1, 10.2, 10.3, 10.4, 10.5])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "skipped_market_data_unavailable"


def test_execution_runtime_fetches_buffered_history_for_indicator_strategies() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {
                    "fast_ema_length": 9,
                    "slow_ema_length": 21,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    market_data = FakeMarketDataAdapter({"AAPL": _make_bars([10.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7])})
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=market_data,
    )

    service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert market_data.fetch_counts[("AAPL", "15Min", 160)] == 1


def test_execution_runtime_slot_cycle_reuses_benchmark_and_performance_reads() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "relative_strength",
                "strategy_settings": {
                    "benchmark_symbol": "SPY",
                    "strength_window": 4,
                    "min_relative_outperformance_percent": 1.0,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "NVDA": {
                "enabled": True,
                "strategy_key": "relative_strength",
                "strategy_settings": {
                    "benchmark_symbol": "SPY",
                    "strength_window": 4,
                    "min_relative_outperformance_percent": 1.0,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    market_data = FakeMarketDataAdapter({
        "AAPL": _make_bars([10.0, 10.2, 10.4, 10.6, 10.9, 11.2]),
        "NVDA": _make_bars([20.0, 20.3, 20.6, 20.9, 21.3, 21.7]),
        "SPY": _make_bars([10.0, 10.05, 10.1, 10.12, 10.15, 10.18]),
    })
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=market_data,
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert store.performance_docs_load_count == 1
    assert market_data.fetch_counts[("SPY", "15Min", 6)] == 1
    assert market_data.fetch_counts[("AAPL", "15Min", 6)] == 1
    assert market_data.fetch_counts[("NVDA", "15Min", 6)] == 1


def test_execution_runtime_slot_cycle_cache_resets_between_runs() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "relative_strength",
                "strategy_settings": {
                    "benchmark_symbol": "SPY",
                    "strength_window": 4,
                    "min_relative_outperformance_percent": 1.0,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    market_data = FakeMarketDataAdapter({
        "AAPL": _make_bars([10.0, 10.2, 10.4, 10.6, 10.9, 11.2]),
        "SPY": _make_bars([10.0, 10.05, 10.1, 10.12, 10.15, 10.18]),
    })
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=market_data,
    )

    first = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    second = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert first.execution_status == "buy_submitted"
    assert second.execution_status == "buy_submitted"
    assert store.performance_docs_load_count == 2
    assert market_data.fetch_counts[("AAPL", "15Min", 6)] == 2
    assert market_data.fetch_counts[("SPY", "15Min", 6)] == 2


def test_execution_runtime_slot_cycle_continues_after_one_symbol_market_data_failure() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "NVDA": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "NVDA": RuntimeError("bars unavailable"),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(
        item.symbol == "NVDA" and item.execution_status == "skipped_market_data_unavailable"
        for item in store.writes
    )


def test_execution_runtime_opening_range_breakout_generates_buy() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "opening_range_breakout",
                "strategy_settings": {
                    "opening_range_minutes": 30,
                    "breakout_buffer_percent": 0.1,
                    "timeframe": "15m",
                },
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_ohlcv_bars([
                (10.0, 10.2, 9.9, 10.1, 1000),
                (10.1, 10.3, 10.0, 10.2, 1000),
                (10.2, 10.35, 10.15, 10.25, 1500),
                (10.25, 10.8, 10.2, 10.75, 1500),
            ]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})
    assert result.execution_status == "buy_submitted"


def test_execution_runtime_unknown_strategy_key_skips_cleanly() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "mystery",
                "strategy_settings": {},
                "risk_settings": {
                    "position_size_mode": "qty",
                    "default_qty": 1,
                },
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.ok is False
    assert result.execution_status == "skipped_strategy_not_implemented"
    assert any(item.symbol == "AAPL" and item.execution_status == "skipped_strategy_not_implemented" for item in store.writes)


def test_execution_runtime_old_slot_wide_shape_is_compatible() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "automation_enabled": True,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
        },
        "selected_symbols": ["AAPL"],
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"


def test_execution_runtime_load_config_clears_symbols_for_different_linked_account() -> None:
    client = FakeFirestoreClient(users_store={})
    store = ExecutionRuntimeStore(settings=get_settings(), client=client)
    config_document = (
        client.collection("users")
        .document("user-a")
        .collection("accounts")
        .document("101")
        .collection("execution")
        .document("current")
        .collection("slots")
        .document("slot_1")
        .collection("config")
        .document("current")
    )
    config_document._store["_data"] = {
        "enabled": True,
        "automation_enabled": True,
        "alpaca_account_id": 501,
        "strategy_key": "ema",
        "strategy_settings": {
            "fast_ema_length": 3,
            "slow_ema_length": 5,
            "timeframe": "15m",
        },
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
        },
        "selected_symbols": ["AAPL"],
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }

    config = store.load_runtime_config(
        ExecutionRuntimeRequest(
            user_id="user-a",
            account_id=101,
            slot=1,
            alpaca_account_id=999,
        )
    )

    assert config.selected_symbols == ()
    assert config.symbol_assignments == {}


def test_execution_runtime_manual_buy_and_close_update_performance_trade_state() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "enabled": True,
        "strategy_key": "ema",
        "risk_settings": {
            "position_size_mode": "qty",
            "default_qty": 1,
        },
    }
    paper_trading = FakePaperTradingAdapterWithFilledHistory()
    paper_trading.recent_orders = [{
        "id": "order-1",
        "status": "filled",
        "symbol": "AAPL",
        "filled_avg_price": "100.0",
        "filled_qty": "1",
        "filled_at": "2026-04-20T10:00:00+00:00",
    }]
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=paper_trading,
        market_data=FakeMarketDataAdapter({}),
    )

    buy_result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "symbol": "AAPL", "action": "buy", "qty": 1})

    assert buy_result.execution_status == "buy_submitted"
    assert len(store.performance_trade_payloads) == 1
    trade_payload = next(iter(store.performance_trade_payloads.values()))
    assert trade_payload["trade_state"] == "open"
    assert trade_payload["entry_price"] == 100.0
    assert trade_payload["strategy_key"] == "ema"

    paper_trading.has_position = True
    paper_trading.recent_orders = [
        {
            "id": "order-1",
            "status": "filled",
            "symbol": "AAPL",
            "filled_avg_price": "100.0",
            "filled_qty": "1",
            "filled_at": "2026-04-20T10:00:00+00:00",
        },
        {
            "id": "order-close",
            "status": "filled",
            "symbol": "AAPL",
            "filled_avg_price": "105.0",
            "filled_qty": "1",
            "filled_at": "2026-04-20T11:00:00+00:00",
        },
    ]

    close_result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "symbol": "AAPL", "action": "close"})

    assert close_result.execution_status == "close_submitted"
    trade_payload = next(iter(store.performance_trade_payloads.values()))
    assert trade_payload["trade_state"] == "closed"
    assert trade_payload["realized_pnl_dollars"] == 5.0
    assert trade_payload["trade_outcome"] == "win"


def test_execution_runtime_store_builds_slot_symbol_and_strategy_performance_summary() -> None:
    client = ExecutionFakeFirestoreClient()
    store = ExecutionRuntimeStore(settings=get_settings(), client=client)

    store.write_execution_trade_performance_batch(
        uid="user-a",
        account_id=101,
        trade_payloads={
            "trade-1": {
                "trade_id": "trade-1",
                "slot_number": 1,
                "symbol": "AAPL",
                "strategy_key": "ema",
                "trade_state": "closed",
                "entry_price": 100.0,
                "exit_price": 105.0,
                "qty": 1.0,
                "entry_notional": 100.0,
                "exit_notional": 105.0,
                "realized_pnl_dollars": 5.0,
                "realized_pnl_percent": 5.0,
                "trade_outcome": "win",
                "exit_filled_at": "2026-04-20T11:00:00+00:00",
            },
            "trade-2": {
                "trade_id": "trade-2",
                "slot_number": 1,
                "symbol": "NVDA",
                "strategy_key": "momentum",
                "trade_state": "closed",
                "entry_price": 200.0,
                "exit_price": 190.0,
                "qty": 1.0,
                "entry_notional": 200.0,
                "exit_notional": 190.0,
                "realized_pnl_dollars": -10.0,
                "realized_pnl_percent": -5.0,
                "trade_outcome": "loss",
                "exit_filled_at": "2026-04-20T12:00:00+00:00",
            },
            "trade-3": {
                "trade_id": "trade-3",
                "slot_number": 2,
                "symbol": "AAPL",
                "strategy_key": "breakout",
                "trade_state": "closed",
                "entry_price": 50.0,
                "exit_price": 55.0,
                "qty": 2.0,
                "entry_notional": 100.0,
                "exit_notional": 110.0,
                "realized_pnl_dollars": 10.0,
                "realized_pnl_percent": 10.0,
                "trade_outcome": "win",
                "exit_filled_at": "2026-04-20T13:00:00+00:00",
            },
        },
    )

    summary_doc = client.storage["users"]["user-a"]["accounts"]["101"]["performance"]["execution"]["summary"]["current"]

    assert summary_doc["total_trades"] == 3
    assert summary_doc["wins"] == 2
    assert summary_doc["losses"] == 1
    assert summary_doc["slot_summaries"]["1"]["total_trades"] == 2
    assert summary_doc["slot_summaries"]["1"]["realized_pnl"] == -5.0
    assert any(item["slot_number"] == 1 and item["symbol"] == "AAPL" and item["realized_pnl"] == 5.0 for item in summary_doc["symbol_summaries"])
    assert any(item["slot_number"] == 1 and item["strategy_key"] == "momentum" and item["realized_pnl"] == -10.0 for item in summary_doc["strategy_summaries"])


def test_execution_runtime_manually_disabled_assignment_skips() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": False,
        "symbol_assignments": {
            "AAPL": {
                "enabled": False,
                "manually_disabled": True,
                "disabled_source": "manual",
                "disabled_reason": "manually_disabled",
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_manually_disabled"
    assert any(item.symbol == "AAPL" and item.execution_status == "skipped_manually_disabled" for item in store.writes)
    assert {
        "symbol": "AAPL",
        "execution_status": "skipped_manually_disabled",
        "write_symbol_state": True,
    } in store.write_calls


def test_execution_runtime_auto_disabled_assignment_skips() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": False,
        "symbol_assignments": {
            "AAPL": {
                "enabled": False,
                "manually_disabled": False,
                "auto_disabled": True,
                "disabled_source": "auto",
                "disabled_reason": "auto_disabled_drawdown",
                "auto_disabled_reason": "auto_disabled_drawdown",
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_auto_disabled"
    assert any(item.symbol == "AAPL" and item.execution_status == "skipped_auto_disabled" for item in store.writes)
    assert {
        "symbol": "AAPL",
        "execution_status": "skipped_auto_disabled",
        "write_symbol_state": True,
    } in store.write_calls


def test_execution_runtime_auto_disable_triggers_after_thresholds_breached() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "auto_disable_enabled": True,
        "auto_disable_min_trades": 2,
        "auto_disable_min_win_rate": 35.0,
        "auto_disable_max_drawdown_percent": 12.0,
        "auto_disable_scope": "symbol_assignment",
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    store.performance_trade_payloads = {
        "trade-1": {
            "trade_id": "trade-1",
            "slot_number": 1,
            "symbol": "AAPL",
            "strategy_key": "ema",
            "trade_state": "closed",
            "realized_pnl_dollars": -10.0,
            "realized_pnl_percent": -10.0,
            "updated_at": "2026-04-20T10:00:00+00:00",
        },
        "trade-2": {
            "trade_id": "trade-2",
            "slot_number": 1,
            "symbol": "AAPL",
            "strategy_key": "ema",
            "trade_state": "closed",
            "realized_pnl_dollars": -8.0,
            "realized_pnl_percent": -8.0,
            "updated_at": "2026-04-20T11:00:00+00:00",
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_auto_disabled"
    assert result.enforcement_reason == "auto_disabled_win_rate"
    assert store.updated_slot_configs
    updated_assignment = store.updated_slot_configs[-1]["config_payload"]["symbol_assignments"]["AAPL"]
    assert updated_assignment["enabled"] is False
    assert updated_assignment["auto_disabled"] is True
    assert updated_assignment["disabled_source"] == "auto"
    assert updated_assignment["disabled_reason"] == "auto_disabled_win_rate"


def test_execution_runtime_auto_disable_off_does_not_disable_assignment() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": False,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "auto_disable_enabled": False,
                "auto_disable_min_trades": 2,
                "auto_disable_min_win_rate": 50.0,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    store.performance_trade_payloads = {
        "trade-1": {"trade_id": "trade-1", "slot_number": 1, "symbol": "AAPL", "strategy_key": "ema", "trade_state": "closed", "realized_pnl_dollars": -10.0, "realized_pnl_percent": -10.0, "updated_at": "2026-04-20T10:00:00+00:00"},
        "trade-2": {"trade_id": "trade-2", "slot_number": 1, "symbol": "AAPL", "strategy_key": "ema", "trade_state": "closed", "realized_pnl_dollars": -8.0, "realized_pnl_percent": -8.0, "updated_at": "2026-04-20T11:00:00+00:00"},
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert store.updated_slot_configs == []


def test_execution_runtime_manual_reenable_allows_runtime_again() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": False,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "manually_disabled": False,
                "auto_disabled": False,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)


def test_execution_runtime_risk_caps_off_skips_cap_checks() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": False,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "risk_caps_enabled": False,
                "max_positions": 1,
                "max_total_exposure_percent": 5.0,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1, "max_positions": 1, "max_total_exposure_percent": 5.0},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterAtMaxPositions(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"


def test_execution_runtime_kill_switch_blocks_new_entries() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "emergency_kill_switch": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_guardrail_kill_switch"
    assert result.guardrail_reason == "kill_switch"


def test_execution_runtime_daily_loss_guardrail_blocks_new_entries() -> None:
    now = datetime(2026, 4, 20, 12, 0, tzinfo=UTC)
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "max_daily_loss_dollars": 50.0,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    store.performance_trade_payloads = {
        "trade-1": {
            "trade_id": "trade-1",
            "slot_number": 1,
            "symbol": "AAPL",
            "strategy_key": "ema",
            "trade_state": "closed",
            "realized_pnl_dollars": -60.0,
            "realized_pnl_percent": -6.0,
            "exit_filled_at": now.isoformat(),
            "entry_filled_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
        now_provider=lambda: now,
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_guardrail_daily_loss"
    assert result.guardrail_reason == "daily_loss"


def test_execution_runtime_daily_trades_guardrail_blocks_new_entries() -> None:
    now = datetime(2026, 4, 20, 12, 0, tzinfo=UTC)
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "max_daily_trades": 1,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    store.performance_trade_payloads = {
        "trade-1": {
            "trade_id": "trade-1",
            "slot_number": 1,
            "symbol": "AAPL",
            "strategy_key": "ema",
            "trade_state": "open",
            "entry_filled_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
        now_provider=lambda: now,
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_guardrail_daily_trades"
    assert result.guardrail_reason == "daily_trades"


def test_execution_runtime_open_positions_guardrail_blocks_new_entries() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "max_open_positions_total": 5,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterAtMaxPositions(),
        market_data=FakeMarketDataAdapter({"AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9])}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "skipped_guardrail_open_positions"
    assert result.guardrail_reason == "open_positions"


def test_execution_runtime_run_entry_limit_blocks_additional_entries() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "max_new_entries_per_run": 1,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "MSFT": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "MSFT": _make_bars([20, 19, 18, 17, 16, 17, 18, 19]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "buy_submitted" for item in store.writes)
    assert any(item.symbol == "MSFT" and item.execution_status == "skipped_guardrail_run_entry_limit" for item in store.writes)


def test_execution_runtime_close_still_allowed_when_kill_switch_is_active() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "global_guardrails_enabled": True,
        "emergency_kill_switch": True,
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithPosition(),
        market_data=FakeMarketDataAdapter({}),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "symbol": "AAPL", "action": "close"})

    assert result.execution_status == "close_submitted"


def test_execution_runtime_mixed_enabled_and_disabled_assignments_are_isolated() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": False,
                "manually_disabled": True,
                "disabled_source": "manual",
                "disabled_reason": "manually_disabled",
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
            "MSFT": {
                "enabled": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapter(),
        market_data=FakeMarketDataAdapter({
            "AAPL": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
            "MSFT": _make_bars([10, 9, 8, 7, 6, 7, 8, 9]),
        }),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1})

    assert result.execution_status == "buy_submitted"
    assert any(item.symbol == "AAPL" and item.execution_status == "skipped_manually_disabled" for item in store.writes)
    assert any(item.symbol == "MSFT" and item.execution_status == "buy_submitted" for item in store.writes)


def test_execution_runtime_order_management_disabled_blocks_cancel() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "order_management_enabled": False,
                "allow_cancel": True,
                "allow_modify": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithOpenOrder(),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "action": "cancel", "order_id": "order-open"})

    assert result.execution_status == "skipped_order_management_disabled"


def test_execution_runtime_allow_cancel_false_blocks_cancel() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "order_management_enabled": True,
                "allow_cancel": False,
                "allow_modify": True,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithOpenOrder(),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "action": "cancel", "order_id": "order-open"})

    assert result.execution_status == "skipped_cancel_disabled"


def test_execution_runtime_allow_modify_false_blocks_modify() -> None:
    store = FakeExecutionRuntimeStore()
    store.runtime_config_payload = {
        "automation_enabled": True,
        "symbol_assignments": {
            "AAPL": {
                "enabled": True,
                "order_management_enabled": True,
                "allow_cancel": True,
                "allow_modify": False,
                "strategy_key": "ema",
                "strategy_settings": {"fast_ema_length": 3, "slow_ema_length": 5, "timeframe": "15m"},
                "risk_settings": {"position_size_mode": "qty", "default_qty": 1},
            },
        },
    }
    service = ExecutionRuntimeService(
        settings=get_settings(),
        runtime_store=store,
        account_resolver=FakeAccountResolver(),
        paper_trading=FakePaperTradingAdapterWithOpenOrder(),
    )

    result = service.run_once({"user_id": "user-a", "account_id": 101, "slot": 1, "action": "modify", "order_id": "order-open", "qty": 2})

    assert result.execution_status == "skipped_modify_disabled"


def test_execution_runtime_store_writes_symbol_runtime_health_fields() -> None:
    client = ExecutionFakeFirestoreClient()
    store = ExecutionRuntimeStore(settings=get_settings(), client=client)
    context = ResolvedAlpacaAccountContext(
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        broker_connection_id=301,
        broker_credential_id=401,
        environment="paper",
        data_feed="iex",
        access_mode="trade",
        trade_enabled=True,
        key_id="paper-key",
        secret="paper-secret",
        slot_number=1,
        product_id="execution",
        broker_name="alpaca",
        runtime_path="users/user-a/accounts/101/execution/current/slots/slot_1",
        linkage_status="connected",
    )
    config = ExecutionRuntimeConfig(
        product_id="execution",
        enabled=True,
        execution_mode="strategy_cycle",
        uid="user-a",
        account_id=101,
        slot_number=1,
        symbol="AAPL",
        action="evaluate",
        alpaca_account_id=501,
        strategy_key="ema",
        strategy_settings={"timeframe": "15m"},
        risk_settings={"position_size_mode": "qty", "default_qty": 1},
        automation_enabled=True,
    )
    request = ExecutionRuntimeRequest(
        user_id="user-a",
        account_id=101,
        slot=1,
        symbol="AAPL",
        action="evaluate",
        alpaca_account_id=501,
        broker_environment="paper",
    )
    result = ExecutionRuntimeResult(
        ok=False,
        run_id="run-live",
        product_id="execution",
        uid="user-a",
        account_id=101,
        slot=1,
        symbol="AAPL",
        action="evaluate",
        execution_status="no_signal",
        message="Execution runtime evaluated AAPL with no signal.",
        firestore_paths=asdict(store.get_paths(uid="user-a", account_id=101, slot_number=1, symbol="AAPL")),
        broker_environment="paper",
        alpaca_account_id=501,
        last_runtime_decision_at="2026-04-20T14:50:00+00:00",
        last_processed_bar_at="2026-04-20T14:45:00+00:00",
        raw_response={"evaluation": {"latest_bar_ended_at": "2026-04-20T14:45:00+00:00"}},
    )

    store.write_runtime_result(
        runtime_request=request,
        runtime_config=config,
        account_context=context,
        result=result,
    )

    root = client.storage["users"]["user-a"]["accounts"]["101"]["execution"]["current"]["slots"]["slot_1"]
    symbol_state = root["symbols"]["AAPL"]["state"]["current"]
    state_current = root["state"]["current"]
    execution_current = root["execution"]["current"]
    heartbeat = root["heartbeat"]["current"]

    assert symbol_state["included_in_latest_cycle"] is True
    assert symbol_state["included_in_last_cycle"] is True
    assert symbol_state["last_checked_at"] == "2026-04-20T14:50:00+00:00"
    assert symbol_state["last_processed_bar_time"] == "2026-04-20T14:45:00+00:00"
    assert symbol_state["last_processed_bar_at"] == "2026-04-20T14:45:00+00:00"
    assert symbol_state["runtime_message"] == "Execution runtime evaluated AAPL with no signal."
    assert symbol_state["latest_result"] == "no_signal"
    assert symbol_state["latest_action"] == "evaluate"
    assert symbol_state["latest_strategy_key"] == "ema"
    assert state_current["included_in_latest_cycle"] is True
    assert execution_current["last_checked_at"] == "2026-04-20T14:50:00+00:00"
    assert heartbeat["last_processed_bar_time"] == "2026-04-20T14:45:00+00:00"
