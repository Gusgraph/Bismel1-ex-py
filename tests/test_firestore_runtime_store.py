from unittest.mock import MagicMock, patch

import pytest

from app.services.firestore_runtime_store import (
    _build_strategy_reasoning_payload,
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksLatestExecutionRecord,
    PrimeStocksRuntimeConfigRecord,
    PrimeStocksRuntimeStateRecord,
    PrimeStocksRuntimeStoreError,
    build_default_runtime_config,
)
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    PrimeStocksStrategyResult,
)
from app.shared.config import AppConfig


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


class FakeFirestoreClient:
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





@pytest.fixture
def mock_app_config():
    return AppConfig(
        app_name="test-app",
        environment="test",
        host="0.0.0.0",
        port=8080,
        cloud_run_target=True,
        pine_source_filename="pine.pine",
        firestore_project_id="test-project",
        firestore_database_id="test-database",
        firestore_runtime_collection="runtime_products",
        firestore_product_document="prime_stocks",
        laravel_runtime_bridge_url="https://bismel1.test",
        laravel_runtime_bridge_token="bridge-token",
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_live_trading_base_url="https://api.alpaca.markets",
        alpaca_api_key_id="key_id",
        alpaca_api_secret="secret",
        alpaca_data_feed="iex",
        gemini_model="gemini-2.5-flash-lite",
        ai_cache_max_age_minutes=360,
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_ai_validation_bypass_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_test_mode=False,
        prime_stocks_test_trigger=None,
        prime_stocks_test_symbol_override=None,
        prime_stocks_execution_bar_limit=10,
        prime_stocks_trend_bar_limit=10,
        prime_stocks_first_lot_notional=100.0,
        prime_stocks_multi_notional=50.0,
        prime_stocks_max_notional_per_order=303.0,
        prime_stocks_max_total_notional_per_symbol=707.0,
        prime_stocks_max_add_count=2,
        prime_stocks_daily_order_cap=None,
        prime_stocks_max_open_positions=None,
        prime_stocks_broker_retry_max_attempts=1,
        prime_stocks_force_candidate_action=None,
        prime_stocks_scheduler_job_name="scheduler-job",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="* * * * *",
        prime_stocks_scheduler_timezone="UTC",
        prime_stocks_scheduler_header_name="X-Scheduler-Header",
        prime_stocks_scheduler_header_value="secret-value",
        prime_stocks_ping_scheduler_job_name="prime-stocks-ping",
        prime_stocks_ping_scheduler_schedule="*/1 * * * *",
        prime_stocks_ping_scheduler_timezone="UTC",
        prime_stocks_ping_scheduler_header_value="ping-secret-value",
    )


def test_load_runtime_config_document_not_found(mock_app_config):
    default_config = PrimeStocksRuntimeConfigRecord(
        product_key="stocks.bismel1",
        strategy_key="prime_stocks",
        strategy_title="Prime Stocks Bot Trader",
        symbol="GOOG",
        asset_type="stock",
        enabled=True,
        dry_run=False,
        paper_execution_enabled=True,
        live_execution_enabled=False,
        ping_enabled=False,
        ping_mode="off",
        ping_daily_heartbeat_enabled=False,
        test_mode=False,
        test_trigger=None,
        test_symbol_override=None,
        force_candidate_action=None,
        ai_validation_bypass_enabled=False,
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=351,
        trend_bar_limit=221,
        first_lot_notional=101.0,
        multi_notional=73.0,
        max_notional_per_order=303.0,
        max_total_notional_per_symbol=707.0,
        max_add_count=2,
        daily_order_cap=None,
        max_open_positions=None,
        broker_retry_max_attempts=1,
        account_id=None,
        alpaca_account_id=None,
        runtime_target="cloud_run",
        entitlement={},
    )

    with patch.object(
        PrimeStocksFirestoreRuntimeStore, "load_runtime_config", return_value=default_config
    ) as mock_load_runtime_config:
        store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config)
        config = store.load_runtime_config(default_config)

        mock_load_runtime_config.assert_called_once_with(default_config)
        assert config == default_config


def test_build_default_runtime_config_defaults_to_scalper(mock_app_config):
    config = build_default_runtime_config(mock_app_config)
    assert config.strategy_mode == "scalper"


def test_load_runtime_config_upgrades_tiny_persisted_bar_limits(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)
    default_config = build_default_runtime_config(mock_app_config)

    fake_client.storage["runtime_products"] = {
        "prime_stocks": {
            "config": {
                "current": {
                    "product_key": "stocks.bismel1",
                    "strategy_key": "prime_stocks",
                    "strategy_title": "Prime Stocks Bot Trader",
                    "symbol": "AAPL",
                    "asset_type": "stock",
                    "enabled": True,
                    "dry_run": True,
                    "paper_execution_enabled": True,
                    "live_execution_enabled": False,
                    "execution_timeframe": "15M",
                    "trend_timeframe": "1D",
                    "execution_bar_limit": 19,
                    "trend_bar_limit": 11,
                }
            }
        }
    }

    config = store.load_runtime_config(default_config)

    assert config.execution_bar_limit == 160
    assert config.trend_bar_limit == 120


def test_load_runtime_config_clears_symbols_for_different_linked_account(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)
    default_config = build_default_runtime_config(mock_app_config)
    scoped_config = PrimeStocksRuntimeConfigRecord(
        **{
            **default_config.__dict__,
            "uid": "user-a",
            "account_id": 101,
            "alpaca_account_id": 999,
            "slot_number": 1,
        }
    )

    fake_client.storage["users"] = {
        "user-a": {
            "accounts": {
                "101": {
                    "prime_stocks": {
                        "current": {
                            "slots": {
                                "slot_1": {
                                    "config": {
                                        "current": {
                                            **default_config.__dict__,
                                            "uid": "user-a",
                                            "account_id": 101,
                                            "alpaca_account_id": 501,
                                            "slot_number": 1,
                                            "selected_symbols": ["AAPL"],
                                            "symbol_states": [{"symbol": "AAPL", "mode": "active"}],
                                            "updated_at": "2026-04-20T12:00:00+00:00",
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

    config = store.load_runtime_config(scoped_config)

    assert config.product_key == default_config.product_key
    assert config.strategy_title == default_config.strategy_title
    assert config.selected_symbols == []
    assert config.symbol_states == []


def test_migrate_account_scoped_runtime_to_slot_skips_different_linked_account(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)
    default_config = build_default_runtime_config(mock_app_config)

    fake_client.storage["users"] = {
        "user-a": {
            "accounts": {
                "101": {
                    "prime_stocks": {
                        "current": {
                            "config": {
                                "current": {
                                    **default_config.__dict__,
                                    "uid": "user-a",
                                    "account_id": 101,
                                    "alpaca_account_id": 501,
                                    "selected_symbols": ["AAPL"],
                                    "symbol_states": [{"symbol": "AAPL", "mode": "active"}],
                                    "updated_at": "2026-04-20T12:00:00+00:00",
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    store.migrate_account_scoped_runtime_to_slot(
        uid="user-a",
        account_id=101,
        slot_number=1,
        expected_alpaca_account_id=999,
    )

    assert _resolve_payload(
        fake_client.storage,
        ["users", "user-a", "accounts", "101", "prime_stocks", "current", "slots", "slot_1", "config", "current"],
    ) is None


def test_migrate_account_scoped_runtime_to_slot_does_not_promote_config_symbols(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)
    default_config = build_default_runtime_config(mock_app_config)

    fake_client.storage["users"] = {
        "user-a": {
            "accounts": {
                "101": {
                    "prime_stocks": {
                        "current": {
                            "config": {
                                "current": {
                                    **default_config.__dict__,
                                    "uid": "user-a",
                                    "account_id": 101,
                                    "alpaca_account_id": 501,
                                    "selected_symbols": ["AAPL", "NVDA"],
                                    "symbol_states": [
                                        {"symbol": "AAPL", "mode": "active"},
                                        {"symbol": "NVDA", "mode": "active"},
                                    ],
                                    "updated_at": "2026-04-20T12:00:00+00:00",
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    store.migrate_account_scoped_runtime_to_slot(
        uid="user-a",
        account_id=101,
        slot_number=1,
        expected_alpaca_account_id=501,
    )

    assert _resolve_payload(
        fake_client.storage,
        ["users", "user-a", "accounts", "101", "prime_stocks", "current", "slots", "slot_1", "config", "current"],
    ) is None


def test_write_runtime_cycle_summary_replaces_stale_symbol_rows(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)

    fake_client.storage["users"] = {
        "user-a": {
            "accounts": {
                "101": {
                    "prime_stocks": {
                        "current": {
                            "slots": {
                                "slot_1": {
                                    "cycles": {
                                        "latest": {
                                            "per_symbol_results": [
                                                {"symbol": "AAPL", "execution_decision": "submitted_buy"},
                                                {"symbol": "MSFT", "execution_decision": "no_op"},
                                            ],
                                            "symbol_states": {
                                                "AAPL": {"symbol": "AAPL"},
                                                "MSFT": {"symbol": "MSFT"},
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

    store.write_runtime_cycle_summary(
        uid="user-a",
        account_id=101,
        alpaca_account_id=999,
        slot_number=1,
        run_id="run-1",
        trigger_type="scheduled",
        trigger_source="test",
        target_count=1,
        completed_count=1,
        results=[{"symbol": "NVDA", "execution_decision": "no_op", "run_id": "run-1"}],
        service_revision="rev-1",
        service_name="svc-1",
    )

    cycle = _resolve_payload(
        fake_client.storage,
        ["users", "user-a", "accounts", "101", "prime_stocks", "current", "slots", "slot_1", "cycles", "latest"],
    )
    assert [item["symbol"] for item in cycle["per_symbol_results"]] == ["NVDA"]
    assert sorted(cycle["symbol_states"].keys()) == ["NVDA"]


def test_write_runtime_cycle_summary_omits_no_active_symbols_placeholder_rows(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)

    store.write_runtime_cycle_summary(
        uid="user-a",
        account_id=101,
        alpaca_account_id=999,
        slot_number=1,
        run_id="run-1",
        trigger_type="scheduled",
        trigger_source="test",
        target_count=1,
        completed_count=1,
        results=[{
            "symbol": "AAPL",
            "execution_decision": "no_active_symbols_configured",
            "skipped_reason": "no_active_symbols_configured",
            "run_id": "run-1",
        }],
        service_revision="rev-1",
        service_name="svc-1",
    )

    cycle = _resolve_payload(
        fake_client.storage,
        ["users", "user-a", "accounts", "101", "prime_stocks", "current", "slots", "slot_1", "cycles", "latest"],
    )
    assert cycle["per_symbol_results"] == []
    assert cycle["symbol_states"] == {}
    assert cycle["symbols_scanned"] == []
    assert cycle["symbols_scanned_count"] == 0


def test_build_strategy_reasoning_payload_includes_signal_score() -> None:
    strategy_result = PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="signal",
        message="ok",
        series=PineComputedSeries(
            trend_ok=[True],
            trend_base_htf=[True],
            htf_ema_slow_slope_up=[True],
            in_pullback_zone=[True],
            setup_ready=[True],
            setup_age_bars=[0],
            setup_invalidated=[False],
            momentum_confirm=[True],
        ),
        latest_signal=PineSignalSnapshot(
            base_entry_signal=True,
            base_entry_trigger=True,
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
        signal_score=88.5,
    )

    payload = _build_strategy_reasoning_payload(
        strategy_result=strategy_result,
        candidate_action="FirstLot",
        execution_decision="submitted_buy",
        ai_decision=None,
    )

    assert payload["signal_score"] == 88.5


def test_write_prime_stocks_trade_performance_updates_trade_and_summary_docs(mock_app_config):
    fake_client = FakeFirestoreClient()
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=fake_client)

    store.write_prime_stocks_trade_performance(
        uid="user-a",
        account_id=101,
        trade_id="trade-1",
        trade_payload={
            "symbol": "USO",
            "realized_pnl_dollars": 12.5,
            "entry_notional": 300.0,
            "exit_notional": 312.5,
            "updated_at": "2026-04-16T20:30:00+00:00",
        },
    )

    performance_root = fake_client.storage["users"]["user-a"]["accounts"]["101"]["performance"]["current"]
    trade_doc = performance_root["trades"]["trade-1"]
    summary_doc = performance_root["summary"]["current"]

    assert trade_doc["symbol"] == "USO"
    assert trade_doc["trade_id"] == "trade-1"
    assert trade_doc["realized_pnl_dollars"] == 12.5
    assert summary_doc["total_trades"] == 1
    assert summary_doc["wins"] == 1
    assert summary_doc["losses"] == 0
    assert summary_doc["win_rate"] == 100.0
    assert summary_doc["best_symbol"] == "USO"
    assert summary_doc["worst_symbol"] == "USO"

def test_load_latest_execution_record_document_not_found(mock_app_config):
    default_record = PrimeStocksLatestExecutionRecord()

    with patch.object(
        PrimeStocksFirestoreRuntimeStore, "load_latest_execution_record", return_value=default_record
    ) as mock_load_latest_execution_record:
        store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config)
        record = store.load_latest_execution_record()




def test_load_runtime_state_record_document_not_found(mock_app_config):
    default_record = PrimeStocksRuntimeStateRecord()

    with patch.object(
        PrimeStocksFirestoreRuntimeStore, "load_runtime_state_record", return_value=default_record
    ) as mock_load_runtime_state_record:
        store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config)
        record = store.load_runtime_state_record()


def test_load_runtime_state_record_parses_extended_fields(mock_app_config):
    client = MagicMock()
    document = MagicMock()
    document.get.return_value = MagicMock(
        exists=True,
        to_dict=lambda: {
            "run_id": "run-123",
            "account_id": 8,
            "alpaca_account_id": 7,
            "broker_environment": "paper",
            "symbol": "AAPL",
            "position_open": True,
            "position_size": 2.0,
            "position_avg_price": 101.5,
            "dollars_used": 174.0,
            "add_count": 2,
            "add_tiers_filled": [1, "2", "bad"],
            "last_add_price": 99.0,
            "pos_high": 107.0,
            "trail_stop": 103.0,
            "last_entry_time": "2026-04-09T10:00:00+00:00",
            "last_exit_time": None,
            "last_action": "MULTI-2",
            "candidate_action": "MULTI-2",
            "execution_key": "MULTI-2:2026-04-09T10:00:00+00:00",
            "last_processed_bar_time": "2026-04-09T10:00:00+00:00",
            "latest_signal_time": "2026-04-09T10:00:00+00:00",
            "latest_candidate_action": "MULTI-2",
            "latest_status": "signal",
            "latest_execution_decision": "submitted_buy",
        },
    )
    root = MagicMock()
    root.collection.return_value.document.return_value = document
    client.collection.return_value.document.return_value = root

    record = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=client).load_runtime_state_record()

    assert record.run_id == "run-123"
    assert record.account_id == 8
    assert record.alpaca_account_id == 7
    assert record.position_open is True
    assert record.position_size == 2.0
    assert record.add_tiers_filled == [1, 2]
    assert record.latest_execution_decision == "submitted_buy"


def test_load_runtime_config_wraps_firestore_errors(mock_app_config):
    failing_document = MagicMock()
    failing_document.get.side_effect = RuntimeError("permission denied")
    failing_root = MagicMock()
    failing_root.collection.return_value.document.return_value = failing_document
    failing_client = MagicMock()
    failing_client.collection.return_value.document.return_value = failing_root
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=failing_client)

    default_config = PrimeStocksRuntimeConfigRecord(
        product_key="stocks.bismel1",
        strategy_key="prime_stocks",
        strategy_title="Prime Stocks Bot Trader",
        symbol="AAPL",
        asset_type="stock",
        enabled=True,
        dry_run=True,
        paper_execution_enabled=False,
        live_execution_enabled=False,
        ping_enabled=False,
        ping_mode="off",
        ping_daily_heartbeat_enabled=False,
        test_mode=False,
        test_trigger=None,
        test_symbol_override=None,
        force_candidate_action=None,
        ai_validation_bypass_enabled=False,
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=351,
        trend_bar_limit=221,
        first_lot_notional=101.0,
        multi_notional=73.0,
        max_notional_per_order=303.0,
        max_total_notional_per_symbol=707.0,
        max_add_count=2,
        daily_order_cap=None,
        max_open_positions=None,
        broker_retry_max_attempts=1,
        account_id=None,
        alpaca_account_id=None,
        runtime_target="cloud_run",
        entitlement={},
    )

    with pytest.raises(PrimeStocksRuntimeStoreError, match="runtime_products/prime_stocks/config/current"):
        store.load_runtime_config(default_config)


@pytest.mark.parametrize(
    "persisted_execution_timeframe, expected_warning_fragment",
    [
        ("1H", "normalized execution_timeframe from 1H to 15M"),
        ("bogus", "normalized execution_timeframe from BOGUS to 15M"),
    ],
)
def test_load_runtime_config_normalizes_invalid_or_non_15m_execution_timeframe(
    mock_app_config,
    persisted_execution_timeframe,
    expected_warning_fragment,
    caplog,
):
    client = MagicMock()
    config_document = MagicMock()
    config_document.get.return_value = MagicMock(
        exists=True,
        to_dict=MagicMock(return_value={
            "execution_timeframe": persisted_execution_timeframe,
            "trend_timeframe": "1D",
        }),
    )
    document_level_1 = MagicMock()
    document_level_2 = MagicMock()
    document_level_3 = MagicMock()
    document_level_4 = MagicMock()
    client.collection.return_value = document_level_1
    document_level_1.document.return_value = document_level_2
    document_level_2.collection.return_value = document_level_3
    document_level_3.document.return_value = config_document
    # Keep the mock chain explicit so the Firestore document path matches
    # runtime_products/prime_stocks/config/current exactly.

    default_config = PrimeStocksRuntimeConfigRecord(
        product_key="stocks.bismel1",
        strategy_key="prime_stocks",
        strategy_title="Prime Stocks Bot Trader",
        symbol="AAPL",
        asset_type="stock",
        enabled=True,
        dry_run=True,
        paper_execution_enabled=True,
        live_execution_enabled=False,
        ping_enabled=False,
        ping_mode="off",
        ping_daily_heartbeat_enabled=False,
        test_mode=False,
        test_trigger=None,
        test_symbol_override=None,
        force_candidate_action=None,
        ai_validation_bypass_enabled=False,
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=351,
        trend_bar_limit=221,
        first_lot_notional=101.0,
        multi_notional=73.0,
        max_notional_per_order=303.0,
        max_total_notional_per_symbol=707.0,
        max_add_count=2,
        daily_order_cap=None,
        max_open_positions=None,
        broker_retry_max_attempts=1,
        account_id=None,
        alpaca_account_id=None,
        runtime_target="cloud_run",
        entitlement={},
    )

    with caplog.at_level("WARNING"):
        config = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config, client=client).load_runtime_config(
            default_config
        )

    assert config.execution_timeframe == "15M"
    assert expected_warning_fragment in caplog.text


def test_get_paths_scopes_runtime_documents_per_user_and_account(mock_app_config):
    store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config)

    paths = store.get_paths(uid="user-a", account_id=11)
    slot_paths = store.get_paths(uid="user-a", account_id=11, slot_number=1)

    assert paths.config_document == "users/user-a/accounts/11/prime_stocks/current/config/current"
    assert paths.state_document == "users/user-a/accounts/11/prime_stocks/current/state/current"
    assert paths.execution_document == "users/user-a/accounts/11/prime_stocks/current/execution/current"
    assert paths.logs_collection == "users/user-a/accounts/11/prime_stocks/current/logs"
    assert slot_paths.config_document == "users/user-a/accounts/11/prime_stocks/current/slots/slot_1/config/current"
    assert slot_paths.state_document == "users/user-a/accounts/11/prime_stocks/current/slots/slot_1/state/current"
