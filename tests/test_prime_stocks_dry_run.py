# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: tests/test_prime_stocks_dry_run.py
# ======================================================

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from app.brokers.alpaca_market_data import PrimeStocksBarSet
from app.brokers.alpaca_paper_trading import AlpacaPaperExecutionResult
from app.products.stocks.bismel1.models import (
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    PineSignalStateBar,
    PineSignalStateEvaluation,
    PriceBar,
    PrimeStocksStrategyResult,
)
from app.runtime.prime_stocks_dry_run import PrimeStocksRuntimeService
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksRuntimeStoreError,
    build_default_runtime_config,
)
from app.shared.config import AppConfig


def test_dry_run_service_writes_snapshot_signal_state_and_log_records() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=False)

    assert result.mode == "dry-run"
    assert result.trigger_type == "manual"
    assert result.trigger_source == "api"
    assert result.runtime_target == "cloud_run"
    assert result.symbol == "AAPL"
    assert result.asset_type == "stock"
    assert result.candidate_action == "HOLD"
    assert result.execution_decision == "dry_run_only"
    assert result.order_submitted is False
    assert result.order_status == "not_submitted"
    assert result.skipped_reason == "paper_execution_disabled"
    assert result.status == "parity_scaffolding_only"
    assert result.bars_processed_execution == 11
    assert result.bars_processed_trend == 11
    assert result.firestore_paths["config_document"] == "runtime_products/prime_stocks/config/current"

    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["config"]["current"]["product_key"] == "stocks.bismel1"
    assert root["state"]["current"]["last_processed_bar_time"] is not None
    assert root["snapshots"]["latest"]["dry_run"] is True
    assert root["signals"]["latest"]["candidate_action"] == "HOLD"
    assert root["execution"]["current"]["execution_decision"] == "dry_run_only"
    assert root["actions"]["latest"]["execution"]["submitted"] is False
    assert len(root["logs"]) == 1


def test_runtime_service_submits_first_lot_buy_when_paper_enabled() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.mode == "paper"
    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert result.order_status == "accepted"
    assert paper_trading.calls[0]["action"] == "FirstLot"
    assert paper_trading.calls[0]["notional"] == 101.0


def test_runtime_service_submits_exit_order_when_exit_candidate_is_present() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("EXIT_ATR"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_exit"
    assert result.order_submitted is True
    assert paper_trading.calls[0]["action"] == "EXIT_ATR"


def test_runtime_service_skips_noop_when_candidate_action_is_hold() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "no_op"
    assert result.order_submitted is False
    assert result.skipped_reason == "no_action_candidate"
    assert paper_trading.calls == []


def test_runtime_service_skips_duplicate_candidate_action() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("execution").document("current").set(
        {
            "execution_key": f"FirstLot:{latest_signal_time}",
            "order_status": "accepted",
            "run_id": "prior-run",
            "candidate_action": "FirstLot",
            "latest_signal_time": latest_signal_time,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "skipped_duplicate"
    assert result.order_submitted is False
    assert result.skipped_reason == "duplicate_candidate_action"
    assert paper_trading.calls == []


def test_runtime_service_rejects_non_stock_runtime_config() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["asset_type"] = "crypto"
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(default_config)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    try:
        service.run_once()
    except ValueError as exc:
        assert "asset_type='stock'" in str(exc)
    else:
        raise AssertionError("Expected Prime Stocks runtime to reject non-stock runtime config.")


def test_runtime_service_skips_when_no_new_closed_bar_is_available() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "last_processed_bar_time": latest_signal_time,
            "latest_candidate_action": "FirstLot",
            "latest_status": "ok",
            "latest_execution_decision": "submitted_buy",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "no_op"
    assert result.execution_decision == "skipped_no_new_bar"
    assert result.skipped_reason == "no_new_closed_bar"
    assert result.trigger_type == "scheduled"
    assert result.trigger_source == "cloud_scheduler"


def test_runtime_service_skips_when_same_bar_is_reprocessed() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "last_processed_bar_time": latest_signal_time,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("MULTI"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.execution_decision == "skipped_no_new_bar"
    assert result.order_submitted is False
    assert paper_trading.calls == []


def test_runtime_service_continues_when_new_bar_exists() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    older_bar_time = _bars()[-2].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "last_processed_bar_time": older_bar_time,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["state"]["current"]["last_processed_bar_time"] == _bars()[-1].ends_at.isoformat()
    assert root["state"]["current"]["trigger_type"] == "scheduled"


def test_runtime_service_returns_blocked_result_when_runtime_config_load_fails() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=FailingRuntimeStore(settings=settings, fail_on="config"),
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "blocked"
    assert result.execution_decision == "runtime_store_unavailable"
    assert result.skipped_reason == "runtime_store_unavailable"
    assert result.order_submitted is False
    assert result.trigger_type == "scheduled"
    assert "Firestore runtime control data could not be loaded" in result.message


def test_runtime_service_returns_degraded_result_when_runtime_result_write_fails() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=FailingRuntimeStore(settings=settings, fail_on="write"),
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "degraded"
    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert result.skipped_reason == "runtime_result_persistence_failed"
    assert "Firestore result persistence failed" in result.message


def test_runtime_service_returns_blocked_result_when_market_data_fetch_fails_after_runtime_config_load() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FailingMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "blocked"
    assert result.execution_decision == "market_data_unavailable"
    assert result.skipped_reason == "market_data_unavailable"
    assert result.order_submitted is False
    assert result.trigger_type == "scheduled"
    assert result.trigger_source == "cloud_scheduler"
    assert "Alpaca market data could not be fetched after runtime config load" in result.message


class FakeMarketData:
    def fetch_prime_stocks_bars(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
    ) -> PrimeStocksBarSet:
        del asset_type, product_key, execution_limit, trend_limit
        return PrimeStocksBarSet(
            symbol=symbol,
            execution_bars=_bars(),
            trend_bars=_bars(),
        )


class FailingMarketData:
    def fetch_prime_stocks_bars(self, **kwargs) -> PrimeStocksBarSet:
        del kwargs
        raise RuntimeError("Alpaca credentials are required for Prime Stocks market-data fetches.")


class FakePaperTrading:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def submit_first_lot_buy(self, **kwargs) -> AlpacaPaperExecutionResult:
        self.calls.append({"action": "FirstLot", **kwargs})
        return AlpacaPaperExecutionResult(
            action="FirstLot",
            submitted=True,
            order_status="accepted",
            order_id="order-first",
            client_order_id=str(kwargs["client_order_id"]),
            side="buy",
            notional=float(kwargs["notional"]),
        )

    def submit_multi_buy(self, **kwargs) -> AlpacaPaperExecutionResult:
        self.calls.append({"action": "MULTI", **kwargs})
        return AlpacaPaperExecutionResult(
            action="MULTI",
            submitted=True,
            order_status="accepted",
            order_id="order-multi",
            client_order_id=str(kwargs["client_order_id"]),
            side="buy",
            notional=float(kwargs["notional"]),
        )

    def close_position(self, **kwargs) -> AlpacaPaperExecutionResult:
        self.calls.append({"action": kwargs["action"], **kwargs})
        return AlpacaPaperExecutionResult(
            action=str(kwargs["action"]),
            submitted=True,
            order_status="accepted",
            order_id="order-exit",
            client_order_id=str(kwargs["client_order_id"]),
            side="sell",
            notional=None,
        )


class FakeSnapshot:
    def __init__(self, payload):
        self._payload = payload
        self.exists = payload is not None

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


class FailingRuntimeStore(PrimeStocksFirestoreRuntimeStore):
    def __init__(self, settings: AppConfig, fail_on: str) -> None:
        super().__init__(settings=settings, client=FakeFirestoreClient())
        self._fail_on = fail_on

    def load_runtime_config(self, default_config):
        if self._fail_on == "config":
            raise PrimeStocksRuntimeStoreError("simulated config read failure")
        return super().load_runtime_config(default_config)

    def write_runtime_result(self, **kwargs) -> None:
        if self._fail_on == "write":
            raise PrimeStocksRuntimeStoreError("simulated write failure")
        return super().write_runtime_result(**kwargs)


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


def _bars() -> list[PriceBar]:
    start = datetime(2026, 4, 5, 13, 0, tzinfo=UTC)
    closes = [101.0, 102.0, 103.0, 104.0, 103.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    return [
        PriceBar(
            starts_at=start + timedelta(hours=index),
            ends_at=start + timedelta(hours=index + 1),
            open=close - 1.0,
            high=close + 1.0,
            low=close - 2.0,
            close=close,
            volume=1000.0 + index,
        )
        for index, close in enumerate(closes)
    ]


def _strategy_result(candidate_action: str) -> PrimeStocksStrategyResult:
    base_entry_trigger = candidate_action == "FirstLot"
    add_trigger = candidate_action == "MULTI"
    hit_atr_trail = candidate_action == "EXIT_ATR"
    hit_regime = candidate_action == "EXIT_REGIME"
    state = BismillahTrobotStocksV1State()
    signal = PineSignalSnapshot(
        base_entry_signal=base_entry_trigger,
        base_entry_trigger=base_entry_trigger,
        add_bounce_confirm=add_trigger,
        gate_atr_ok=True,
        gate_dp_ok=True,
        cap_ok=True,
        add_signal_raw=add_trigger,
        add_trigger=add_trigger,
        hit_atr_trail=hit_atr_trail,
        hit_regime=hit_regime,
    )
    latest_bar = PineSignalStateBar(
        bar_index=10,
        regime_fail=hit_regime,
        auto_paused=False,
        pause_new_basket=False,
        pause_adds=False,
        in_position_before=True,
        signal=signal,
        state_before=state,
        state_after=state,
    )
    evaluation = PineSignalStateEvaluation(
        series=PineComputedSeries(),
        bars=[latest_bar],
        final_state=state,
    )
    return PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="parity_scaffolding_only",
        message="stub strategy result",
        series=evaluation.series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=state,
        execution_timeframe="1H",
        trend_timeframe="1D",
    )


def _settings(**overrides) -> AppConfig:
    base = dict(
        app_name="Bismel1-ex-py",
        environment="test",
        host="0.0.0.0",
        port=8080,
        cloud_run_target=True,
        pine_source_filename="Stocks-pine.pine",
        firestore_project_id=None,
        firestore_database_id="(default)",
        firestore_runtime_collection="runtime_products",
        firestore_product_document="prime_stocks",
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_api_key_id="key-123",
        alpaca_api_secret="secret-123",
        alpaca_data_feed="iex",
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
        prime_stocks_first_lot_notional=101.0,
        prime_stocks_multi_notional=73.0,
        prime_stocks_scheduler_job_name="prime-stocks-scheduled",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="secret-value",
    )
    base.update(overrides)
    return AppConfig(**base)
