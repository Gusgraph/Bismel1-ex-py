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

from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta

from app.brokers.alpaca_market_data import PrimeStocksBarSet
from app.brokers.alpaca_paper_trading import (
    AlpacaPaperAccountState,
    AlpacaPaperAssetState,
    AlpacaPaperExecutionResult,
    AlpacaPaperPositionState,
    AlpacaPaperSubmissionState,
    AlpacaPaperTradingError,
)
from app.products.stocks.bismel1.models import (
    AiCacheRecord,
    BismillahTrobotStocksV1State,
    PineComputedSeries,
    PineSignalSnapshot,
    PineSignalStateBar,
    PineSignalStateEvaluation,
    PriceBar,
    PrimeStocksStrategyResult,
)
from app.runtime.prime_stocks_dry_run import PrimeStocksRuntimeService
from app.services.alpaca_account_resolver import AlpacaAccountResolutionError, ResolvedAlpacaAccountContext
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksRuntimeStoreError,
    build_default_runtime_config,
    _build_strategy_reasoning_payload,
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
        account_resolver=FakeAccountResolver(),
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
    assert result.status == "no_signal"
    assert result.bars_processed_execution == 11
    assert result.bars_processed_trend == 11
    assert result.firestore_paths["config_document"] == "runtime_products/prime_stocks/config/current"

    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["config"]["current"]["product_key"] == "stocks.bismel1"
    assert root["state"]["current"]["last_processed_bar_time"] is not None
    assert root["snapshots"]["latest"]["dry_run"] is True
    assert root["snapshots"]["latest"]["ai"]["Ai_regime_label"] == "neutral"
    assert root["signals"]["latest"]["candidate_action"] == "HOLD"
    assert root["execution"]["current"]["execution_decision"] == "dry_run_only"
    assert root["actions"]["latest"]["execution"]["submitted"] is False
    assert len(root["logs"]) == 1


def test_strategy_reasoning_uses_full_1h_and_1d_context() -> None:
    series = PineComputedSeries(
        trend_ok=[False, True],
        trend_base_htf=[False, True],
        htf_ema_slow_slope_up=[False, True],
        in_pullback_zone=[False, True],
        regime_fail=[True, False],
    )
    signal = PineSignalSnapshot(
        base_entry_signal=True,
        base_entry_trigger=True,
        add_bounce_confirm=False,
        gate_atr_ok=True,
        gate_dp_ok=True,
        cap_ok=True,
        add_signal_raw=False,
        add_trigger=False,
        hit_atr_trail=False,
        hit_regime=False,
    )
    latest_bar = PineSignalStateBar(
        bar_index=1,
        regime_fail=False,
        auto_paused=False,
        pause_new_basket=False,
        pause_adds=False,
        in_position_before=False,
        signal=signal,
        state_before=BismillahTrobotStocksV1State(),
        state_after=BismillahTrobotStocksV1State(),
    )
    strategy_result = PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="signal",
        message="stub strategy result",
        series=series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=BismillahTrobotStocksV1State(),
        ai_decision=None,
        execution_allowed=True,
        execution_timeframe="15M",
        trend_timeframe="1D",
    )

    reasoning = _build_strategy_reasoning_payload(
        strategy_result=strategy_result,
        candidate_action="FirstLot",
        execution_decision="submitted_buy",
        ai_decision=None,
    )

    assert reasoning["strategy_context"] == "15M closed bars + 1D trend"
    assert reasoning["trend_1d"] == "Up"
    assert reasoning["bias_state"] == "Long preferred"
    assert reasoning["pullback_state"] == "Yes"
    assert reasoning["confirmation_state"] == "Yes"
    assert reasoning["trigger_state"] == "Buy"
    assert reasoning["ai_filter_state"] == "Neutral"
    assert reasoning["setup_state"] == "Valid"
    assert reasoning["final_decision"] == "Buy"
    assert "15M trigger aligned" in reasoning["primary_reason"]


def test_strategy_reasoning_reports_setup_state_from_signal_not_trigger() -> None:
    series = PineComputedSeries(
        trend_ok=[True, True],
        trend_base_htf=[True, True],
        htf_ema_slow_slope_up=[True, True],
        in_pullback_zone=[False, True],
        regime_fail=[False, False],
    )
    signal = PineSignalSnapshot(
        base_entry_signal=True,
        base_entry_trigger=False,
        add_bounce_confirm=False,
        gate_atr_ok=True,
        gate_dp_ok=True,
        cap_ok=True,
        add_signal_raw=False,
        add_trigger=False,
        hit_atr_trail=False,
        hit_regime=False,
    )
    latest_bar = PineSignalStateBar(
        bar_index=1,
        regime_fail=False,
        auto_paused=False,
        pause_new_basket=False,
        pause_adds=False,
        in_position_before=False,
        signal=signal,
        state_before=BismillahTrobotStocksV1State(),
        state_after=BismillahTrobotStocksV1State(),
    )
    strategy_result = PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="signal",
        message="stub strategy result",
        series=series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=BismillahTrobotStocksV1State(),
        ai_decision=None,
        execution_allowed=True,
        execution_timeframe="15M",
        trend_timeframe="1D",
    )

    reasoning = _build_strategy_reasoning_payload(
        strategy_result=strategy_result,
        candidate_action="FirstLot",
        execution_decision="no_op",
        ai_decision=None,
    )

    assert reasoning["setup_state"] == "Valid"
    assert reasoning["confirmation_state"] == "No"
    assert reasoning["trigger_state"] == "Waiting"


def test_strategy_reasoning_treats_downtrend_as_soft_bias_in_scalper_mode() -> None:
    series = PineComputedSeries(
        trend_ok=[False, False],
        trend_base_htf=[False, False],
        htf_ema_slow_slope_up=[False, False],
        in_pullback_zone=[False, True],
        regime_fail=[True, True],
    )
    signal = PineSignalSnapshot(
        base_entry_signal=True,
        base_entry_trigger=True,
        add_bounce_confirm=False,
        gate_atr_ok=True,
        gate_dp_ok=True,
        cap_ok=True,
        add_signal_raw=False,
        add_trigger=False,
        hit_atr_trail=False,
        hit_regime=False,
    )
    latest_bar = PineSignalStateBar(
        bar_index=1,
        regime_fail=False,
        auto_paused=False,
        pause_new_basket=False,
        pause_adds=False,
        in_position_before=False,
        signal=signal,
        state_before=BismillahTrobotStocksV1State(),
        state_after=BismillahTrobotStocksV1State(),
    )
    strategy_result = PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="signal",
        message="stub strategy result",
        series=series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=BismillahTrobotStocksV1State(),
        ai_decision=None,
        execution_allowed=True,
        execution_timeframe="15M",
        trend_timeframe="1D",
        strategy_mode="scalper",
    )

    reasoning = _build_strategy_reasoning_payload(
        strategy_result=strategy_result,
        candidate_action="FirstLot",
        execution_decision="submitted_buy",
        ai_decision=None,
    )

    assert reasoning["bias_state"] == "Against trend (scalper mode)"
    assert reasoning["trend_weight"] == 0.7
    assert reasoning["primary_reason"] == "Against trend (scalper mode)."


def test_strategy_reasoning_treats_ai_cache_stale_as_advisory() -> None:
    series = PineComputedSeries(
        trend_ok=[True, True],
        trend_base_htf=[True, True],
        htf_ema_slow_slope_up=[True, True],
        in_pullback_zone=[True, True],
        regime_fail=[False, False],
    )
    signal = PineSignalSnapshot(
        base_entry_signal=True,
        base_entry_trigger=True,
        add_bounce_confirm=False,
        gate_atr_ok=True,
        gate_dp_ok=True,
        cap_ok=True,
        add_signal_raw=False,
        add_trigger=False,
        hit_atr_trail=False,
        hit_regime=False,
    )
    latest_bar = PineSignalStateBar(
        bar_index=1,
        regime_fail=False,
        auto_paused=False,
        pause_new_basket=False,
        pause_adds=False,
        in_position_before=False,
        signal=signal,
        state_before=BismillahTrobotStocksV1State(),
        state_after=BismillahTrobotStocksV1State(),
    )
    strategy_result = PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="signal",
        message="stub strategy result",
        series=series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=BismillahTrobotStocksV1State(),
        ai_decision=None,
        execution_allowed=True,
        execution_timeframe="15M",
        trend_timeframe="1D",
    )
    ai_decision = AiCacheRecord(
        scope="symbol",
        symbol="AAPL",
        Ai_regime_label="neutral",
        Ai_sentiment_label="neutral",
        Ai_safety_label="caution",
        Ai_confidence=0.0,
        Ai_reason="AI cache is stale for Prime Stocks runtime evaluation.",
        Ai_updated_at=datetime.now(tz=UTC).isoformat(),
        Ai_source="cached_gemini",
        Ai_execution_allowed=True,
        Ai_block_new_entries=False,
        Ai_block_adds=False,
        Ai_blocked_reason=None,
        is_stale=True,
        is_available=True,
    )

    reasoning = _build_strategy_reasoning_payload(
        strategy_result=strategy_result,
        candidate_action="FirstLot",
        execution_decision="submitted_buy",
        ai_decision=ai_decision,
    )

    assert reasoning["ai_filter_state"] == "Cautious"
    assert reasoning["primary_reason"] == "15M trigger aligned with 1D bias."


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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.mode == "paper"
    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert result.order_status == "accepted"
    assert paper_trading.calls[-1]["action"] == "FirstLot"
    assert paper_trading.calls[-1]["notional"] == 300.0


def test_runtime_service_can_force_candidate_action_for_validation() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_force_candidate_action="FirstLot",
    )
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.candidate_action == "FirstLot"
    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert paper_trading.calls[-1]["action"] == "FirstLot"


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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("EXIT_ATR"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_exit"
    assert result.order_submitted is True
    assert paper_trading.calls[-1]["action"] == "EXIT_ATR"


def test_runtime_service_submits_tier_aware_multi_buy_when_add_signal_is_present() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-2"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert result.add_tier == 2
    assert paper_trading.calls[-1]["action"] == "MULTI-2"
    assert paper_trading.calls[-1]["add_tier"] == 2
    assert paper_trading.calls[-1]["notional"] == 161.6


def test_runtime_service_writes_isolated_firestore_docs_per_user_and_account() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)

    user_a_account_1_service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(
            context=ResolvedAlpacaAccountContext(
                uid="user-a",
                account_id=1,
                alpaca_account_id=501,
                broker_connection_id=301,
                broker_credential_id=401,
                environment="paper",
                data_feed="iex",
                access_mode="trade",
                trade_enabled=True,
                key_id="resolved-key-a1",
                secret="resolved-secret-a1",
                slot_number=1,
                entitlement={
                    "product_key": "stocks.bismel1",
                    "enabled": True,
                    "runtime_allowed": True,
                    "paper_available": True,
                    "live_available": False,
                },
            )
        ),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )
    user_a_account_2_service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(
            context=ResolvedAlpacaAccountContext(
                uid="user-a",
                account_id=2,
                alpaca_account_id=502,
                broker_connection_id=302,
                broker_credential_id=402,
                environment="paper",
                data_feed="iex",
                access_mode="trade",
                trade_enabled=True,
                key_id="resolved-key-a2",
                secret="resolved-secret-a2",
                slot_number=2,
                entitlement={
                    "product_key": "stocks.bismel1",
                    "enabled": True,
                    "runtime_allowed": True,
                    "paper_available": True,
                    "live_available": False,
                },
            )
        ),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )
    user_b_account_1_service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(
            context=ResolvedAlpacaAccountContext(
                uid="user-b",
                account_id=1,
                alpaca_account_id=601,
                broker_connection_id=303,
                broker_credential_id=403,
                environment="paper",
                data_feed="iex",
                access_mode="trade",
                trade_enabled=True,
                key_id="resolved-key-b1",
                secret="resolved-secret-b1",
                slot_number=1,
                entitlement={
                    "product_key": "stocks.bismel1",
                    "enabled": True,
                    "runtime_allowed": True,
                    "paper_available": True,
                    "live_available": False,
                },
            )
        ),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    user_a_account_1_result = user_a_account_1_service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=1,
        alpaca_account_id=501,
        slot_number=1,
        allow_execution=False,
    )
    user_a_account_2_result = user_a_account_2_service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=2,
        alpaca_account_id=502,
        slot_number=2,
        allow_execution=False,
    )
    user_b_account_1_result = user_b_account_1_service.run_once(
        symbol="AAPL",
        uid="user-b",
        account_id=1,
        alpaca_account_id=601,
        slot_number=1,
        allow_execution=False,
    )

    assert user_a_account_1_result.firestore_paths["config_document"] == "users/user-a/accounts/1/prime_stocks/current/slots/slot_1/config/current"
    assert user_a_account_2_result.firestore_paths["config_document"] == "users/user-a/accounts/2/prime_stocks/current/slots/slot_2/config/current"
    assert user_b_account_1_result.firestore_paths["config_document"] == "users/user-b/accounts/1/prime_stocks/current/slots/slot_1/config/current"

    user_a_account_1_root = fake_client.storage["users"]["user-a"]["accounts"]["1"]["prime_stocks"]["current"]["slots"]["slot_1"]
    user_a_account_2_root = fake_client.storage["users"]["user-a"]["accounts"]["2"]["prime_stocks"]["current"]["slots"]["slot_2"]
    user_b_account_1_root = fake_client.storage["users"]["user-b"]["accounts"]["1"]["prime_stocks"]["current"]["slots"]["slot_1"]

    assert user_a_account_1_root["state"]["current"]["uid"] == "user-a"
    assert user_a_account_1_root["state"]["current"]["account_id"] == 1
    assert user_a_account_1_root["state"]["current"]["alpaca_account_id"] == 501

    assert user_a_account_2_root["state"]["current"]["uid"] == "user-a"
    assert user_a_account_2_root["state"]["current"]["account_id"] == 2
    assert user_a_account_2_root["state"]["current"]["alpaca_account_id"] == 502

    assert user_b_account_1_root["state"]["current"]["uid"] == "user-b"
    assert user_b_account_1_root["state"]["current"]["account_id"] == 1
    assert user_b_account_1_root["state"]["current"]["alpaca_account_id"] == 601

    assert user_a_account_1_root["state"]["current"]["run_id"] != user_a_account_2_root["state"]["current"]["run_id"]
    assert user_a_account_1_root["state"]["current"]["run_id"] != user_b_account_1_root["state"]["current"]["run_id"]
    assert user_a_account_2_root["state"]["current"]["run_id"] != user_b_account_1_root["state"]["current"]["run_id"]


def test_runtime_service_persists_symbol_specific_ai_state_per_symbol() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("ai_symbols").document("AAPL").set(
        {
            "scope": "symbol",
            "symbol": "AAPL",
            "Ai_regime_label": "risk_on",
            "Ai_sentiment_label": "bearish",
            "Ai_safety_label": "safe",
            "Ai_confidence": 0.88,
            "Ai_reason": "AAPL bearish cache",
            "Ai_updated_at": datetime.now(tz=UTC).isoformat(),
            "Ai_source": "gemini:test",
            "Ai_execution_allowed": True,
            "Ai_block_new_entries": False,
            "Ai_block_adds": False,
            "Ai_blocked_reason": None,
        }
    )
    fake_client.collection("runtime_products").document("prime_stocks").collection("ai_symbols").document("MSFT").set(
        {
            "scope": "symbol",
            "symbol": "MSFT",
            "Ai_regime_label": "risk_on",
            "Ai_sentiment_label": "bullish",
            "Ai_safety_label": "safe",
            "Ai_confidence": 0.92,
            "Ai_reason": "MSFT bullish cache",
            "Ai_updated_at": datetime.now(tz=UTC).isoformat(),
            "Ai_source": "gemini:test",
            "Ai_execution_allowed": True,
            "Ai_block_new_entries": False,
            "Ai_block_adds": False,
            "Ai_blocked_reason": None,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    aapl_result = service.run_once(symbol="AAPL", uid="user-a", account_id=1, alpaca_account_id=501, allow_execution=False)
    msft_result = service.run_once(symbol="MSFT", uid="user-a", account_id=1, alpaca_account_id=501, allow_execution=False)

    assert aapl_result.ai is not None
    assert aapl_result.ai["Ai_sentiment_label"] == "bearish"

    scoped_root = fake_client.storage["users"]["user-a"]["accounts"]["1"]["prime_stocks"]["current"]["slots"]["slot_1"]
    assert scoped_root["symbols"]["AAPL"]["state"]["current"]["Ai_sentiment_label"] == "bearish"
    assert scoped_root["symbols"]["MSFT"]["state"]["current"]["Ai_sentiment_label"] == "bullish"

    cycle = scoped_root["cycles"]["latest"]
    assert cycle["symbol_states"]["AAPL"]["Ai_sentiment_label"] == "bearish"
    assert cycle["symbol_states"]["MSFT"]["Ai_sentiment_label"] == "bullish"
    assert cycle["per_symbol_results"][0]["Ai_sentiment_label"] in {"bearish", "bullish"}


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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "no_op"
    assert result.order_submitted is False
    assert result.skipped_reason == "no_action_candidate"
    assert paper_trading.calls == []


def test_runtime_service_force_candidate_action_still_respects_ai_context() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_force_candidate_action="FirstLot",
    )
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, symbol_sentiment="bearish")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.candidate_action == "FirstLot"
    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert paper_trading.calls[-1]["action"] == "FirstLot"


def test_runtime_service_first_lot_ignores_stale_runtime_position_when_broker_is_flat() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        submission_state=AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=1000.0,
                open_positions_count=0,
                equity=10000.0,
                total_exposure=0.0,
            ),
            asset=AlpacaPaperAssetState(symbol="MSFT", tradable=True, status="active"),
            position=None,
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot", in_position_before=True),
    )

    result = service.run_once(symbol="MSFT", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert result.candidate_action == "FirstLot"
    assert paper_trading.calls[-1]["action"] == "FirstLot"


def test_runtime_service_skips_duplicate_candidate_action() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("execution").document("current").set(
        {
            "execution_key": f"FirstLot:AAPL:{latest_signal_time}",
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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "skipped_duplicate"
    assert result.order_submitted is False
    assert result.skipped_reason == "duplicate_candidate_action"
    assert paper_trading.calls == []


def test_runtime_service_skips_duplicate_same_multi_tier_on_same_bar() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("execution").document("current").set(
        {
            "execution_key": f"MULTI-2:{latest_signal_time}",
            "order_status": "accepted",
            "run_id": "prior-run",
            "candidate_action": "MULTI-2",
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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-2"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "skipped_duplicate"
    assert result.add_tier == 2
    assert result.order_submitted is False
    assert paper_trading.calls == []


def test_runtime_service_blocks_invalid_add_without_base_position() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        submission_state=AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=1000.0,
                open_positions_count=0,
                equity=10000.0,
                total_exposure=0.0,
            ),
            asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
            position=None,
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-1", in_position_before=False),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "add_requires_base_position"
    assert result.order_submitted is False
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["blocked_reason"] == "add_requires_base_position"


def test_runtime_service_blocks_when_laravel_entitlement_runtime_is_not_allowed() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(
            context=_account_context(
                entitlement={
                    "product_key": "stocks.bismel1",
                    "enabled": True,
                    "runtime_allowed": False,
                    "blocked_summary": "Prime Stocks product is disabled for the current workspace.",
                }
            )
        ),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "entitlement_runtime_blocked"
    assert result.order_submitted is False
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["blocked_reason"] == "entitlement_runtime_blocked"
    assert root["state"]["current"]["latest_execution_decision"] == "entitlement_runtime_blocked"


def test_runtime_service_uses_specific_billing_block_reason_from_laravel_entitlement() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(
            context=_account_context(
                entitlement={
                    "product_key": "stocks.bismel1",
                    "enabled": True,
                    "runtime_allowed": False,
                    "blocked_reason": "billing_past_due",
                    "blocked_summary": "Billing is inactive because the Stripe subscription is past due.",
                }
            )
        ),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "entitlement_billing_past_due"
    assert result.order_submitted is False


def test_runtime_service_blocks_exit_without_open_position() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("EXIT_ATR", in_position_before=False),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "exit_requires_open_position"
    assert result.order_submitted is False
    assert paper_trading.calls[0]["action"] == "submission_state"


def test_runtime_service_blocks_when_max_add_count_is_exceeded() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True, prime_stocks_max_add_count=2)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        submission_state=AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=1000.0,
                open_positions_count=1,
                equity=10000.0,
                total_exposure=101.0,
            ),
            asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
            position=AlpacaPaperPositionState(symbol="AAPL", qty=1.0, market_value=190.0),
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-3"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "max_add_count_exceeded"


def test_runtime_service_prime_first_lot_uses_prime_per_symbol_entry_budget() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_cap_pct=1.0,
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient())
    paper_trading = FakePaperTrading(
        submission_state=AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=1000.0,
                open_positions_count=0,
                equity=1000.0,
                total_exposure=0.0,
            ),
            asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
            position=None,
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    buy_call = next(call for call in paper_trading.calls if call["action"] == "FirstLot")
    assert abs(float(buy_call["notional"]) - 10.0) < 0.01


def test_runtime_service_prime_first_lot_respects_total_entry_budget() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_cap_pct=27.0,
        prime_stocks_total_entry_exposure_cap_pct=11.0,
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient())
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(
            submission_state=AlpacaPaperSubmissionState(
                account=AlpacaPaperAccountState(
                    buying_power=1000.0,
                    open_positions_count=1,
                    equity=1000.0,
                    total_exposure=90.0,
                ),
                asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
                position=None,
            )
        ),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot", in_position_before=False),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "prime_total_entry_budget_reached"
    assert result.order_submitted is False


def test_runtime_service_prime_add_respects_total_add_budget() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_cap_pct=27.0,
        prime_stocks_total_add_exposure_cap_pct=11.0,
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient())
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(
            submission_state=AlpacaPaperSubmissionState(
                account=AlpacaPaperAccountState(
                    buying_power=1000.0,
                    open_positions_count=1,
                    equity=1000.0,
                    total_exposure=70.0,
                ),
                asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
                position=AlpacaPaperPositionState(symbol="AAPL", qty=1.0, market_value=70.0),
            )
        ),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-1"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "prime_total_add_budget_reached"
    assert result.order_submitted is False


def test_runtime_service_prime_first_lot_ignores_global_kill_switch() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_global_kill_switch_enabled=True,
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient())
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True


def test_runtime_service_allows_exit_when_global_kill_switch_is_enabled() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_global_kill_switch_enabled=True,
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient())
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("EXIT_REGIME"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_exit"
    assert result.order_submitted is True
    assert paper_trading.calls[-1]["action"] == "EXIT_REGIME"


def test_runtime_service_uses_same_logic_for_paper_and_live_execution_paths() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_live_execution_enabled=True,
    )
    paper_trading = FakePaperTrading()

    paper_service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient()),
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(context=_account_context(environment="paper")),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )
    live_service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=PrimeStocksFirestoreRuntimeStore(settings=settings, client=FakeFirestoreClient()),
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(context=_account_context(environment="live")),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    paper_result = paper_service.run_once(symbol="AAPL", allow_execution=True)
    live_result = live_service.run_once(symbol="AAPL", allow_execution=True)

    assert paper_result.execution_decision == "submitted_buy"
    assert live_result.execution_decision == "submitted_buy"
    assert paper_result.candidate_action == live_result.candidate_action == "FirstLot"
    assert paper_result.order_submitted is True
    assert live_result.order_submitted is True


def test_runtime_service_prime_first_lot_ignores_max_notional_cap() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_max_notional_per_order=299.0,
    )
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True


def test_runtime_service_persists_broker_rejection_details() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        submit_first_lot_error=AlpacaPaperTradingError(
            code="broker_insufficient_buying_power",
            message="insufficient buying power",
            retryable=False,
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "broker_insufficient_buying_power"
    assert result.order_submitted is False
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["broker_error_code"] == "broker_insufficient_buying_power"
    assert root["execution"]["current"]["broker_error_message"] == "insufficient buying power"


def test_runtime_service_preview_only_does_not_mark_prime_as_dry_run() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, preview_only=True)

    assert result.execution_decision == "preview_only"
    assert result.order_submitted is False
    assert result.mode == "paper"
    assert paper_trading.calls == []


def test_runtime_service_preview_only_does_not_overwrite_submitted_buy_state() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    executed = service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        slot_number=1,
        allow_execution=True,
    )
    preview = service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        slot_number=1,
        allow_execution=True,
        preview_only=True,
    )

    scoped_root = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]
    symbol_state = scoped_root["symbols"]["AAPL"]["state"]["current"]

    assert executed.execution_decision == "submitted_buy"
    assert preview.execution_decision == "preview_only"
    assert symbol_state["execution_decision"] == "submitted_buy"
    assert symbol_state["order_id"] == "order-first"
    assert symbol_state["latest_order_status"] == "accepted"
    assert scoped_root["execution"]["current"]["execution_decision"] == "submitted_buy"
    assert scoped_root["actions"]["latest"]["execution_decision"] == "submitted_buy"


def test_runtime_service_retries_transient_submit_once_without_duplicate_persistence() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_broker_retry_max_attempts=1,
    )
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(submit_first_lot_failures_before_success=1)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert len([call for call in paper_trading.calls if call["action"] == "FirstLot"]) == 2
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["retry_count"] == 1
    assert root["execution"]["current"]["submitted"] is True


def test_runtime_service_does_not_mark_submitted_buy_without_order_identity() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        submit_first_lot_result=AlpacaPaperExecutionResult(
            action="FirstLot",
            submitted=True,
            order_status="accepted",
            order_id=None,
            client_order_id=None,
            side="buy",
            notional=300.0,
            add_tier=None,
        )
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert result.execution_decision == "broker_invalid_response"
    assert result.order_submitted is False
    assert root["execution"]["current"]["execution_decision"] == "broker_invalid_response"
    assert root["execution"]["current"]["order_status"] == "not_submitted"


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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    try:
        service.run_once()
    except ValueError as exc:
        assert "asset_type='stock'" in str(exc)
    else:
        raise AssertionError("Expected Prime Stocks runtime to reject non-stock runtime config.")


def test_runtime_service_uses_first_active_configured_symbol_for_execution() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["uid"] = "user-a"
    default_config["account_id"] = 101
    default_config["alpaca_account_id"] = 501
    default_config["slot_number"] = 1
    default_config["symbol"] = "AAPL"
    default_config["selected_symbols"] = ["AAPL", "NVDA"]
    default_config["symbol_states"] = [
        {"symbol": "AAPL", "mode": "paused"},
        {"symbol": "NVDA", "mode": "active"},
    ]
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("config").document("current").set(default_config)
    _seed_ai_cache(fake_client)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, slot_number=1, allow_execution=False)

    assert result.symbol == "NVDA"
    assert market_data.calls[0]["symbol"] == "NVDA"


def test_runtime_service_skips_when_no_active_configured_symbols_exist() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["uid"] = "user-a"
    default_config["account_id"] = 101
    default_config["alpaca_account_id"] = 501
    default_config["slot_number"] = 1
    default_config["symbol"] = "AAPL"
    default_config["selected_symbols"] = ["AAPL"]
    default_config["symbol_states"] = [
        {"symbol": "AAPL", "mode": "paused"},
    ]
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("config").document("current").set(default_config)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, slot_number=1, allow_execution=False)

    assert result.execution_decision == "no_active_symbols_configured"
    assert result.candidate_action == "BLOCKED"
    assert market_data.calls == []


def test_slot_scoped_runtime_with_no_managed_symbols_starts_clean() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["uid"] = "user-a"
    default_config["account_id"] = 101
    default_config["alpaca_account_id"] = 501
    default_config["slot_number"] = 1
    default_config["symbol"] = "AAPL"
    default_config["selected_symbols"] = []
    default_config["symbol_states"] = []
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("config").document("current").set(default_config)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, slot_number=1, allow_execution=False)

    assert result.execution_decision == "no_active_symbols_configured"
    assert result.candidate_action == "BLOCKED"
    assert market_data.calls == []


def test_runtime_service_migrates_account_scoped_prime_runtime_into_slot_scope() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    default_config = asdict(build_default_runtime_config(settings))
    default_config["uid"] = "user-a"
    default_config["account_id"] = 101
    default_config["alpaca_account_id"] = 501
    default_config["slot_number"] = 1
    default_config["symbol"] = "AAPL"
    default_config["selected_symbols"] = ["AAPL"]
    default_config["symbol_states"] = [{"symbol": "AAPL", "mode": "active"}]

    account_root = fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current")
    account_root.collection("config").document("current").set(default_config)
    account_root.collection("state").document("current").set({
        "symbol": "AAPL",
        "updated_at": "2026-04-20T12:00:00+00:00",
        "strategy_reasoning": {"trend_1d": "Up"},
    })
    account_root.collection("execution").document("current").set({
        "updated_at": "2026-04-20T12:00:00+00:00",
        "execution_decision": "submitted_buy",
    })
    account_root.collection("symbols").document("AAPL").collection("state").document("current").set({
        "symbol": "AAPL",
        "updated_at": "2026-04-20T12:00:00+00:00",
        "strategy_reasoning": {"final_decision": "Buy"},
    })

    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, slot_number=1, allow_execution=False)

    slot_root = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]
    assert result.firestore_paths["state_document"] == "users/user-a/accounts/101/prime_stocks/current/slots/slot_1/state/current"
    assert slot_root["config"]["current"]["selected_symbols"] == ["AAPL"]
    assert slot_root["state"]["current"]["symbol"] == "AAPL"
    assert slot_root["execution"]["current"]["execution_decision"] is not None
    assert slot_root["symbols"]["AAPL"]["state"]["current"]["strategy_reasoning"]["final_decision"] == "Buy"


def test_runtime_service_skips_when_no_new_closed_bar_is_available() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "last_processed_bar_time": latest_signal_time,
            "latest_candidate_action": "HOLD",
            "latest_status": "ok",
            "latest_execution_decision": "no_op",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "no_op"
    assert result.execution_decision == "skipped_no_new_bar"
    assert result.execution_allowed is False
    assert result.skipped_reason == "no_new_closed_bar"
    assert result.trigger_type == "scheduled"
    assert result.trigger_source == "cloud_scheduler"


def test_runtime_service_allows_firstlot_execution_even_when_no_new_closed_bar_is_available() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "last_processed_bar_time": latest_signal_time,
            "latest_candidate_action": "HOLD",
            "latest_status": "ok",
            "latest_execution_decision": "no_op",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(
        symbol="AAPL",
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.candidate_action == "FirstLot"
    assert any(call["action"] == "FirstLot" for call in paper_trading.calls)


def test_runtime_service_still_writes_symbol_strategy_state_when_no_new_bar_blocks_execution() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("symbols").document("AAPL").collection("state").document("current").set(
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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "skipped_duplicate"
    assert result.execution_allowed is False
    account_state = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]["state"]["current"]
    symbol_state = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]["symbols"]["AAPL"]["state"]["current"]
    assert account_state["setup_state"] == "Valid"
    assert account_state["confirmation_state"] == "Yes"
    assert account_state["trigger_state"] == "Buy"
    assert account_state["final_decision"] == "Wait"
    assert symbol_state["candidate_action"] == "FirstLot"
    assert symbol_state["execution_decision"] == "skipped_duplicate"
    assert symbol_state["last_checked_at"] is not None


def test_runtime_service_suppresses_duplicate_firstlot_when_symbol_state_execution_key_matches() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("symbols").document("USO").collection("state").document("current").set(
        {
            "execution_key": f"FirstLot:USO:{latest_signal_time}",
            "last_processed_bar_time": latest_signal_time,
            "latest_candidate_action": "FirstLot",
            "latest_status": "signal",
            "latest_execution_decision": "submitted_buy",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(
        symbol="USO",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "skipped_duplicate"
    assert result.execution_allowed is False
    assert result.order_submitted is False
    assert all(call["action"] != "FirstLot" for call in paper_trading.calls)


def test_runtime_service_suppresses_duplicate_firstlot_when_recent_alpaca_order_matches_same_symbol_and_bar() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading(
        recent_orders=[
            {
                "symbol": "USO",
                "side": "buy",
                "type": "market",
                "status": "accepted",
                "created_at": _bars()[-1].ends_at.isoformat(),
            }
        ]
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(
        symbol="USO",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "skipped_duplicate"
    assert result.execution_allowed is False
    assert result.order_submitted is False
    assert any(call["action"] == "recent_orders" for call in paper_trading.calls)


def test_runtime_service_skips_when_same_bar_is_reprocessed() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("symbols").document("AAPL").collection("state").document("current").set(
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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI"),
    )

    result = service.run_once(
        symbol="AAPL",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.order_submitted is True
    assert any(call["action"] == "MULTI-1" for call in paper_trading.calls)


def test_runtime_service_uses_symbol_state_to_allow_first_evaluation_for_new_symbol() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    latest_signal_time = _bars()[-1].ends_at.isoformat()
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("slots").document("slot_1").collection("state").document("current").set(
        {
            "last_processed_bar_time": latest_signal_time,
            "latest_candidate_action": "FirstLot",
            "latest_status": "ok",
            "latest_execution_decision": "submitted_buy",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(
        symbol="FRO",
        uid="user-a",
        account_id=101,
        alpaca_account_id=501,
        allow_execution=True,
        trigger_type="scheduled",
        trigger_source="cloud_scheduler",
    )

    assert result.execution_decision == "submitted_buy"
    assert result.order_submitted is True
    assert any(call["action"] == "FirstLot" for call in paper_trading.calls)
    symbol_state = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]["symbols"]["FRO"]["state"]["current"]
    assert symbol_state["last_processed_bar_time"] == _bars()[-1].ends_at.isoformat()


def test_runtime_service_blocks_when_market_data_is_stale() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    stale_now = datetime.now(tz=UTC)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeStaleMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
        now_provider=lambda: stale_now,
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "blocked"
    assert result.execution_decision == "stale_data"
    assert result.execution_allowed is False
    assert result.skipped_reason == "stale_data"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["state"]["current"]["run_id"] == result.run_id
    assert root["state"]["current"]["latest_execution_decision"] == "stale_data"
    assert root["execution"]["current"]["execution_decision"] == "stale_data"
    assert root["execution"]["current"]["blocked_reason"] == "stale_data"


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
        account_resolver=FakeAccountResolver(),
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
        account_resolver=FakeAccountResolver(),
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
        account_resolver=FakeAccountResolver(),
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
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True, trigger_type="scheduled", trigger_source="cloud_scheduler")

    assert result.status == "blocked"
    assert result.execution_decision == "market_data_unavailable"
    assert result.execution_allowed is False
    assert result.skipped_reason == "market_data_unavailable"
    assert result.order_submitted is False
    assert result.trigger_type == "scheduled"
    assert result.trigger_source == "cloud_scheduler"
    assert "Alpaca market data could not be fetched after runtime config load" in result.message


def test_runtime_service_routes_paper_account_credentials_into_market_data_and_execution() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {"account_id": 101, "alpaca_account_id": 501}
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    paper_trading = FakePaperTrading()
    account_context = _account_context(environment="paper", trade_enabled=True)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(context=account_context),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.mode == "paper"
    assert market_data.calls[0]["credential_context"] == account_context
    assert paper_trading.calls[0]["credential_context"] == account_context
    assert result.execution_allowed is True


def test_runtime_service_routes_live_account_credentials_into_market_data_and_execution() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=False,
        prime_stocks_live_execution_enabled=True,
    )
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {"account_id": 101, "alpaca_account_id": 502}
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    paper_trading = FakePaperTrading()
    account_context = _account_context(environment="live", trade_enabled=True)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(context=account_context),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.mode == "live"
    assert market_data.calls[0]["credential_context"] == account_context
    assert paper_trading.calls[0]["credential_context"] == account_context
    assert result.execution_allowed is True


def test_runtime_service_allows_request_level_account_selector_overrides() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {"account_id": 101, "alpaca_account_id": 501}
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    resolver = FakeAccountResolver(context=_account_context(environment="paper", trade_enabled=True))
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=resolver,
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    service.run_once(account_id=222, alpaca_account_id=333, allow_execution=False)

    assert resolver.runtime_configs[0].account_id == 222
    assert resolver.runtime_configs[0].alpaca_account_id == 333


def test_runtime_service_applies_runtime_config_to_market_data_and_strategy_runner() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {
            "execution_timeframe": "1hour",
            "trend_timeframe": "day",
            "pullback_window": 27,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    captured: dict[str, object] = {}

    def strategy_runner(**kwargs):
        captured.update(kwargs)
        return _strategy_result("HOLD")

    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=strategy_runner,
    )

    result = service.run_once(symbol="AAPL", allow_execution=False)

    assert result.status == "no_signal"
    assert market_data.calls[0]["execution_timeframe"] == "15M"
    assert market_data.calls[0]["trend_timeframe"] == "1D"
    assert captured["config"].execution_timeframe == "15M"
    assert captured["config"].trend_timeframe == "1D"
    assert captured["config"].swing_len == 27


def test_runtime_service_passes_loaded_runtime_state_into_strategy_runner() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "position_open": True,
            "position_size": 2.0,
            "position_avg_price": 101.0,
            "dollars_used": 174.0,
            "add_count": 2,
            "last_add_price": 99.0,
            "pos_high": 107.0,
            "trail_stop": 103.0,
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    captured: dict[str, object] = {}

    def strategy_runner(**kwargs):
        captured.update(kwargs)
        return _strategy_result("HOLD", final_state=kwargs["initial_state"])

    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=strategy_runner,
    )

    result = service.run_once(symbol="AAPL", allow_execution=False)

    assert result.execution_decision == "dry_run_only"
    assert captured["initial_state"].position_size == 2.0
    assert captured["initial_state"].add_count == 2


def test_runtime_service_persists_runtime_state_after_first_lot_buy() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **kwargs: _strategy_result("FirstLot", final_state=kwargs["initial_state"]),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    root = fake_client.storage["runtime_products"]["prime_stocks"]
    state = root["state"]["current"]
    assert result.execution_decision == "submitted_buy"
    assert state["position_open"] is True
    assert state["position_size"] > 0.0
    assert state["dollars_used"] == 300.0
    assert state["add_count"] == 0
    assert state["last_entry_time"] == _bars()[-1].ends_at.isoformat()
    assert state["latest_execution_decision"] == "submitted_buy"
    assert state["execution_key"] == f"FirstLot:AAPL:{_bars()[-1].ends_at.isoformat()}"


def test_runtime_service_persists_runtime_state_after_add() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "position_open": True,
            "position_size": 1.0,
            "position_avg_price": 101.0,
            "dollars_used": 101.0,
            "add_count": 1,
            "add_tiers_filled": [1],
            "last_add_price": 101.0,
            "last_entry_time": "2026-04-09T10:00:00+00:00",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    state_before = BismillahTrobotStocksV1State(
        add_count=1,
        last_add_price=101.0,
        dollars_used=101.0,
        position_avg_price=101.0,
        position_size=1.0,
    )
    final_state = BismillahTrobotStocksV1State(
        add_count=2,
        last_add_price=99.0,
        dollars_used=174.0,
        pos_high=107.0,
        trail_stop=103.0,
        position_avg_price=100.0,
        position_size=1.7,
    )
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(
            submission_state=AlpacaPaperSubmissionState(
                account=AlpacaPaperAccountState(
                    buying_power=1000.0,
                    open_positions_count=1,
                    equity=10000.0,
                    total_exposure=101.0,
                ),
                asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
                position=AlpacaPaperPositionState(symbol="AAPL", qty=1.0, market_value=101.0),
            )
        ),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("MULTI-2", state_before=state_before, final_state=final_state),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    state = fake_client.storage["runtime_products"]["prime_stocks"]["state"]["current"]
    assert result.execution_decision == "submitted_buy"
    assert state["position_open"] is True
    assert state["add_count"] == 2
    assert state["add_tiers_filled"] == [1, 2]
    assert state["last_add_price"] is not None
    assert state["last_entry_time"] == "2026-04-09T10:00:00+00:00"


def test_runtime_service_resets_runtime_state_after_exit() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "position_open": True,
            "position_size": 2.0,
            "position_avg_price": 101.0,
            "dollars_used": 174.0,
            "add_count": 2,
            "add_tiers_filled": [1, 2],
            "last_entry_time": "2026-04-09T10:00:00+00:00",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(
            submission_state=AlpacaPaperSubmissionState(
                account=AlpacaPaperAccountState(
                    buying_power=1000.0,
                    open_positions_count=1,
                    equity=10000.0,
                    total_exposure=101.0,
                ),
                asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
                position=AlpacaPaperPositionState(symbol="AAPL", qty=2.0, market_value=202.0),
            )
        ),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **kwargs: _strategy_result("EXIT_ATR", final_state=kwargs["initial_state"]),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    state = fake_client.storage["runtime_products"]["prime_stocks"]["state"]["current"]
    assert result.execution_decision == "submitted_exit"
    assert state["position_open"] is False
    assert state["position_size"] == 0.0
    assert state["position_avg_price"] is None
    assert state["dollars_used"] == 0.0
    assert state["add_count"] == 0
    assert state["last_exit_time"] == _bars()[-1].ends_at.isoformat()


def test_runtime_service_updates_state_fields_on_no_signal() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("state").document("current").set(
        {
            "position_open": True,
            "position_size": 1.0,
            "position_avg_price": 101.0,
            "dollars_used": 101.0,
            "add_count": 1,
            "last_entry_time": "2026-04-09T10:00:00+00:00",
        }
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **kwargs: _strategy_result("HOLD", final_state=kwargs["initial_state"]),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    state = fake_client.storage["runtime_products"]["prime_stocks"]["state"]["current"]
    assert result.execution_decision == "no_op"
    assert state["position_open"] is True
    assert state["latest_signal_time"] == _bars()[-1].ends_at.isoformat()
    assert state["last_processed_bar_time"] == _bars()[-1].ends_at.isoformat()
    assert state["latest_execution_decision"] == "no_op"


def test_runtime_service_blocks_when_linked_account_resolution_fails() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(error="missing linked account"),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "blocked"
    assert result.execution_decision == "linked_account_unavailable"
    assert result.execution_allowed is False
    assert result.skipped_reason == "linked_account_unavailable"


def test_runtime_service_blocks_execution_when_selected_account_is_not_trade_enabled() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {"account_id": 101, "alpaca_account_id": 503}
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(context=_account_context(environment="paper", trade_enabled=False)),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "signal"
    assert result.execution_decision == "credential_trade_access_missing"
    assert result.execution_allowed is False
    assert result.skipped_reason == "credential_trade_access_missing"
    assert paper_trading.calls == []


def test_runtime_service_allows_new_entry_when_ai_cache_is_risk_on_bullish_and_safe() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(
        fake_client,
        market_regime="risk_on",
        market_sentiment="bullish",
        market_safety="safe",
        symbol_regime="risk_on",
        symbol_sentiment="bullish",
        symbol_safety="safe",
    )
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.candidate_action == "FirstLot"
    assert result.ai is not None
    assert result.ai["Ai_regime_label"] == "risk_on"
    assert result.ai["Ai_sentiment_label"] == "bullish"
    assert result.ai["Ai_safety_label"] == "safe"
    assert result.ai["Ai_execution_allowed"] is True
    assert result.ai["Ai_blocked_reason"] is None
    assert paper_trading.calls[-1]["action"] == "FirstLot"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["snapshots"]["latest"]["ai"]["Ai_execution_allowed"] is True
    assert root["execution"]["current"]["execution_allowed"] is True
    assert root["execution"]["current"]["blocked_reason"] is None
    assert root["actions"]["latest"]["candidate_action"] == "FirstLot"


def test_runtime_service_allows_new_entry_when_ai_cache_is_bearish() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, symbol_sentiment="bearish")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "signal"
    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.ai is not None
    assert result.ai["Ai_blocked_reason"] is None
    assert result.ai["Ai_execution_allowed"] is True
    assert paper_trading.calls[-1]["action"] == "FirstLot"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["snapshots"]["latest"]["ai"]["Ai_sentiment_label"] == "bearish"
    assert root["execution"]["current"]["blocked_reason"] is None
    assert root["execution"]["current"]["ai"]["Ai_blocked_reason"] is None


def test_runtime_service_blocks_new_entry_when_ai_cache_is_unsafe() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, symbol_safety="unsafe")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "blocked"
    assert result.execution_decision == "ai_safety_unsafe"
    assert result.execution_allowed is False
    assert result.ai is not None
    assert result.ai["Ai_safety_label"] == "unsafe"
    assert result.ai["Ai_execution_allowed"] is False
    assert result.ai["Ai_blocked_reason"] == "ai_safety_unsafe"
    assert paper_trading.calls == []
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["blocked_reason"] == "ai_safety_unsafe"
    assert root["execution"]["current"]["ai"]["Ai_blocked_reason"] == "ai_safety_unsafe"


def test_runtime_service_allows_new_entry_when_ai_cache_is_risk_off() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, market_regime="risk_off")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "signal"
    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.ai is not None
    assert result.ai["Ai_blocked_reason"] is None
    assert result.ai["Ai_execution_allowed"] is True
    assert paper_trading.calls[-1]["action"] == "FirstLot"


def test_runtime_service_allows_exit_when_market_ai_cache_is_risk_off() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, market_regime="risk_off")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("EXIT_ATR"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_exit"
    assert result.execution_allowed is True
    assert result.order_submitted is True
    assert result.candidate_action == "EXIT_ATR"
    assert result.ai is not None
    assert result.ai["Ai_regime_label"] == "risk_off"
    assert result.ai["Ai_blocked_reason"] is None
    assert paper_trading.calls[-1]["action"] == "EXIT_ATR"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["execution_decision"] == "submitted_exit"
    assert root["execution"]["current"]["execution_allowed"] is True


def test_runtime_service_allows_exit_when_symbol_ai_cache_is_unsafe() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, symbol_safety="unsafe")
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("EXIT_REGIME"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.execution_decision == "submitted_exit"
    assert result.execution_allowed is True
    assert result.order_submitted is True
    assert result.candidate_action == "EXIT_REGIME"
    assert result.ai is not None
    assert result.ai["Ai_safety_label"] == "unsafe"
    assert result.ai["Ai_blocked_reason"] == "ai_safety_unsafe"
    assert paper_trading.calls[-1]["action"] == "EXIT_REGIME"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["execution_decision"] == "submitted_exit"
    assert root["execution"]["current"]["execution_allowed"] is True


def test_runtime_service_allows_when_ai_cache_is_stale() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True, ai_cache_max_age_minutes=60)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client, age_hours=27)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "signal"
    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.ai is not None
    assert result.ai["is_stale"] is True
    assert result.ai["Ai_blocked_reason"] is None
    assert paper_trading.calls[-1]["action"] == "FirstLot"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["execution_decision"] == "submitted_buy"
    assert root["execution"]["current"]["blocked_reason"] is None


def test_runtime_service_allows_when_ai_cache_is_unavailable() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    del fake_client.storage["runtime_products"]["prime_stocks"]["ai_market"]["current"]
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    paper_trading = FakePaperTrading()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=paper_trading,
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=True)

    assert result.status == "signal"
    assert result.execution_decision == "submitted_buy"
    assert result.execution_allowed is True
    assert result.ai is not None
    assert result.ai["Ai_blocked_reason"] is None
    assert result.ai["is_available"] is False
    assert result.ai["is_stale"] is False
    assert paper_trading.calls[-1]["action"] == "FirstLot"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["blocked_reason"] is None
    assert root["execution"]["current"]["execution_decision"] == "submitted_buy"
    assert root["execution"]["current"]["skipped_reason"] is None


def test_runtime_service_uses_ai_cache_without_direct_gemini_runtime_dependency() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    _seed_ai_cache(fake_client)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(symbol="AAPL", allow_execution=False)

    assert result.ai is not None
    assert result.ai["Ai_source"] == "cached_gemini"


def test_runtime_service_validation_ping_uses_test_mode_symbol_override_and_persists_visibility() -> None:
    settings = _settings(
        prime_stocks_test_mode=True,
        prime_stocks_test_trigger="ping",
        prime_stocks_test_symbol_override="SHIBUSD",
    )
    fake_client = FakeFirestoreClient()
    _seed_ping_runtime_config(fake_client)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    market_data = FakeMarketData()
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=market_data,
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(allow_execution=False, trigger_type="ping", trigger_source="api", test_trigger="ping")

    assert result.symbol == "SHIBUSD"
    assert result.asset_type == "crypto"
    assert result.execution_decision == "validation_ping_ok"
    assert result.order_submitted is False
    assert market_data.calls[0]["symbol"] == "SHIBUSD"
    assert market_data.calls[0]["asset_type"] == "crypto"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["state"]["current"]["test_mode"] is True
    assert root["state"]["current"]["test_trigger"] == "ping"
    assert root["state"]["current"]["symbol_override"] == "SHIBUSD"
    assert root["state"]["current"]["validation_only"] is True
    assert root["execution"]["current"]["execution_decision"] == "validation_ping_ok"
    assert root["heartbeat"]["current"]["status"] == "ok"
    assert root["heartbeat"]["current"]["test_mode"] is True
    assert root["heartbeat"]["current"]["run_id"] == result.run_id


def test_runtime_service_validation_ping_requires_test_mode() -> None:
    settings = _settings(prime_stocks_test_mode=False, prime_stocks_test_trigger=None, prime_stocks_test_symbol_override=None)
    fake_client = FakeFirestoreClient()
    _seed_ping_runtime_config(fake_client, test_mode=False)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    try:
        service.run_once(allow_execution=False, trigger_type="ping", trigger_source="api", test_trigger="ping")
    except ValueError as exc:
        assert str(exc) == "Prime Stocks validation ping trigger is disabled because test_mode is off."
    else:
        raise AssertionError("Expected validation ping to require test_mode.")


def test_runtime_service_validation_ping_keeps_ai_cache_stale_as_advisory() -> None:
    settings = _settings(
        prime_stocks_test_mode=True,
        prime_stocks_test_trigger="ping",
        prime_stocks_test_symbol_override="SHIBUSD",
        ai_cache_max_age_minutes=60,
    )
    fake_client = FakeFirestoreClient()
    _seed_ping_runtime_config(fake_client)
    _seed_ai_cache(fake_client, age_hours=27)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(allow_execution=False, trigger_type="ping", trigger_source="api", test_trigger="ping")

    assert result.execution_decision == "validation_ping_ok"
    assert result.ai is not None
    assert result.ai["Ai_execution_allowed"] is True
    assert result.ai["Ai_blocked_reason"] is None
    assert result.ai["is_stale"] is True
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["execution_decision"] == "validation_ping_ok"
    assert root["execution"]["current"]["ai"]["Ai_execution_allowed"] is True
    assert root["execution"]["current"]["ai"]["Ai_blocked_reason"] is None


def test_runtime_service_validation_ping_bypasses_market_data_stale_block() -> None:
    settings = _settings(
        prime_stocks_test_mode=True,
        prime_stocks_test_trigger="ping",
        prime_stocks_test_symbol_override="SHIBUSD",
    )
    fake_client = FakeFirestoreClient()
    _seed_ping_runtime_config(fake_client)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeStaleMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
        now_provider=lambda: datetime.now(tz=UTC),
    )

    result = service.run_once(allow_execution=False, trigger_type="ping", trigger_source="api", test_trigger="ping")

    assert result.execution_decision == "validation_ping_ok"
    assert "market-data stale bypass active" in result.message
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["execution"]["current"]["execution_decision"] == "validation_ping_ok"
    assert root["execution"]["current"]["test_mode"] is True


def test_runtime_service_scheduled_ping_returns_clean_disabled_result_when_admin_ping_is_off() -> None:
    settings = _settings(
        prime_stocks_test_mode=True,
        prime_stocks_test_trigger="ping",
        prime_stocks_test_symbol_override="SHIBUSD",
    )
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(allow_execution=False, trigger_type="ping", trigger_source="cloud_scheduler", test_trigger="ping")

    assert result.execution_decision == "validation_ping_disabled"
    assert result.candidate_action == "PING_OFF"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert "state" not in root
    assert "execution" not in root
    assert "logs" not in root


def test_runtime_service_scheduled_ping_can_write_daily_heartbeat_only_when_ping_is_off() -> None:
    settings = _settings(
        prime_stocks_test_mode=True,
        prime_stocks_test_trigger="ping",
        prime_stocks_test_symbol_override="SHIBUSD",
    )
    fake_client = FakeFirestoreClient()
    _seed_ping_runtime_config(fake_client, ping_enabled=False, ping_mode="off", ping_daily_heartbeat_enabled=True)
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(allow_execution=False, trigger_type="ping", trigger_source="cloud_scheduler", test_trigger="ping")

    assert result.execution_decision == "validation_ping_disabled"
    root = fake_client.storage["runtime_products"]["prime_stocks"]
    assert root["heartbeat"]["current"]["status"] == "ok"
    assert "state" not in root
    assert "execution" not in root


class FakeMarketData:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def fetch_prime_stocks_bars(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        execution_timeframe: str | None = None,
        trend_timeframe: str | None = None,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> PrimeStocksBarSet:
        self.calls.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "product_key": product_key,
                "execution_timeframe": execution_timeframe,
                "trend_timeframe": trend_timeframe,
                "execution_limit": execution_limit,
                "trend_limit": trend_limit,
                "credential_context": credential_context,
            }
        )
        return PrimeStocksBarSet(
            symbol=symbol,
            execution_bars=_bars(),
            trend_bars=_bars(),
        )


class FailingMarketData:
    def fetch_prime_stocks_bars(self, **kwargs) -> PrimeStocksBarSet:
        del kwargs
        raise RuntimeError("Alpaca credentials are required for Prime Stocks market-data fetches.")


class FakeStaleMarketData(FakeMarketData):
    def fetch_prime_stocks_bars(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        execution_timeframe: str | None = None,
        trend_timeframe: str | None = None,
        execution_limit: int | None = None,
        trend_limit: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> PrimeStocksBarSet:
        self.calls.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "product_key": product_key,
                "execution_timeframe": execution_timeframe,
                "trend_timeframe": trend_timeframe,
                "execution_limit": execution_limit,
                "trend_limit": trend_limit,
                "credential_context": credential_context,
            }
        )
        return PrimeStocksBarSet(
            symbol=symbol,
            execution_bars=_bars(age_hours=96),
            trend_bars=_bars(age_hours=96),
        )


class FakePaperTrading:
    def __init__(
        self,
        *,
        submission_state: AlpacaPaperSubmissionState | None = None,
        submit_first_lot_error: AlpacaPaperTradingError | None = None,
        submit_first_lot_result: AlpacaPaperExecutionResult | None = None,
        submit_first_lot_failures_before_success: int = 0,
        recent_orders: list[dict[str, object]] | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.submission_state = submission_state or AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=1000.0,
                open_positions_count=0,
                equity=10000.0,
                total_exposure=0.0,
            ),
            asset=AlpacaPaperAssetState(symbol="AAPL", tradable=True, status="active"),
            position=None,
        )
        self.submit_first_lot_error = submit_first_lot_error
        self.submit_first_lot_result = submit_first_lot_result
        self.submit_first_lot_failures_before_success = submit_first_lot_failures_before_success
        self.submission_state_calls = 0
        self.recent_orders = recent_orders or []

    def submit_first_lot_buy(self, **kwargs) -> AlpacaPaperExecutionResult:
        self.calls.append({"action": "FirstLot", **kwargs})
        if self.submit_first_lot_failures_before_success > 0:
            self.submit_first_lot_failures_before_success -= 1
            raise AlpacaPaperTradingError(
                code="broker_api_timeout",
                message="simulated transient timeout",
                retryable=True,
            )
        if self.submit_first_lot_error is not None:
            raise self.submit_first_lot_error
        if self.submit_first_lot_result is not None:
            return self.submit_first_lot_result
        return AlpacaPaperExecutionResult(
            action="FirstLot",
            submitted=True,
            order_status="accepted",
            order_id="order-first",
            client_order_id=str(kwargs["client_order_id"]),
            side="buy",
            notional=float(kwargs["notional"]),
            add_tier=None,
        )

    def submit_multi_buy(self, **kwargs) -> AlpacaPaperExecutionResult:
        self.calls.append({"action": kwargs.get("action", "MULTI"), **kwargs})
        return AlpacaPaperExecutionResult(
            action=str(kwargs.get("action", "MULTI")),
            submitted=True,
            order_status="accepted",
            order_id="order-multi",
            client_order_id=str(kwargs["client_order_id"]),
            side="buy",
            notional=float(kwargs["notional"]),
            add_tier=kwargs.get("add_tier"),
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
            add_tier=None,
        )

    def get_submission_state(self, **kwargs) -> AlpacaPaperSubmissionState:
        self.submission_state_calls += 1
        self.calls.append({"action": "submission_state", **kwargs})
        return self.submission_state

    def list_recent_orders(self, **kwargs) -> list[dict[str, object]]:
        self.calls.append({"action": "recent_orders", **kwargs})
        return list(self.recent_orders)


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
        _seed_ai_cache(self)

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


def _bars(*, age_hours: int = 11) -> list[PriceBar]:
    start = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=age_hours)
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


def _strategy_result(
    candidate_action: str,
    *,
    in_position_before: bool | None = None,
    state_before: BismillahTrobotStocksV1State | None = None,
    final_state: BismillahTrobotStocksV1State | None = None,
) -> PrimeStocksStrategyResult:
    base_entry_trigger = candidate_action == "FirstLot"
    add_trigger = candidate_action.startswith("MULTI")
    hit_atr_trail = candidate_action == "EXIT_ATR"
    hit_regime = candidate_action == "EXIT_REGIME"
    if in_position_before is None:
        in_position_before = add_trigger or hit_atr_trail or hit_regime
    state = state_before or BismillahTrobotStocksV1State()
    if add_trigger and state_before is None:
        try:
            state.add_count = max(0, int(candidate_action.split("-", maxsplit=1)[1]) - 1)
        except (IndexError, ValueError):
            state.add_count = 0
    resolved_final_state = final_state or state
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
        in_position_before=in_position_before,
        signal=signal,
        state_before=state,
        state_after=resolved_final_state,
    )
    evaluation = PineSignalStateEvaluation(
        series=PineComputedSeries(),
        bars=[latest_bar],
        final_state=resolved_final_state,
    )
    return PrimeStocksStrategyResult(
        product_key="stocks.bismel1",
        pine_strategy_title="Prime Stocks Bot Trader",
        status="exit" if hit_atr_trail or hit_regime else "signal" if base_entry_trigger or add_trigger else "no_signal",
        message="stub strategy result",
        series=evaluation.series,
        latest_signal=signal,
        latest_bar=latest_bar,
        final_state=resolved_final_state,
        ai_decision=None,
        execution_allowed=base_entry_trigger or add_trigger or hit_atr_trail or hit_regime,
        execution_timeframe="15M",
        trend_timeframe="1D",
    )


class FakeAccountResolver:
    def __init__(
        self,
        context: ResolvedAlpacaAccountContext | None = None,
        error: str | None = None,
    ) -> None:
        self.context = context or _account_context()
        self.error = error
        self.runtime_configs: list[object] = []

    def resolve_runtime_account(self, runtime_config) -> ResolvedAlpacaAccountContext:
        self.runtime_configs.append(runtime_config)
        assert runtime_config.account_id is None or isinstance(runtime_config.account_id, int)
        assert runtime_config.alpaca_account_id is None or isinstance(runtime_config.alpaca_account_id, int)
        if self.error is not None:
            raise AlpacaAccountResolutionError(self.error)
        return self.context


def test_runtime_store_applies_global_admin_controls_before_scoped_runtime_fields() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {
            "enabled": False,
            "paper_execution_enabled": False,
            "live_execution_enabled": False,
            "test_mode": True,
            "ping_enabled": True,
            "ping_mode": "gauge",
            "test_symbol_override": "SHIBUSD",
            "force_candidate_action": "FirstLot",
            "ai_validation_bypass_enabled": True,
        },
        merge=True,
    )
    fake_client.collection("users").document("user-a").collection("accounts").document("101").collection("prime_stocks").document("current").collection("config").document("current").set(
        {
            "enabled": True,
            "paper_execution_enabled": True,
            "live_execution_enabled": True,
            "test_mode": False,
            "ping_enabled": False,
            "ping_mode": "off",
            "test_symbol_override": None,
            "force_candidate_action": None,
            "ai_validation_bypass_enabled": False,
        },
        merge=True,
    )

    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    runtime_config = runtime_store.load_runtime_config(
        replace(build_default_runtime_config(settings), uid="user-a", account_id=101)
    )

    assert runtime_config.enabled is False
    assert runtime_config.paper_execution_enabled is False
    assert runtime_config.live_execution_enabled is False
    assert runtime_config.test_mode is True
    assert runtime_config.ping_enabled is True
    assert runtime_config.ping_mode == "gauge"
    assert runtime_config.test_symbol_override == "SHIBUSD"
    assert runtime_config.force_candidate_action == "FirstLot"
    assert runtime_config.ai_validation_bypass_enabled is True


def test_runtime_service_persists_linked_account_blocked_path() -> None:
    settings = _settings()
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(error="bridge down"),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501)

    state_payload = (
        fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["state"]["current"]
    )
    assert result.execution_decision == "linked_account_unavailable"
    assert state_payload["latest_execution_decision"] == "linked_account_unavailable"
    assert state_payload["last_error_code"] == "linked_account_unavailable"


def test_runtime_service_writes_account_scoped_audit_documents() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("HOLD"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, allow_execution=False)

    scoped_root = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]
    signal_audit = scoped_root["audit_signals"][result.run_id]
    action_audit = scoped_root["audit_actions"][result.run_id]
    order_audit = scoped_root["audit_orders"][result.run_id]

    assert signal_audit["candidate_action"] == "HOLD"
    assert action_audit["execution_decision"] == "dry_run_only"
    assert order_audit["order_status"] == "not_submitted"
    assert order_audit["submitted"] is False


def test_runtime_service_writes_notification_for_submitted_buy() -> None:
    settings = _settings(prime_stocks_dry_run=False, prime_stocks_paper_execution_enabled=True)
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    result = service.run_once(uid="user-a", account_id=101, alpaca_account_id=501, allow_execution=True)

    notification = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]["notifications"][result.run_id]
    assert notification["event_type"] == "submitted_buy"
    assert notification["candidate_action"] == "FirstLot"
    assert notification["delivery"]["status"] == "written"


def test_runtime_service_prime_ignores_global_kill_switch_for_duplicate_entry_flow() -> None:
    settings = _settings(
        prime_stocks_dry_run=False,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_global_kill_switch_enabled=True,
    )
    fake_client = FakeFirestoreClient()
    runtime_store = PrimeStocksFirestoreRuntimeStore(settings=settings, client=fake_client)
    service = PrimeStocksRuntimeService(
        settings=settings,
        market_data=FakeMarketData(),
        runtime_store=runtime_store,
        paper_trading=FakePaperTrading(),
        account_resolver=FakeAccountResolver(),
        strategy_runner=lambda **_: _strategy_result("FirstLot"),
    )

    first = service.run_once(symbol="AAPL", uid="user-a", account_id=101, alpaca_account_id=501, allow_execution=True)
    second = service.run_once(symbol="AAPL", uid="user-a", account_id=101, alpaca_account_id=501, allow_execution=True)

    scoped_root = fake_client.storage["users"]["user-a"]["accounts"]["101"]["prime_stocks"]["current"]["slots"]["slot_1"]
    assert first.execution_decision == "submitted_buy"
    assert second.execution_decision in {"skipped_duplicate", "submitted_buy"}
    assert len(scoped_root["notifications"]) == 1


def _account_context(
    *,
    environment: str = "paper",
    trade_enabled: bool = True,
    entitlement: dict[str, object] | None = None,
) -> ResolvedAlpacaAccountContext:
    return ResolvedAlpacaAccountContext(
        uid="user-a",
        account_id=101,
        alpaca_account_id=501 if environment == "paper" else 502,
        broker_connection_id=301,
        broker_credential_id=401,
        environment=environment,
        data_feed="iex",
        access_mode="trade" if trade_enabled else "trade_disabled",
        trade_enabled=trade_enabled,
        key_id="resolved-key",
        secret="resolved-secret",
        slot_number=1,
        entitlement=entitlement
        or {
            "product_key": "stocks.bismel1",
            "enabled": True,
            "runtime_allowed": True,
            "paper_available": True,
            "live_available": environment == "live",
        },
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
        laravel_runtime_bridge_url="https://bismel1.test",
        laravel_runtime_bridge_token="bridge-token",
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_live_trading_base_url="https://api.alpaca.markets",
        alpaca_api_key_id="key-123",
        alpaca_api_secret="secret-123",
        alpaca_data_feed="iex",
        gemini_model="gemini-2.5-flash-lite",
        ai_cache_max_age_minutes=360,
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=False,
        prime_stocks_live_execution_enabled=False,
        prime_stocks_ai_validation_bypass_enabled=False,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_test_mode=False,
        prime_stocks_test_trigger=None,
        prime_stocks_test_symbol_override=None,
        prime_stocks_execution_bar_limit=351,
        prime_stocks_trend_bar_limit=221,
        prime_stocks_first_lot_notional=101.0,
        prime_stocks_multi_notional=73.0,
        prime_stocks_max_notional_per_order=303.0,
        prime_stocks_max_total_notional_per_symbol=707.0,
        prime_stocks_max_add_count=2,
        prime_stocks_daily_order_cap=None,
        prime_stocks_max_open_positions=None,
        prime_stocks_broker_retry_max_attempts=1,
        prime_stocks_force_candidate_action=None,
        prime_stocks_scheduler_job_name="prime-stocks-scheduled",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="5 * * * 1-5",
        prime_stocks_scheduler_timezone="Etc/UTC",
        prime_stocks_scheduler_header_name="X-Prime-Stocks-Scheduler",
        prime_stocks_scheduler_header_value="secret-value",
        prime_stocks_ping_scheduler_job_name="prime-stocks-ping",
        prime_stocks_ping_scheduler_schedule="*/1 * * * *",
        prime_stocks_ping_scheduler_timezone="Etc/UTC",
        prime_stocks_ping_scheduler_header_value="ping-secret-value",
    )
    base.update(overrides)
    return AppConfig(**base)

def _seed_ping_runtime_config(
    fake_client: FakeFirestoreClient,
    *,
    ping_enabled: bool = True,
    ping_mode: str = "gauge",
    ping_daily_heartbeat_enabled: bool = False,
    test_mode: bool = True,
    test_trigger: str | None = "ping",
    test_symbol_override: str | None = "SHIBUSD",
) -> None:
    fake_client.collection("runtime_products").document("prime_stocks").collection("config").document("current").set(
        {
            "ping_enabled": ping_enabled,
            "ping_mode": ping_mode,
            "ping_daily_heartbeat_enabled": ping_daily_heartbeat_enabled,
            "test_mode": test_mode,
            "test_trigger": test_trigger,
            "test_symbol_override": test_symbol_override,
        },
        merge=True,
    )


def _seed_ai_cache(
    fake_client: FakeFirestoreClient,
    *,
    age_hours: int = 1,
    market_regime: str = "risk_on",
    market_sentiment: str = "neutral",
    market_safety: str = "safe",
    symbol_regime: str = "neutral",
    symbol_sentiment: str = "bullish",
    symbol_safety: str = "safe",
) -> None:
    updated_at = (datetime.now(tz=UTC) - timedelta(hours=age_hours)).isoformat()
    fake_client.collection("runtime_products").document("prime_stocks").collection("ai_market").document("current").set(
        {
            "scope": "market",
            "symbol": None,
            "Ai_regime_label": market_regime,
            "Ai_sentiment_label": market_sentiment,
            "Ai_safety_label": market_safety,
            "Ai_confidence": 0.84,
            "Ai_reason": "Market cache reason.",
            "Ai_updated_at": updated_at,
            "Ai_source": "gemini:test",
            "Ai_execution_allowed": market_safety != "unsafe",
            "Ai_block_new_entries": market_safety == "unsafe",
            "Ai_block_adds": market_safety == "unsafe",
            "Ai_blocked_reason": "ai_safety_unsafe" if market_safety == "unsafe" else None,
        }
    )
    fake_client.collection("runtime_products").document("prime_stocks").collection("ai_symbols").document("AAPL").set(
        {
            "scope": "symbol",
            "symbol": "AAPL",
            "Ai_regime_label": symbol_regime,
            "Ai_sentiment_label": symbol_sentiment,
            "Ai_safety_label": symbol_safety,
            "Ai_confidence": 0.88,
            "Ai_reason": "Symbol cache reason.",
            "Ai_updated_at": updated_at,
            "Ai_source": "gemini:test",
            "Ai_execution_allowed": symbol_safety != "unsafe",
            "Ai_block_new_entries": symbol_safety == "unsafe",
            "Ai_block_adds": symbol_safety == "unsafe",
            "Ai_blocked_reason": "ai_safety_unsafe" if symbol_safety == "unsafe" else None,
        }
    )
