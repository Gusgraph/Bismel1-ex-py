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

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from typing import Callable

from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter
from app.brokers.alpaca_paper_trading import AlpacaPaperExecutionResult, AlpacaPaperTradingAdapter
from app.products.stocks.bismel1.config import BismillahTrobotStocksV1Config
from app.products.stocks.bismel1.strategy import run_prime_stocks_strategy
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
)
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksLatestExecutionRecord,
    PrimeStocksRuntimeConfigRecord,
    PrimeStocksRuntimeStateRecord,
    PrimeStocksRuntimeStoreError,
    build_default_runtime_config,
)
from app.shared.config import AppConfig


logger = logging.getLogger(__name__)


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
    status: str
    message: str
    bars_processed_execution: int
    bars_processed_trend: int
    firestore_paths: dict[str, str]


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
        self._paper_trading = paper_trading or AlpacaPaperTradingAdapter(settings=settings)
        self._account_resolver = account_resolver or LaravelAlpacaAccountResolver(settings=settings)
        self._strategy_runner = strategy_runner or run_prime_stocks_strategy
        self._now_provider = now_provider or (lambda: datetime.now(tz=UTC))

    def run_once(
        self,
        symbol: str | None = None,
        account_id: int | None = None,
        alpaca_account_id: int | None = None,
        allow_execution: bool | None = None,
        trigger_type: str = "manual",
        trigger_source: str = "api",
    ) -> PrimeStocksRuntimeResult:
        run_id = self._runtime_store.create_run_id()
        default_runtime_config = build_default_runtime_config(self._settings)
        fallback_runtime_config = _override_symbol(default_runtime_config, symbol)
        try:
            runtime_config = self._runtime_store.load_runtime_config(default_runtime_config)
            resolved_runtime_config = _override_runtime_selection(
                runtime_config,
                symbol=symbol,
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
                firestore_paths=self._runtime_store.get_paths().__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore runtime control data could not be loaded. "
                    f"{exc}"
                ),
            )
        if not resolved_runtime_config.enabled:
            message = "Prime Stocks runtime skipped because the runtime config is disabled."
            disabled_result = PrimeStocksRuntimeResult(
                run_id=run_id,
                mode="dry-run",
                runtime_target=resolved_runtime_config.runtime_target,
                product_key=resolved_runtime_config.product_key,
                strategy_key=resolved_runtime_config.strategy_key,
                strategy_title=resolved_runtime_config.strategy_title,
                symbol=resolved_runtime_config.symbol,
                asset_type=resolved_runtime_config.asset_type,
                enabled=False,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                candidate_action="DISABLED",
                execution_decision="skipped",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="runtime_disabled",
                latest_signal_time=None,
                status="disabled",
                message=message,
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths=self._runtime_store.get_paths().__dict__,
            )
            logger.info(message)
            return disabled_result
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
            return _build_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths().__dict__,
                message=(
                    "Prime Stocks runtime blocked because the selected linked Alpaca account could not be resolved. "
                    f"{exc}"
                ),
                execution_decision="linked_account_unavailable",
                skipped_reason="linked_account_unavailable",
            )
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
            return _build_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths().__dict__,
                message=(
                    "Prime Stocks runtime blocked because Alpaca market data could not be fetched after runtime config load. "
                    f"{exc}"
                ),
                execution_decision="market_data_unavailable",
                skipped_reason="market_data_unavailable",
            )
        latest_signal_time = _resolve_latest_signal_time(bar_set.execution_bars)
        if _is_stale_market_data(
            latest_signal_time=latest_signal_time,
            execution_timeframe=resolved_runtime_config.execution_timeframe,
            now=self._now_provider(),
        ):
            return _build_blocked_runtime_result(
                run_id=run_id,
                runtime_config=resolved_runtime_config,
                allow_execution=allow_execution,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                firestore_paths=self._runtime_store.get_paths().__dict__,
                message=(
                    "Prime Stocks runtime blocked because the latest Alpaca execution bar is stale for the configured "
                    f"{resolved_runtime_config.execution_timeframe} timeframe."
                ),
                latest_signal_time=latest_signal_time,
                execution_decision="stale_data",
                skipped_reason="stale_data",
            )
        try:
            runtime_state = self._runtime_store.load_runtime_state_record()
        except PrimeStocksRuntimeStoreError as exc:
            logger.exception(
                "Prime Stocks runtime blocked after market-data fetch because Firestore state access failed "
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
                firestore_paths=self._runtime_store.get_paths().__dict__,
                message=(
                    "Prime Stocks runtime blocked because Firestore runtime state could not be loaded after market data "
                    f"was fetched. {exc}"
                ),
                latest_signal_time=latest_signal_time,
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
            )
        if _has_no_new_closed_bar(runtime_state=runtime_state, latest_signal_time=latest_signal_time):
            no_new_bar_result = PrimeStocksRuntimeResult(
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
                candidate_action="NO_NEW_BAR",
                execution_decision="skipped_no_new_bar",
                order_status="not_submitted",
                order_submitted=False,
                order_id=None,
                client_order_id=None,
                add_tier=None,
                execution_allowed=False,
                skipped_reason="no_new_closed_bar",
                latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
                status="no_op",
                message=_build_runtime_message(
                    execution_mode=_resolve_mode(
                        resolved_runtime_config,
                        allow_execution=allow_execution,
                        account_context=account_context,
                        settings=self._settings,
                    ),
                    execution_decision="skipped_no_new_bar",
                ),
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
                firestore_paths=self._runtime_store.get_paths().__dict__,
            )
            logger.info(
                "Prime Stocks runtime skipped for %s because no newly closed bar is available trigger_type=%s trigger_source=%s run_id=%s",
                resolved_runtime_config.symbol,
                trigger_type,
                trigger_source,
                run_id,
            )
            return no_new_bar_result
        strategy_result = self._strategy_runner(
            execution_bars=bar_set.execution_bars,
            htf_bars=bar_set.trend_bars,
            symbol=resolved_runtime_config.symbol,
            asset_type=resolved_runtime_config.asset_type,
            config=_build_strategy_config(resolved_runtime_config),
        )
        candidate_action = _resolve_candidate_action(strategy_result)
        try:
            latest_execution = self._runtime_store.load_latest_execution_record()
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
                firestore_paths=self._runtime_store.get_paths().__dict__,
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
        execution_result, execution_decision, skipped_reason, execution_allowed = self._execute_candidate_action(
            run_id=run_id,
            runtime_config=resolved_runtime_config,
            strategy_config=strategy_config,
            account_context=account_context,
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            latest_execution=latest_execution,
            allow_execution=allow_execution,
        )
        runtime_message = _build_runtime_message(
            execution_mode=_resolve_mode(
                resolved_runtime_config,
                allow_execution=allow_execution,
                account_context=account_context,
                settings=self._settings,
            ),
            execution_decision=execution_decision,
        )
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
            )
        except PrimeStocksRuntimeStoreError as exc:
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
                status="degraded",
                message=(
                    "Prime Stocks runtime reached execution but Firestore result persistence failed. "
                    f"{exc}"
                ),
                bars_processed_execution=len(bar_set.execution_bars),
                bars_processed_trend=len(bar_set.trend_bars),
                firestore_paths=self._runtime_store.get_paths().__dict__,
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
            status=strategy_result.status,
            message=runtime_message,
            bars_processed_execution=len(bar_set.execution_bars),
            bars_processed_trend=len(bar_set.trend_bars),
            firestore_paths=self._runtime_store.get_paths().__dict__,
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
        account_context: ResolvedAlpacaAccountContext,
        candidate_action: str,
        latest_signal_time: datetime | None,
        latest_execution: PrimeStocksLatestExecutionRecord,
        allow_execution: bool | None,
    ) -> tuple[AlpacaPaperExecutionResult | None, str, str | None, bool]:
        execution_mode = _resolve_mode(
            runtime_config,
            allow_execution=allow_execution,
            account_context=account_context,
            settings=self._settings,
        )
        if execution_mode == "dry-run":
            return None, "dry_run_only", "paper_execution_disabled", False
        if candidate_action not in {"FirstLot", "EXIT_ATR", "EXIT_REGIME"} and not candidate_action.startswith("MULTI-"):
            return None, "no_op", "no_action_candidate", False
        if not account_context.trade_enabled:
            return None, "credential_trade_access_missing", "credential_trade_access_missing", False
        if execution_mode == "live" and not self._settings.prime_stocks_live_execution_enabled:
            return None, "live_execution_disabled", "live_execution_disabled", False

        execution_key = _build_execution_key(candidate_action, latest_signal_time)
        if latest_execution.execution_key == execution_key and latest_execution.order_status in {"accepted", "new", "partially_filled", "filled", "submitted"}:
            return None, "skipped_duplicate", "duplicate_candidate_action", False

        client_order_id = _build_client_order_id(run_id=run_id, candidate_action=candidate_action)
        if candidate_action == "FirstLot":
            result = self._paper_trading.submit_first_lot_buy(
                symbol=runtime_config.symbol,
                asset_type=runtime_config.asset_type,
                product_key=runtime_config.product_key,
                notional=runtime_config.first_lot_notional,
                client_order_id=client_order_id,
                credential_context=account_context,
            )
            return result, "submitted_buy", None, True
        if candidate_action.startswith("MULTI-"):
            add_tier = _parse_add_tier(candidate_action)
            resolved_notional = _resolve_multi_notional(add_tier=add_tier, strategy_config=strategy_config)
            result = self._paper_trading.submit_multi_buy(
                symbol=runtime_config.symbol,
                asset_type=runtime_config.asset_type,
                product_key=runtime_config.product_key,
                notional=resolved_notional,
                client_order_id=client_order_id,
                action=candidate_action,
                add_tier=add_tier,
                credential_context=account_context,
            )
            return result, "submitted_buy", None, True
        result = self._paper_trading.close_position(
            symbol=runtime_config.symbol,
            asset_type=runtime_config.asset_type,
            product_key=runtime_config.product_key,
            action=candidate_action,
            client_order_id=client_order_id,
            credential_context=account_context,
        )
        return result, "submitted_exit", None, True


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
        paper_trading=paper_trading or AlpacaPaperTradingAdapter(settings=settings),
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
    if runtime_config.asset_type != "stock":
        raise ValueError(f"Prime Stocks runtime only supports asset_type='stock'. Received {runtime_config.asset_type!r}.")
    if allow_execution is True and not runtime_config.paper_execution_enabled:
        logger.info("Prime Stocks runtime execute trigger received while paper execution is disabled; request will stay no-op.")


def _override_symbol(runtime_config: PrimeStocksRuntimeConfigRecord, symbol: str | None) -> PrimeStocksRuntimeConfigRecord:
    return _override_runtime_selection(runtime_config, symbol=symbol)


def _override_runtime_selection(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    symbol: str | None = None,
    account_id: int | None = None,
    alpaca_account_id: int | None = None,
) -> PrimeStocksRuntimeConfigRecord:
    if symbol is None or not symbol.strip():
        resolved_symbol = runtime_config.symbol
    else:
        resolved_symbol = symbol.strip().upper()

    resolved_account_id = runtime_config.account_id if account_id is None else account_id
    resolved_alpaca_account_id = runtime_config.alpaca_account_id if alpaca_account_id is None else alpaca_account_id

    if (
        resolved_symbol == runtime_config.symbol
        and resolved_account_id == runtime_config.account_id
        and resolved_alpaca_account_id == runtime_config.alpaca_account_id
    ):
        return runtime_config

    return PrimeStocksRuntimeConfigRecord(
        product_key=runtime_config.product_key,
        strategy_key=runtime_config.strategy_key,
        strategy_title=runtime_config.strategy_title,
        symbol=resolved_symbol,
        asset_type=runtime_config.asset_type,
        enabled=runtime_config.enabled,
        dry_run=runtime_config.dry_run,
        paper_execution_enabled=runtime_config.paper_execution_enabled,
        execution_timeframe=runtime_config.execution_timeframe,
        trend_timeframe=runtime_config.trend_timeframe,
        pullback_window=runtime_config.pullback_window,
        execution_bar_limit=runtime_config.execution_bar_limit,
        trend_bar_limit=runtime_config.trend_bar_limit,
        first_lot_notional=runtime_config.first_lot_notional,
        multi_notional=runtime_config.multi_notional,
        account_id=resolved_account_id,
        alpaca_account_id=resolved_alpaca_account_id,
        runtime_target=runtime_config.runtime_target,
    )


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
        return "EXIT_ATR"
    if latest_signal.hit_regime:
        return "EXIT_REGIME"
    if latest_signal.add_trigger:
        add_tier = 1
        if strategy_result.latest_bar is not None:
            add_tier = strategy_result.latest_bar.state_before.add_count + 1
        return f"MULTI-{add_tier}"
    if latest_signal.base_entry_trigger:
        return "FirstLot"
    return "HOLD"


def _resolve_mode(
    runtime_config: PrimeStocksRuntimeConfigRecord,
    *,
    allow_execution: bool | None,
    account_context: ResolvedAlpacaAccountContext,
    settings: AppConfig,
) -> str:
    if allow_execution is False:
        return "dry-run"
    if account_context.environment == "live":
        if settings.prime_stocks_live_execution_enabled and allow_execution is True:
            return "live"
        return "dry-run"
    if allow_execution is True and runtime_config.paper_execution_enabled:
        return "paper"
    if runtime_config.paper_execution_enabled and not runtime_config.dry_run:
        return "paper"
    return "dry-run"


def _build_runtime_message(*, execution_mode: str, execution_decision: str) -> str:
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


def _build_execution_key(candidate_action: str, latest_signal_time: datetime | None) -> str:
    return f"{candidate_action}:{None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat()}"


def _build_client_order_id(*, run_id: str, candidate_action: str) -> str:
    action_slug = candidate_action.lower().replace("_", "-")
    run_slug = run_id.replace("dryrun-", "").replace("run-", "")
    return f"prime-{action_slug}-{run_slug}"[:47]


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
    )


def _normalize_runtime_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().upper()
    aliases = {
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
