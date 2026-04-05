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
from datetime import UTC, datetime
import logging

from app.brokers.alpaca_market_data import AlpacaMarketDataAdapter
from app.products.stocks.bismel1.strategy import run_prime_stocks_strategy
from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksRuntimeConfigRecord,
    build_default_runtime_config,
)
from app.shared.config import AppConfig


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrimeStocksDryRunResult:
    run_id: str
    mode: str
    runtime_target: str
    product_key: str
    strategy_key: str
    strategy_title: str
    symbol: str
    asset_type: str
    enabled: bool
    candidate_action: str
    latest_signal_time: str | None
    status: str
    message: str
    bars_processed_execution: int
    bars_processed_trend: int
    firestore_paths: dict[str, str]


class PrimeStocksDryRunService:
    def __init__(
        self,
        settings: AppConfig,
        market_data: AlpacaMarketDataAdapter,
        runtime_store: PrimeStocksFirestoreRuntimeStore,
    ) -> None:
        self._settings = settings
        self._market_data = market_data
        self._runtime_store = runtime_store

    def run_once(self, symbol: str | None = None) -> PrimeStocksDryRunResult:
        default_runtime_config = build_default_runtime_config(self._settings)
        runtime_config = self._runtime_store.load_runtime_config(default_runtime_config)
        resolved_runtime_config = _override_symbol(runtime_config, symbol)
        _ensure_prime_stocks_runtime_context(resolved_runtime_config)

        run_id = self._runtime_store.create_run_id()
        if not resolved_runtime_config.enabled:
            message = "Prime Stocks dry-run skipped because the runtime config is disabled."
            disabled_result = PrimeStocksDryRunResult(
                run_id=run_id,
                mode="dry-run",
                runtime_target=resolved_runtime_config.runtime_target,
                product_key=resolved_runtime_config.product_key,
                strategy_key=resolved_runtime_config.strategy_key,
                strategy_title=resolved_runtime_config.strategy_title,
                symbol=resolved_runtime_config.symbol,
                asset_type=resolved_runtime_config.asset_type,
                enabled=False,
                candidate_action="DISABLED",
                latest_signal_time=None,
                status="disabled",
                message=message,
                bars_processed_execution=0,
                bars_processed_trend=0,
                firestore_paths=self._runtime_store.get_paths().__dict__,
            )
            logger.info(message)
            return disabled_result

        bar_set = self._market_data.fetch_prime_stocks_bars(
            symbol=resolved_runtime_config.symbol,
            asset_type=resolved_runtime_config.asset_type,
            product_key=resolved_runtime_config.product_key,
            execution_limit=resolved_runtime_config.execution_bar_limit,
            trend_limit=resolved_runtime_config.trend_bar_limit,
        )
        strategy_result = run_prime_stocks_strategy(
            execution_bars=bar_set.execution_bars,
            htf_bars=bar_set.trend_bars,
            symbol=resolved_runtime_config.symbol,
            asset_type=resolved_runtime_config.asset_type,
        )
        latest_signal_time = _resolve_latest_signal_time(bar_set.execution_bars)
        candidate_action = _resolve_candidate_action(strategy_result)
        dry_run_message = (
            "Prime Stocks Cloud Run dry-run completed with market data, strategy evaluation, and Firestore writes. "
            "No live orders were placed."
        )
        self._runtime_store.write_dry_run_result(
            run_id=run_id,
            runtime_config=resolved_runtime_config,
            strategy_result=strategy_result,
            candidate_action=candidate_action,
            latest_signal_time=latest_signal_time,
            dry_run_message=dry_run_message,
        )
        result = PrimeStocksDryRunResult(
            run_id=run_id,
            mode="dry-run",
            runtime_target=resolved_runtime_config.runtime_target,
            product_key=resolved_runtime_config.product_key,
            strategy_key=resolved_runtime_config.strategy_key,
            strategy_title=resolved_runtime_config.strategy_title,
            symbol=resolved_runtime_config.symbol,
            asset_type=resolved_runtime_config.asset_type,
            enabled=resolved_runtime_config.enabled,
            candidate_action=candidate_action,
            latest_signal_time=None if latest_signal_time is None else latest_signal_time.astimezone(UTC).isoformat(),
            status=strategy_result.status,
            message=dry_run_message,
            bars_processed_execution=len(bar_set.execution_bars),
            bars_processed_trend=len(bar_set.trend_bars),
            firestore_paths=self._runtime_store.get_paths().__dict__,
        )
        logger.info(
            "Prime Stocks dry-run completed for %s with candidate_action=%s run_id=%s",
            resolved_runtime_config.symbol,
            candidate_action,
            run_id,
        )
        return result


def build_prime_stocks_dry_run_service(
    settings: AppConfig,
    market_data: AlpacaMarketDataAdapter | None = None,
    runtime_store: PrimeStocksFirestoreRuntimeStore | None = None,
) -> PrimeStocksDryRunService:
    return PrimeStocksDryRunService(
        settings=settings,
        market_data=market_data or AlpacaMarketDataAdapter(settings=settings),
        runtime_store=runtime_store or PrimeStocksFirestoreRuntimeStore(settings=settings),
    )


def _ensure_prime_stocks_runtime_context(runtime_config: PrimeStocksRuntimeConfigRecord) -> None:
    if runtime_config.product_key != "stocks.bismel1":
        raise ValueError(f"Prime Stocks dry-run only supports product_key='stocks.bismel1'. Received {runtime_config.product_key!r}.")
    if runtime_config.asset_type != "stock":
        raise ValueError(f"Prime Stocks dry-run only supports asset_type='stock'. Received {runtime_config.asset_type!r}.")
    if not runtime_config.dry_run:
        raise ValueError("This Prime Stocks runtime phase supports dry-run execution only.")


def _override_symbol(runtime_config: PrimeStocksRuntimeConfigRecord, symbol: str | None) -> PrimeStocksRuntimeConfigRecord:
    if symbol is None or not symbol.strip():
        return runtime_config
    return PrimeStocksRuntimeConfigRecord(
        product_key=runtime_config.product_key,
        strategy_key=runtime_config.strategy_key,
        strategy_title=runtime_config.strategy_title,
        symbol=symbol.strip().upper(),
        asset_type=runtime_config.asset_type,
        enabled=runtime_config.enabled,
        dry_run=runtime_config.dry_run,
        execution_timeframe=runtime_config.execution_timeframe,
        trend_timeframe=runtime_config.trend_timeframe,
        pullback_window=runtime_config.pullback_window,
        execution_bar_limit=runtime_config.execution_bar_limit,
        trend_bar_limit=runtime_config.trend_bar_limit,
        runtime_target=runtime_config.runtime_target,
    )


def _resolve_latest_signal_time(execution_bars) -> datetime | None:
    if not execution_bars:
        return None
    latest_bar = execution_bars[-1]
    return latest_bar.ends_at or latest_bar.starts_at


def _resolve_candidate_action(strategy_result) -> str:
    latest_signal = strategy_result.latest_signal
    if latest_signal.hit_atr_trail:
        return "EXIT_ATR"
    if latest_signal.hit_regime:
        return "EXIT_REGIME"
    if latest_signal.add_trigger:
        return "MULTI"
    if latest_signal.base_entry_trigger:
        return "FirstLot"
    return "HOLD"
