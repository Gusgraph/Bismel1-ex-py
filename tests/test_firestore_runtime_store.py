from unittest.mock import MagicMock, patch

import pytest

from app.services.firestore_runtime_store import (
    PrimeStocksFirestoreRuntimeStore,
    PrimeStocksLatestExecutionRecord,
    PrimeStocksRuntimeConfigRecord,
    PrimeStocksRuntimeStateRecord,
)
from app.shared.config import AppConfig





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
        alpaca_data_base_url="https://data.alpaca.markets",
        alpaca_trading_base_url="https://paper-api.alpaca.markets",
        alpaca_api_key_id="key_id",
        alpaca_api_secret="secret",
        alpaca_data_feed="iex",
        prime_stocks_runtime_enabled=True,
        prime_stocks_dry_run=True,
        prime_stocks_paper_execution_enabled=True,
        prime_stocks_default_symbol="AAPL",
        prime_stocks_asset_type="stock",
        prime_stocks_execution_bar_limit=10,
        prime_stocks_trend_bar_limit=10,
        prime_stocks_first_lot_notional=100.0,
        prime_stocks_multi_notional=50.0,
        prime_stocks_scheduler_job_name="scheduler-job",
        prime_stocks_scheduler_region="us-central1",
        prime_stocks_scheduler_schedule="* * * * *",
        prime_stocks_scheduler_timezone="UTC",
        prime_stocks_scheduler_header_name="X-Scheduler-Header",
        prime_stocks_scheduler_header_value="secret-value",
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
        execution_timeframe="1H",
        trend_timeframe="1D",
        pullback_window=5,
        execution_bar_limit=351,
        trend_bar_limit=221,
        first_lot_notional=101.0,
        multi_notional=73.0,
        runtime_target="cloud_run",
    )

    with patch.object(
        PrimeStocksFirestoreRuntimeStore, "load_runtime_config", return_value=default_config
    ) as mock_load_runtime_config:
        store = PrimeStocksFirestoreRuntimeStore(settings=mock_app_config)
        config = store.load_runtime_config(default_config)

        mock_load_runtime_config.assert_called_once_with(default_config)
        assert config == default_config

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


