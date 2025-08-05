from unittest.mock import patch
from neuralwealth.data_layer.data_pipeline import DataPipeline
import pandas as pd
import numpy as np

@patch('neuralwealth.data_layer.collectors.market_data.MarketDataCollector.get_basic_info')
@patch('neuralwealth.data_layer.collectors.market_data.MarketDataCollector.get_market_data')
@patch('neuralwealth.data_layer.collectors.news_sentiment.NewsSentimentCollector.scrape_news_sentiment')
@patch('neuralwealth.data_layer.collectors.macro_data.FREDCollector.fetch_all')
@patch('neuralwealth.data_layer.collectors.financials_data.FinancialsCollector.get_financials')
@patch('neuralwealth.data_layer.collectors.ticker_collector.TickerCollector.collect_tickers')
@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBStorage.write_dataframe')
@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBStorage.write_unstructured')
@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBStorage.close')
def test_pipeline_e2e(
    mock_close,
    mock_write_unstructured,
    mock_write_dataframe,
    mock_collect_tickers,
    mock_get_financials,
    mock_fetch_all,
    mock_scrape_news_sentiment,
    mock_get_market_data,
    mock_get_basic_info
):
    """Test end-to-end DataPipeline execution."""
    # Mock configuration
    config = {
        "env": "test",
        "influxdb_url": "http://localhost:8086",
        "influxdb_token": "test_token",
        "influxdb_org": "test_org",
        "influxdb_bucket": "test_bucket",
        "twitter_bearer_token": "test_twitter_token",
        "fred_api_key": "test_fred_key"
    }

    # Mock ticker data
    mock_tickers = [
        {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ", "symbol": "AAPL"}
    ]
    mock_collect_tickers.return_value = mock_tickers

    # Mock collector responses
    mock_get_basic_info.return_value = {"symbol": "AAPL", "name": "Apple Inc."}
    dates = pd.date_range(start='2025-06-30', periods=26, freq='D')
    mock_get_market_data.return_value = pd.DataFrame({
        'time': dates,
        'open': 158.0 + np.random.normal(0, 2, 26),
        'high': 159.0 + np.random.normal(0, 2, 26),
        'low': 156.0 + np.random.normal(0, 2, 26),
        'close': 157.0 + np.random.normal(0, 2, 26),
        'volume': 200000000 + np.random.normal(0, 10000000, 26),
        'dividends': [0.0] * 26,
        'stock_splits': [0.0] * 26
    })
    mock_scrape_news_sentiment.return_value = pd.DataFrame({
        'timestamp': [pd.to_datetime('2025-06-29')],
        'sentiment': [0.5]
    })
    mock_get_financials.return_value = {
        "quarterly_income_statement": pd.DataFrame({
            '': [pd.to_datetime('2025-03-31')],
            'Revenue': [1000000.0]
        })
    }
    mock_fetch_all.return_value = {
        "nominal_gdp": pd.DataFrame({
            'nominal_gdp': [20000.0]
        }, index=[pd.to_datetime('2025-03-31')])
    }

    # Initialize pipeline
    pipeline = DataPipeline(config)

    # Run pipeline with test tickers
    result = pipeline.run_pipeline(mock_tickers)

    # Assertions
    assert result["status"] == "success"
    assert result["tickers_processed"] == 1
    assert result["macro_indicators_processed"] == 1

    # Verify collector calls
    mock_get_basic_info.assert_called_once_with("AAPL")
    mock_get_market_data.assert_called_once_with("AAPL", period="max")
    mock_scrape_news_sentiment.assert_called_once_with("AAPL")
    mock_get_financials.assert_called_once_with("AAPL")
    mock_fetch_all.assert_called_once()

    # Verify database writes
    mock_write_unstructured.assert_called_once()
    mock_write_dataframe.assert_called()
    mock_close.assert_called_once()