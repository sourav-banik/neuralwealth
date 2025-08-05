from unittest.mock import patch
import pandas as pd
from neuralwealth.data_layer.collectors.market_data import MarketDataCollector 

@patch("yfinance.download")
def test_get_market_data_success(mock_yfinance):
    mock_yfinance.return_value = pd.DataFrame({"Close": [100, 101]})
    collector = MarketDataCollector()
    data = collector.get_market_data("AAPL")
    assert "close" in data.columns

@patch("yfinance.Ticker")
def test_basic_info_success(mock_ticker):
    mock_ticker.return_value.info = {"quoteType": "EQUITY"}
    collector = MarketDataCollector()
    data = collector.get_basic_info("AAPL")
    assert "quoteType" in data