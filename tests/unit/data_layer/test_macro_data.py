from unittest.mock import patch
import pandas as pd
from neuralwealth.data_layer.collectors.macro_data import FREDCollector

@patch("neuralwealth.data_layer.collectors.macro_data.Fred.get_series")
def test_fetch_series_success(mock_get_series):
    """Test fetch_series for successful data retrieval."""
    mock_get_series.return_value = pd.Series([100.0, 101.0], index=pd.date_range("2025-01-01", periods=2))
    collector = FREDCollector(api_key="mock_api_key")
    data = collector.fetch_series("GDPC1")
    assert "GDPC1" in data.columns

@patch("neuralwealth.data_layer.collectors.macro_data.Fred.get_series")
def test_fetch_all_success(mock_get_series):
    """Test fetch_all for successful data retrieval."""
    mock_get_series.return_value = pd.Series([100.0, 101.0], index=pd.date_range("2025-01-01", periods=2))
    collector = FREDCollector(api_key="mock_api_key")
    data = collector.fetch_all()
    assert "Real GDP" in data
    assert "GDPC1" in data["Real GDP"].columns