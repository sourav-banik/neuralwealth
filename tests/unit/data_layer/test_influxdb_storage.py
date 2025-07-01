from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from neuralwealth.data_layer.storage.influxdb_storage import InfluxDBStorage

# Sample data
def create_sample_df():
    return pd.DataFrame({
        'time': pd.to_datetime(['2025-06-29 12:00:00']),
        'price': [100.0],
        'symbol': ['AAPL']
    })

def create_sample_unstructured_data():
    return {"price": 100.0, "symbol": "AAPL", "companyOfficers": [{"name": "Tim Cook"}]}

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_init(mock_influx_client):
    """Test initialization."""
    mock_client = MagicMock()
    mock_influx_client.return_value = mock_client
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    assert client.bucket == "test_bucket"
    mock_influx_client.assert_called_once()
    mock_client.write_api.assert_called_once()

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_write_dataframe(mock_influx_client):
    """Test write_dataframe."""
    mock_client = MagicMock()
    mock_influx_client.return_value = mock_client
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    df = create_sample_df()

    # Success
    client.write_dataframe(df, "stock_prices", ['symbol'], 'time')
    mock_client.write_api().write.assert_called_once()

    # Error
    mock_client.write_api().reset_mock()
    mock_client.write_api().write.side_effect = Exception("Connection error")
    with pytest.raises(ValueError, match="Influx write failed"):
        client.write_dataframe(df, "stock_prices", ['symbol'], 'time')
    mock_client.write_api().flush.assert_not_called()

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_write_unstructured(mock_influx_client):
    """Test write_unstructured."""
    mock_client = MagicMock()
    mock_influx_client.return_value = mock_client
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    data = create_sample_unstructured_data()
    metadata = {"symbol": data["symbol"]}

    # Success
    client.write_unstructured("stock_prices", data, metadata)
    mock_client.write_api().write.assert_called_once()
    # Error
    mock_client.write_api().reset_mock()
    mock_client.write_api().write.side_effect = Exception("Connection error")
    with pytest.raises(ValueError, match="Influx write failed"):
        client.write_unstructured("stock_prices", data, metadata)
    mock_client.write_api().flush.assert_not_called()

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_query_to_dataframe(mock_influx_client):
    """Test query_to_dataframe."""
    mock_client = MagicMock()
    mock_influx_client.return_value = mock_client
    mock_df = pd.DataFrame({'value': [100.0]})
    mock_client.query_api().query_data_frame.return_value = mock_df
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    result = client.query_to_dataframe('from(bucket:"test_bucket") |> range(start:-1h)')
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, mock_df)

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_close(mock_influx_client):
    """Test close."""
    mock_client = MagicMock()
    mock_influx_client.return_value = mock_client
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    client.close()
    mock_client.write_api().flush.assert_called_once()
    mock_client.close.assert_called_once()

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_preprocess_data(mock_influx_client):
    """Test preprocess_data."""
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    data = create_sample_unstructured_data()
    processed = client.preprocess_data(data)
    assert isinstance(processed["companyOfficers"], str)
    assert processed["price"] == 100.0
    assert processed["symbol"] == "AAPL"

@patch('neuralwealth.data_layer.storage.influxdb_storage.InfluxDBClient')
def test_preprocess_dataframe(mock_influx_client):
    """Test preprocess_dataframe."""
    client = InfluxDBStorage(url="http://localhost:8086", token="test_token", org="test_org", bucket="test_bucket")
    df = pd.DataFrame({
        'time': pd.to_datetime(['2025-06-29 12:00:00']),
        'price': [100],
        'symbol': ['AAPL']
    })
    processed_df = client.preprocess_dataframe(df, time_col='time')
    assert processed_df['price'].dtype == 'float64'