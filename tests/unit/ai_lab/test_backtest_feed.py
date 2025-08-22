from unittest.mock import patch
import pandas as pd
import numpy as np
from datetime import datetime
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from neuralwealth.ai_lab.utils.backtest_client import BackTestDataFeed


@patch("influxdb_client.InfluxDBClient")
def test_init(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    assert feeder.bucket == "test_bucket"
    assert feeder.org == "test_org"


@patch("influxdb_client.InfluxDBClient")
def test_get_asset_data(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    record = FluxRecord(table=0)
    record.values = {"_time": datetime(2015, 1, 1), "Open": 100.0, "High": 110.0, "Low": 90.0, "Close": 105.0, "Volume": 1000.0}
    table = FluxTable()
    table.records = [record]
    mock_query_api.query.return_value = [table]
    
    assets = [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}]
    fields = {"market_info": ["Close", "RSI"]}
    result = feeder.get_asset_data(assets, fields, "2015-01-01", "2015-03-31")
    assert "AAPL" in result
    assert isinstance(result["AAPL"], pd.DataFrame)
    assert all(col in result["AAPL"].columns for col in ["open", "high", "low", "close", "volume"])


@patch("influxdb_client.InfluxDBClient")
def test_get_market_data(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    record = FluxRecord(table=0)
    record.values = {"_time": datetime(2015, 1, 1), "Open": 100.0, "High": 110.0, "Low": 90.0, "Close": 105.0, "Volume": 1000.0, "RSI": 60.0}
    table = FluxTable()
    table.records = [record]
    mock_query_api.query.return_value = [table]
    
    asset = {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}
    df = feeder._get_market_data(asset, ["Close", "RSI"], "2015-01-01", "2015-03-31")
    assert not df.empty
    assert all(col in df.columns for col in ["open", "high", "low", "close", "volume", "RSI"])


@patch("influxdb_client.InfluxDBClient")
def test_get_asset_data_specific_field(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    record = FluxRecord(table=0)
    record.values = {"_time": datetime(2015, 1, 1), "roe": 0.15}
    table = FluxTable()
    table.records = [record]
    mock_query_api.query.return_value = [table]
    
    asset = {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}
    series = feeder._get_asset_data(asset, "financial_ratios", "roe", "2015-01-01", "2015-03-31")
    assert series is not None
    assert series.name == "roe"
    assert not series.empty


@patch("influxdb_client.InfluxDBClient")
def test_get_macro_data(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    record = FluxRecord(table=0)
    record.values = {"_time": datetime(2015, 1, 1), "CPI Inflation": 2.5}
    table = FluxTable()
    table.records = [record]
    mock_query_api.query.return_value = [table]
    
    series = feeder._get_macro_data("CPI Inflation", "2015-01-01", "2015-03-31")
    assert series is not None
    assert series.name == "CPI Inflation"
    assert not series.empty


@patch("influxdb_client.InfluxDBClient")
def test_get_news_sentiment(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    record = FluxRecord(table=0)
    record.values = {"_time": datetime(2015, 1, 1), "score": 0.75}
    table = FluxTable()
    table.records = [record]
    mock_query_api.query.return_value = [table]
    
    asset = {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}
    series = feeder._get_news_sentiment(asset, "score", "2015-01-01", "2015-03-31")
    assert series is not None
    assert series.name == "score"
    assert not series.empty


@patch("influxdb_client.InfluxDBClient")
def test_flux_to_dataframe_empty(mock_influx_client):
    feeder = BackTestDataFeed(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    mock_query_api.query.return_value = []
    
    df = feeder._flux_to_dataframe([])
    assert df.empty