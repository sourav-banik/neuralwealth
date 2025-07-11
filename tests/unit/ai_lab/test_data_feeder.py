from unittest.mock import patch
import pandas as pd
import json
from datetime import datetime
from neuralwealth.ai_lab.utils.data_feeder import DataFeeder


@patch("neuralwealth.ai_lab.utils.data_feeder.DataSchemaGenerator")
@patch("influxdb_client.InfluxDBClient")
def test_init(mock_influx_client, mock_schema_generator):
    mock_schema_generator.return_value.generate_schema.return_value = {
        "market_info": {"fields": ["Close"], "tags": ["ticker"]}
    }
    config = {
        "influxdb_url": "http://localhost:8086",
        "influxdb_token": "test_token",
        "influxdb_org": "test_org",
        "influxdb_bucket": "test_bucket"
    }
    feeder = DataFeeder(config)
    assert feeder.data_bucket == "test_bucket"
    assert "market_info" in feeder.schema


def test_get_llm_prompt():
    config = {"influxdb_url": "http://localhost:8086", "influxdb_token": "", "influxdb_org": "", "influxdb_bucket": ""}
    feeder = DataFeeder(config)
    feeder.schema = {"market_info": {"fields": ["Close"], "tags": ["ticker"]}}
    prompt = feeder.get_llm_prompt("2015-01-01 to 2019-12-31", "fundamental_technical")
    assert "fundamental_technical" in prompt
    assert "2015-01-01 to 2019-12-31" in prompt
    assert '"time_series"' in prompt


@patch("influxdb_client.InfluxDBClient")
def test_execute_query_invalid_pattern(mock_influx_client):
    config = {"influxdb_url": "http://localhost:8086", "influxdb_token": "", "influxdb_org": "", "influxdb_bucket": "test_bucket"}
    feeder = DataFeeder(config)
    feeder.schema = {"market_info": {"fields": ["Close"], "tags": ["ticker"]}}
    params = {"measurement": "market_info", "fields": ["Close"], "tickers": ["AAPL"]}
    df = feeder.execute_query("invalid_pattern", params)
    assert df.empty


@patch("influxdb_client.InfluxDBClient")
def test_process_llm_query_request(mock_influx_client):
    config = {"influxdb_url": "http://localhost:8086", "influxdb_token": "", "influxdb_org": "", "influxdb_bucket": "test_bucket"}
    feeder = DataFeeder(config)
    feeder.schema = {"market_info": {"fields": ["Close"], "tags": ["ticker"]}}
    mock_query_api = mock_influx_client.return_value.query_api.return_value
    mock_query_api.query_data_frame.return_value = pd.DataFrame({
        "_time": [datetime(2015, 1, 1)],
        "ticker": ["AAPL"],
        "Close": [110.5]
    })
    llm_response = json.dumps([{
        "group_name": "Test Group",
        "explanation": "Test explanation",
        "queries": [{
            "heading": "Test Query",
            "query_pattern": "time_series",
            "parameters": {
                "measurement": "market_info",
                "fields": ["Close"],
                "tickers": ["AAPL"],
                "start": "2015-01-01",
                "stop": "2015-03-31",
                "every": "1w",
                "fn": "mean"
            }
        }]
    }])
    result_json = feeder.process_llm_query_request(llm_response)
    assert result_json[0]["group_name"] == "Test Group"
    assert result_json[0]["queries"][0]["heading"] == "Test Query"


def test_summarize_data_empty():
    config = {"influxdb_url": "http://localhost:8086", "influxdb_token": "", "influxdb_org": "", "influxdb_bucket": ""}
    feeder = DataFeeder(config)
    df = pd.DataFrame()
    result = feeder.summarize_data(df)
    assert result == "No data available."
