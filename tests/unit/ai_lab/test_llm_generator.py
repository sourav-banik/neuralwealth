from unittest.mock import patch
import json
from neuralwealth.ai_lab.hypothesis.llm_generator import LLMGenerator


@patch("neuralwealth.ai_lab.utils.data_feeder.DataFeeder")
@patch("openai.OpenAI")
def test_init(mock_openai, mock_data_feeder):
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "http://localhost:8086",
        "influxdb_token": "test_token",
        "influxdb_org": "test_org",
        "influxdb_bucket": "test_bucket"
    }
    generator = LLMGenerator(config)
    assert generator.llm_model == "test_model"
    assert generator.conversation_cache == {}


@patch("neuralwealth.ai_lab.utils.data_feeder.DataFeeder")
@patch("openai.OpenAI")
def test_generate_hypotheses(mock_openai, mock_data_feeder):
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "http://localhost:8086",
        "influxdb_token": "",
        "influxdb_org": "",
        "influxdb_bucket": ""
    }
    mock_data_feeder.return_value.get_llm_prompt.return_value = "test_prompt"
    mock_data_feeder.return_value.process_llm_query_request.return_value = [
        {
            "group_name": "Test Group",
            "explanation": "Test explanation",
            "queries": [{"result": "| _time | ticker | Close |\n| 2015-01-01 | AAPL | 110.5 |"}]
        }
    ]
    mock_openai.return_value.chat.completions.create.return_value.choices = [
        type("obj", (), {"message": type("obj", (), {"content": json.dumps([
            {"pattern": "Test pattern", "assets": ["AAPL"], "strength": "HIGH"},
            {"pattern": "Another pattern", "assets": ["MSFT"], "strength": "MEDIUM"}
        ])})})
    ] * 3  # Return for preliminary, validated, final
    generator = LLMGenerator(config)
    params = {"timeframe": "2015-01-01 to 2019-12-31", "analysis_focus": "fundamental_technical"}
    result = generator.generate_hypotheses(params)
    assert isinstance(result, list)


def test_parse_hypotheses_preliminary():
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "",
        "influxdb_token": "",
        "influxdb_org": "",
        "influxdb_bucket": ""
    }
    generator = LLMGenerator(config)
    text = json.dumps([
        {"pattern": "Test pattern", "assets": ["AAPL"], "strength": "HIGH"}
    ])
    result = generator._parse_hypotheses(text, "preliminary")
    assert len(result) == 1
    assert result[0]["thesis"] == "Test pattern"
    assert result[0]["assets"] == ["AAPL"]
    assert result[0]["strength"] == "HIGH"


def test_parse_hypotheses_final():
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "",
        "influxdb_token": "",
        "influxdb_org": "",
        "influxdb_bucket": ""
    }
    generator = LLMGenerator(config)
    text = json.dumps([
        {
            "hypothesis": "Buy AAPL",
            "assets": ["AAPL"],
            "trigger": "RSI > 60",
            "timeframe": "1-3 months",
            "confidence": 0.7,
            "risks": "Market volatility"
        }
    ])
    result = generator._parse_hypotheses(text, "final")
    assert len(result) == 1
    assert result[0]["thesis"] == "Buy AAPL"
    assert result[0]["trigger"] == "RSI > 60"
    assert result[0]["confidence"] == 0.7


def test_format_output():
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "",
        "influxdb_token": "",
        "influxdb_org": "",
        "influxdb_bucket": ""
    }
    generator = LLMGenerator(config)
    hypotheses = [
        {"thesis": "Test pattern", "assets": ["AAPL"], "strength": "HIGH"},
        {"thesis": "Buy AAPL", "assets": ["AAPL"], "trigger": "RSI > 60", "timeframe": "1-3 months", "confidence": 0.7, "risks": "Market volatility"}
    ]
    result = generator._format_output(hypotheses)
    assert len(result) == 2
    assert "id" in result[0]
    assert "confidence" in result[0]
    assert "last_updated" in result[0]
    assert result[0]["confidence"] >= result[1]["confidence"]


def test_estimate_confidence():
    config = {
        "openai_sdk_base_url": "https://api.example.com",
        "open_ai_sdk_api_key": "test_key",
        "llm_model": "test_model",
        "influxdb_url": "",
        "influxdb_token": "",
        "influxdb_org": "",
        "influxdb_bucket": ""
    }
    generator = LLMGenerator(config)
    result = generator._estimate_confidence("p < 0.05, consistent across")
    assert 0.7 <= result <= 0.99
    result = generator._estimate_confidence("anecdotal evidence")
    assert 0.1 <= result <= 0.4