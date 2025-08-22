from unittest.mock import patch
import json
from neuralwealth.ai_lab.hypothesis.hypothesis_initializer import HypothesisInitializer


@patch("neuralwealth.ai_lab.utils.data_feeder.DataFeeder")
@patch("openai.OpenAI")
def test_init(mock_openai, mock_data_feeder):
    generator = HypothesisInitializer(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        bucket="test_bucket"
    )
    assert generator.llm_model == "test_model"
    assert generator.conversation_cache == {}


@patch("neuralwealth.ai_lab.utils.data_feeder.DataFeeder")
@patch("openai.OpenAI")
def test_generate_hypotheses(mock_openai, mock_data_feeder):
    generator = HypothesisInitializer(
        url="http://localhost:8086",
        token="",
        org="",
        bucket=""
    )
    mock_data_feeder.return_value.get_llm_prompt.return_value = "test_prompt"
    mock_data_feeder.return_value.process_llm_query_request.return_value = [
        {
            "group_name": "Test Group",
            "explanation": "Test explanation",
            "queries": [{"result": "| _time | ticker | Close |\n| 2015-01-01 | AAPL | 110.5 |"}],
            "associated_tickers": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}]
        }
    ]
    mock_openai.return_value.chat.completions.create.return_value.choices = [
        type("obj", (), {"message": type("obj", (), {"content": json.dumps([
            {
                "pattern": "Test pattern",
                "assets": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
                "strength": "HIGH"
            },
            {
                "pattern": "Another pattern",
                "assets": [{"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}],
                "strength": "MEDIUM"
            }
        ])})})
    ] * 3
    result = generator.generate_hypotheses(
        tickers=[{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
        timeframe="2015-01-01 to 2019-12-31",
        analysis_focus="fundamental_technical"
    )
    assert isinstance(result, list)


def test_parse_hypotheses_preliminary():
    generator = HypothesisInitializer(
        url="",
        token="",
        org="",
        bucket=""
    )
    text = json.dumps([
        {
            "pattern": "Test pattern",
            "assets": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
            "strength": "HIGH"
        }
    ])
    result = generator._parse_hypotheses(text, "preliminary")
    assert len(result) == 1
    assert result[0]["thesis"] == "Test pattern"
    assert result[0]["assets"] == [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}]
    assert result[0]["strength"] == "HIGH"


def test_parse_hypotheses_final():
    generator = HypothesisInitializer(
        url="",
        token="",
        org="",
        bucket=""
    )
    text = json.dumps([
        {
            "hypothesis": "Buy AAPL",
            "assets": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
            "trigger": "RSI > 60",
            "timeframe": "1-3 months",
            "confidence": 0.7,
            "risks": "Market volatility",
            "strategy": {
                "indicators": [],
                "buy_conditions": ["rsi > 60"],
                "sell_conditions": ["rsi < 40"],
                "holding_period": 60,
                "data_feed": {"market_info": ["RSI"]}
            }
        }
    ])
    result = generator._parse_hypotheses(text, "final")
    assert len(result) == 1
    assert result[0]["thesis"] == "Buy AAPL"
    assert result[0]["trigger"] == "RSI > 60"
    assert result[0]["confidence"] == 0.7
    assert result[0]["strategy"] == {
        "indicators": [],
        "buy_conditions": ["rsi > 60"],
        "sell_conditions": ["rsi < 40"],
        "holding_period": 60,
        "data_feed": {"market_info": ["RSI"]}
    }


def test_format_output():
    generator = HypothesisInitializer(
        url="",
        token="",
        org="",
        bucket=""
    )
    hypotheses = [
        {
            "thesis": "Test pattern",
            "assets": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
            "strength": "HIGH"
        },
        {
            "thesis": "Buy AAPL",
            "assets": [{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}],
            "trigger": "RSI > 60",
            "timeframe": "1-3 months",
            "confidence": 0.7,
            "risks": "Market volatility",
            "strategy": {
                "indicators": [],
                "buy_conditions": ["rsi > 60"],
                "sell_conditions": ["rsi < 40"],
                "holding_period": 60,
                "data_feed": {"market_info": ["RSI"]}
            }
        }
    ]
    result = generator._format_output(hypotheses)
    assert len(result) == 2
    assert "id" in result[0]
    assert "confidence" in result[0]
    assert "last_updated" in result[0]
    assert result[0]["confidence"] >= result[1]["confidence"]


def test_estimate_confidence():
    generator = HypothesisInitializer(
        url="",
        token="",
        org="",
        bucket=""
    )
    result = generator._estimate_confidence("p < 0.05, consistent across")
    assert 0.7 <= result <= 0.99
    result = generator._estimate_confidence("anecdotal evidence")
    assert 0.1 <= result <= 0.4