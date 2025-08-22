import json
from unittest.mock import patch, MagicMock
from neuralwealth.ai_lab.hypothesis.hypothesis_optimizer import HypothesisOptimizer

def test_hypothesis_optimizer():
    # Set up test fixtures
    data_schema = {"measurements": ["stock_data"], "fields": ["open", "close"], "tags": ["ticker"]}
    optimizer = HypothesisOptimizer(data_schema)
    context = "Test Group: Test explanation"
    constraints = "Max 5 assets, confidence > 0.7"
    
    # Sample input data
    preliminary_input = [{"result": "| time | open | close |\n|------|------|-------|\n|2025-01-01|100.0|101.0|"}]
    validated_input = [{"thesis": "Bullish trend", "assets": ["AAPL"], "strength": "HIGH"}]
    expected_data_samples = "| time | open | close |\n|------|------|-------|\n|2025-01-01|100.0|101.0|"

    # Test prepare_hypothesis_prompt for preliminary phase
    with patch('neuralwealth.ai_lab.hypothesis.resources.llm_strategy_prompts.LLM_STRATEGY_PROMPTS', new={'preliminary': MagicMock()}) as mock_prompts:
        mock_prompts['preliminary'].format.side_effect = lambda **kwargs: (
            f"Based on {kwargs['context']} and constraints: {kwargs['constraints']}\n"
            f"Identify 5-10 raw patterns from:\n{kwargs['data_samples']}"
        )
        prompt = optimizer.prepare_hypothesis_prompt(preliminary_input, context, constraints, "preliminary")
        assert "Based on Test Group: Test explanation and constraints: Max 5 assets, confidence > 0.7" in prompt
        assert expected_data_samples in prompt
        mock_prompts['preliminary'].format.assert_called_once_with(
            context=context,
            data_schema=data_schema,
            constraints=constraints,
            data_samples=expected_data_samples,
            patterns=expected_data_samples
        )

    # Test prepare_hypothesis_prompt for validated phase
    with patch('neuralwealth.ai_lab.hypothesis.resources.llm_strategy_prompts.LLM_STRATEGY_PROMPTS', new={'validated': MagicMock()}) as mock_prompts:
        mock_prompts['validated'].format.side_effect = lambda **kwargs: (
            f"Validated hypotheses for {kwargs['context']}:\n{kwargs['data_samples']}"
        )
        prompt = optimizer.prepare_hypothesis_prompt(validated_input, context, constraints, "validated")
        assert "Validated hypotheses for Test Group: Test explanation" in prompt
        assert json.dumps([{"pattern": "Bullish trend", "assets": ["AAPL"], "strength": "HIGH"}], indent=2) in prompt
        mock_prompts['validated'].format.assert_called_once()

    # Test parse_hypothesis_response for preliminary phase
    preliminary_response = '[{"pattern": "Bullish trend", "assets": ["AAPL"], "strength": "HIGH"}]'
    hypotheses = optimizer.parse_hypothesis_response(preliminary_response, "preliminary")
    assert len(hypotheses) == 1
    assert hypotheses[0] == {"thesis": "Bullish trend", "assets": ["AAPL"], "strength": "HIGH"}

    # Test parse_hypothesis_response for final phase
    final_response = '''[{"hypothesis": "Strong buy", "assets": ["AAPL"], "trigger": "MACD crossover", 
                         "timeframe": "6 months", "strategy": "Long", "confidence": 0.85, "risks": ["Volatility"]}]'''
    hypotheses = optimizer.parse_hypothesis_response(final_response, "final")
    assert len(hypotheses) == 1
    assert hypotheses[0] == {
        "thesis": "Strong buy", "assets": ["AAPL"], "trigger": "MACD crossover",
        "timeframe": "6 months", "strategy": "Long", "confidence": 0.85, "risks": ["Volatility"]
    }

    # Test parse_hypothesis_response with invalid JSON
    hypotheses = optimizer.parse_hypothesis_response("invalid_json", "preliminary")
    assert len(hypotheses) == 0

    # Test parse_hypothesis_response with incomplete preliminary data
    incomplete_response = '[{"pattern": "Bullish trend", "assets": ["AAPL"]}]'
    hypotheses = optimizer.parse_hypothesis_response(incomplete_response, "preliminary")
    assert len(hypotheses) == 0