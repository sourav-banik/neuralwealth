LLM_REFINEMENT_PROMPT = """
    You are an expert in financial strategy optimization. 
    Refine the following investment hypothesis based on its backtest and crash scenario results to improve performance. 
    The refined hypothesis must:
    - Maintain the same assets and general strategy structure.
    - Improve Sharpe ratio (> {min_sharpe}), reduce max drawdown (< {max_drawdown}), and ensure positive returns (> {min_total_return}).
    - Ensure crash resilience (drawdown < {max_crash_drawdown}, return > {min_crash_return}).
    - Use only Backtrader-compatible indicators (e.g., SMA, EMA, RSI, MACD, ADX, ATR, Stochastic).
    - Use named lines for indicators (e.g., indicators['rsi'].lines.rsi[0]).
    - Ensure conditions reference fields in the original data_feed.
    - Return a single refined hypothesis in JSON format matching the original structure.

    Original Hypothesis:
    {hypothesis}

    Backtest Results:
    {backtest_results}

    Crash Scenario Results:
    {crash_results}

    Return JSON:
    {{
        "thesis": "Refined thesis statement",
        "assets": [...],  // Same as original
        "trigger": "Refined trigger description",
        "timeframe": "Timeframe (e.g., '3-6 months')",
        "strategy": {{
            "indicators": [...],
            "buy_conditions": ["Python expression"],
            "sell_conditions": ["Python expression"],
            "holding_period": Integer,
            "data_feed": {{...}}
        }},
        "confidence": Float (0.0-1.0, slightly reduced),
        "risks": "Risk description",
        "id": "OriginalID_refined",
        "last_updated": "YYYY-MM-DDTHH:MM:SS.ssssss"
    }}
    If refinement is not possible, return null.
"""