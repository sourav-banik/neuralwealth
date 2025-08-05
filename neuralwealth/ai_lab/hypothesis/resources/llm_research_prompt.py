# Prompt guide for LLM parameter selection
LLM_RESEARCH_PROMPT = """
    You are a trading and investment strategist designing InfluxDB query groups to generate reliable hypotheses for winning trading and investment strategies. Your goal is to select query parameters that produce interconnected datasets to uncover patterns, trends, or anomalies that can lead to actionable, high-confidence trading signals or investment decisions.

    Schema: {schema}
    Available query patterns: {patterns_json}
    Lookup Period: {timeframe}

    Based on the {analysis_focus}, create **groups of query patterns** to support hypothesis generation for trading/investment strategies. Each group should combine related data (e.g., market performance from 'market_info' with financial metrics from 'financial_ratios' or sentiment from 'news_sentiment') to enable hypotheses about price movements, portfolio optimization, or risk management. You have flexibility to:
    - Choose any combination of measurements, fields, or time periods that maximize predictive power.
    - Explore novel data relationships (e.g., macroeconomic indicators with market volatility).
    - Split the lookup period into multiple smaller periods (e.g., yearly, quarterly) to capture temporal dynamics.
    - Return the data in the json format mentioned below without any extra explanation.

    For each group:
    - Each group is independent of one another.
    - Provide an **explanation** describing how the queries contribute to trading/investment hypotheses (e.g., linking 'roe' to 'Close' for alpha generation).
    - Assign **associated_tickers** list for which the query group is created as provided in the data below.
    - Assign a **heading** to each query pattern to clarify its role in the analysis with enough explanation (e.g., 'Price Momentum Signals for 2015-2017 for Tech Stocks', 'Fundamental Strength Indicators for Forex Currencies').
    - Prefer **longer aggregation windows** (e.g., '1w', '1mo', '3mo') for weekly, monthly, or quarterly trends to ensure robust signals.
    - There is no limit on how many queries in a group, use as many as you think necessary for analysis.

    Parameters for each query:
    - **measurement**: Choose from measurements (e.g., basic_info, market_info, news_sentiment).
    - **fields**: Select fields relevant to trading/investment (e.g., 'Close', 'RSI' for market_info; 'roe', 'net_profit_margin' for financial_ratios).
    - **tickers**: List of tickers with asset class and market:
    {tickers}
    - **start/stop**: Timeframe for each period (e.g., '2015-01-01', '2015-12-31').
    - **every**: Aggregation window (e.g., '1w', '1mo', '3mo').
    - **fn**: Aggregation function (e.g., 'mean', 'last', 'sum').
    - **tag_key/tag_value**: For filtered_by_tag pattern (e.g., tag_key='asset_class', tag_value='equity').
    - **sample_size**: For sampled_data pattern (e.g., 1000-5000).

    Suggestions (not strict):
    - For **fundamental_technical**: Consider 'financial_ratios' (e.g., 'roe', 'debt_equity_ratio'), 'market_info' (e.g., 'Close', 'RSI'), or 'income_statement' (e.g., 'eps_earnings_per_share_diluted') to identify undervalued assets or momentum signals. Monthly ('1mo') or quarterly ('3mo') aggregation works well for financial data.
    - For **market_sentiment**: Pair 'news_sentiment' (e.g., 'score') with 'market_info' (e.g., 'Volume', 'ATR') to detect sentiment-driven price spikes. Weekly ('1w') aggregation is often effective.
    - For **macroeconomic analysis**: Combine 'macro_data' (e.g., 'CPI Inflation', 'Real GDP') with 'market_info' (e.g., 'Close') to assess macro impacts on asset prices. Use monthly ('1mo') or quarterly ('3mo') aggregation.
    - Experiment with fields like 'beta' (basic_info) or 'MACD' (market_info) for risk or trend analysis.
    - Use 'mean' for continuous data (e.g., 'Close'), 'last' for point-in-time data (e.g., 'total_assets'), 'sum' for cumulative data (e.g., 'Volume').
    - Keep sample_size between 1000-5000 for large datasets (e.g., 'market_info' from 1990).
    - Ensure timeframes align with measurement availability (e.g., 2009-01-31 to 2025-01-31 for 'balance_sheet').

    Return JSON:
    \\```json
    [
        {{
            "group_name": "string",
            "explanation": "string explaining how queries support trading/investment hypotheses",
            "associated_tickers": [{{"ticker": "string", "asset_class": "string", "market": "string"}}],
            "queries": [
                {{
                    "heading": "string describing data purpose",
                    "query_pattern": "pattern_name",
                    "parameters": {{
                        "measurement": "string",
                        "fields": ["string"],
                        "tickers": ["string"],
                        "start": "YYYY-MM-DD",
                        "stop": "YYYY-MM-DD",
                        "every": "duration",
                        "fn": "aggregation_function",
                        "tag_key": "string",
                        "tag_value": "string",
                        "sample_size": integer
                    }}
                }}
            ]
        }}
    ]
    \\```
    Example:
    \\```json
    [
        {{
            "group_name": "Momentum and Fundamentals",
            "explanation": "Combines market momentum indicators with fundamental metrics over quarterly periods to identify stocks with strong price trends supported by high profitability, potentially leading to buy signals.",
            "associated_tickers": [{{"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}}],
            "queries": [
                {{
                    "heading": "Price Momentum Signals",
                    "query_pattern": "time_series",
                    "parameters": {{
                        "measurement": "market_info",
                        "fields": ["Close", "RSI", "MACD"],
                        "tickers": ["AAPL", "MSFT"],
                        "start": "2015-01-01",
                        "stop": "2015-03-31",
                        "every": "1w",
                        "fn": "mean"
                    }}
                }},
                {{
                    "heading": "Fundamental Strength Indicators",
                    "query_pattern": "time_series",
                    "parameters": {{
                        "measurement": "financial_ratios",
                        "fields": ["roe", "net_profit_margin"],
                        "tickers": ["AAPL", "MSFT"],
                        "start": "2015-01-01",
                        "stop": "2015-03-31",
                        "every": "3mo",
                        "fn": "last"
                    }}
                }}
            ]
        }}
    ]
    \\```
"""