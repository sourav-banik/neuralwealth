import json, os, re
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from influxdb_client import InfluxDBClient
from neuralwealth.ai_lab.utils.data_schema_generator import DataSchemaGenerator


class DataFeeder:
    """Manages InfluxDB queries with predefined, flexible query patterns for LLM parameter adjustment."""

    # Predefined query patterns with placeholders for parameters
    QUERY_PATTERNS = {
        "time_series": """
            from(bucket: "{bucket}")
            |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
            |> filter(fn: (r) => r["_field"] =~ /({fields})/)
            |> aggregateWindow(every: {every}, fn: {fn})
            |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
        """,
        "aggregate_summary": """
            from(bucket: "{bucket}")
            |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
            |> filter(fn: (r) => r["_field"] =~ /({fields})/)
            |> group(columns: ["ticker"])
            |> aggregateWindow(every: {every}, fn: {fn})
            |> yield(name: "summary")
        """,
        "correlation": """
            from(bucket: "{bucket}")
            |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
            |> filter(fn: (r) => r["_field"] =~ /({fields})/)
            |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{ r with correlation: // Add correlation logic }}))
        """,
        "filtered_by_tag": """
            from(bucket: "{bucket}")
            |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["{tag_key}"] =~ /({tag_value})/)
            |> filter(fn: (r) => r["_field"] =~ /({fields})/)
            |> aggregateWindow(every: {every}, fn: {fn})
            |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
        """,
        "sampled_data": """
            from(bucket: "{bucket}")
            |> range(start: {start}T00:00:00Z, stop: {stop}T23:59:59Z)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["ticker"] =~ /({tickers})/)
            |> filter(fn: (r) => r["_field"] =~ /({fields})/)
            |> sample(n: {sample_size})
            |> pivot(rowKey: ["_time", "ticker"], columnKey: ["_field"], valueColumn: "_value")
        """
    }

    # Prompt guide for LLM parameter selection
    LLM_PROMPT_GUIDE = """
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

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        """Initialize with InfluxDB client and schema.

        Args:
            url: influxdb url, 
            token: influxdb token, 
            org: influxdb organization name, 
            bucket: influxdb bucket name
        """
        self.db = InfluxDBClient(
            url=url,
            token=token,
            org=org
        )
        self.data_bucket = bucket
        data_schema = DataSchemaGenerator(
            url=url,
            token=token,
            org=org
        )
        try:
            dir = os.path.dirname(__file__)
            with open(os.path.join(dir, 'influxdb_schema.json'), 'r', encoding='utf-8') as file:
                self.schema = json.load(file)
        except FileNotFoundError as e:
            self.schema = data_schema.generate_schema(bucket)

    def _format_tickers(self, tickers: List[Dict[str, Any]]) -> str:
        """Formats ticker list for LLM input.

        Args:
            tickers: List of dict containing tickers with asset_class and market parameter (e.g., [{"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"},
                    {"ticker": "NVDA", "asset_class": "stock", "market": "NASDAQ"}]

        Returns:
            Well Formatted  ticker list str for LLM input. 
        """
        output = []
        for item in tickers:
            output.append(
                f"        - (ticker: {item['ticker']}, asset_class: {item['asset_class']}, market: {item['market']})"
            )
        return "\n".join(output)

    def get_llm_prompt(self, tickers: List[Dict[str, Any]], timeframe: str, analysis_focus: str) -> str:
        """Generate prompt for LLM to select query parameters.

        Args:
            timeframe: Time range for queries (e.g., '2015-01-01 to 2019-12-31').
            analysis_focus: Analysis focus (e.g., 'fundamental_technical', 'market_sentiment').

        Returns:
            Formatted prompt string for LLM.
        """
        return self.LLM_PROMPT_GUIDE.format(
            tickers = self._format_tickers(tickers),
            schema=json.dumps(self.schema, indent=2),
            patterns_json=json.dumps(list(self.QUERY_PATTERNS.keys()), indent=2),
            analysis_focus=analysis_focus,
            timeframe=timeframe
        )

    def execute_query(self, query_pattern: str, params: Dict[str, Any]) -> pd.DataFrame:
        """Execute query with provided pattern and parameters.

        Args:
            query_pattern: Name of the query pattern (e.g., 'time_series').
            params: Dictionary of query parameters.

        Returns:
            DataFrame containing query results, or empty DataFrame if query fails.
        """
        if query_pattern not in self.QUERY_PATTERNS:
            return pd.DataFrame()

        try:
            query = self.QUERY_PATTERNS[query_pattern].format(
                bucket=self.data_bucket,
                measurement=params.get("measurement", ""),
                tickers="|".join(params.get("tickers", [])),
                fields="|".join(params.get("fields", [])),
                start=params.get("start", "2000-01-01"),
                stop=params.get("stop", datetime.now().strftime("%Y-%m-%d")),
                every=params.get("every", "1d"),
                fn=params.get("fn", "mean"),
                tag_key=params.get("tag_key", "ticker"),
                tag_value=params.get("tag_value", ".*"),
                sample_size=params.get("sample_size", 1000)
            )
            result = self.db.query_api().query_data_frame(query)
            df = pd.DataFrame(result)
            if "_time" in df.columns:
                df["_time"] = pd.to_datetime(df["_time"])
            return df.drop_duplicates()
        except Exception as e:
            return pd.DataFrame()

    def process_llm_query_request(self, llm_response: str) -> List[Dict[str, Any]]:
        """Process LLM-generated query groups, execute queries, and return JSON with markdown results.

        Args:
            llm_response: JSON string containing query groups from LLM.

        Returns:
            List of dict (query results in markdown format).
        """
        try:
            # Parse the LLM response as a JSON array of query groups
            query_groups = json.loads(re.search(r"\[.*\]", llm_response, re.DOTALL).group())
            result_groups = []

            # Iterate through each query group and its queries
            for group in query_groups:
                group_name = group.get("group_name", "Unnamed Group")
                explanation = group.get("explanation", "")
                associated_tickers = group.get("associated_tickers", [])
                queries = []

                for query in group.get("queries", []):
                    heading = query.get("heading", "Unnamed Query")
                    query_pattern = query.get("query_pattern")
                    query_params = query.get("parameters", {})

                    # Execute the query
                    df = self.execute_query(query_pattern, query_params)
                    # Convert query result to markdown
                    markdown_result = self.summarize_data(df)

                    # Create query result structure
                    query_result = {
                        "heading": heading,
                        "query_pattern": query_pattern,
                        "result": markdown_result
                    }
                    queries.append(query_result)

                # Create group result structure
                group_result = {
                    "group_name": group_name,
                    "associated_tickers": associated_tickers,
                    "explanation": explanation,
                    "queries": queries
                }
                result_groups.append(group_result)
            return result_groups

        except Exception as e:
            print(e)
            return []

    def summarize_data(self, df: pd.DataFrame) -> str:
        """Summarize DataFrame for LLM hypothesis generation.

        Args:
            df: DataFrame containing query results.

        Returns:
            Markdown string of the DataFrame or a message if empty.
        """
        if df.empty:
            return "No data available."
        summary = df.to_markdown()
        return summary