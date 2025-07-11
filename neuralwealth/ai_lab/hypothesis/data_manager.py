import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from influxdb_client import InfluxDBClient
import re

class InfluxDBQueryManager:
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
    You are a financial analyst selecting parameters for InfluxDB queries to support hypothesis generation.
    Schema: {schema}
    Available query patterns: {query_patterns}

    Based on the analysis focus (e.g., fundamental_technical, market_sentiment), select parameters for the query:
    - **measurement**: Choose from {measurements}.
    - **fields**: Select relevant fields (e.g., 'Close', 'Volume' for market_info; 'roe', 'net_profit_margin' for financial_ratios).
    - **tickers**: List of assets (e.g., 'AAPL|MSFT|NVDA').
    - **start/stop**: Timeframe (e.g., 2015-01-01, 2019-12-31).
    - **every**: Aggregation window (e.g., '1d', '1w', '1mo').
    - **fn**: Aggregation function (e.g., 'mean', 'last', 'sum').
    - **tag_key/tag_value**: For filtered_by_tag pattern (e.g., tag_key='asset_class', tag_value='equity').
    - **sample_size**: For sampled_data pattern (e.g., 1000).

    Guidelines:
    - For fundamental_technical: Use 'financial_ratios' (e.g., 'roe', 'current_ratio'), 'market_info' (e.g., 'Close', 'RSI'), or 'income_statement' (e.g., 'net_income'). Prefer monthly ('1mo') or quarterly ('3mo') aggregation.
    - For market_sentiment: Use 'news_sentiment' (e.g., 'score') with daily ('1d') or weekly ('1w') aggregation.
    - For macroeconomic analysis: Use 'macro_data' (e.g., 'CPI Inflation', 'Real GDP') with monthly ('1mo') aggregation.
    - Ensure fields exist in the measurement's schema.
    - Use 'mean' for continuous data (e.g., 'Close'), 'last' for point-in-time data (e.g., 'total_assets'), 'sum' for cumulative data (e.g., 'Volume').
    - Limit sample_size to 1000-5000 to manage data volume.
    - Adjust timeframe to match measurement availability (e.g., 2009-01-31 to 2025-01-31 for 'balance_sheet').

    Return JSON:
    ```json
    {
        "query_pattern": "pattern_name",
        "parameters": {
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
        }
    }
    ```
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize with InfluxDB client and schema."""
        self.db = InfluxDBClient(
            url=config["influxdb_url"],
            token=config["influxdb_token"],
            org=config["influxdb_org"],
            bucket=config["influxdb_bucket"]
        )
        self.schema = self.discover_schema()

    def discover_schema(self) -> Dict[str, Any]:
        """Dynamically discover InfluxDB schema."""
        query = f"""
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{self.db.bucket}")
        """
        measurements = self.db.query_api().query_data_frame(query)['_value'].tolist()
        schema = {}
        for measurement in measurements:
            query = f"""
            import "influxdata/influxdb/schema"
            schema.measurementTagKeys(bucket: "{self.db.bucket}", measurement: "{measurement}")
            """
            tags = self.db.query_api().query_data_frame(query)['_value'].tolist()
            query = f"""
            import "influxdata/influxdb/schema"
            schema.measurementFieldKeys(bucket: "{self.db.bucket}", measurement: "{measurement}")
            """
            fields = self.db.query_api().query_data_frame(query)['_value'].tolist()
            schema[measurement] = {"tags": tags, "fields": fields}
        return schema

    def get_llm_prompt(self, analysis_focus: str) -> str:
        """Generate prompt for LLM to select query parameters."""
        return self.LLM_PROMPT_GUIDE.format(
            schema=json.dumps(self.schema, indent=2),
            query_patterns=json.dumps(list(self.QUERY_PATTERNS.keys()), indent=2),
            measurements=json.dumps(list(self.schema.keys()), indent=2)
        )

    def validate_parameters(self, params: Dict[str, Any]) -> bool:
        """Validate query parameters against schema."""
        measurement = params.get("measurement")
        fields = params.get("fields", [])
        tag_key = params.get("tag_key")
        
        if measurement not in self.schema:
            return False
        if not all(field in self.schema[measurement]["fields"] for field in fields):
            return False
        if tag_key and tag_key not in self.schema[measurement]["tags"]:
            return False
        return True

    def execute_query(self, query_pattern: str, params: Dict[str, Any]) -> pd.DataFrame:
        """Execute query with provided pattern and parameters."""
        if query_pattern not in self.QUERY_PATTERNS:
            return pd.DataFrame()
        
        if not self.validate_parameters(params):
            return pd.DataFrame()
        
        try:
            query = self.QUERY_PATTERNS[query_pattern].format(
                bucket=self.db.bucket,
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
            if '_time' in df.columns:
                df['_time'] = pd.to_datetime(df['_time'])
            return df.drop_duplicates()
        except Exception as e:
            return pd.DataFrame()

    def process_llm_query_request(self, llm_response: str) -> pd.DataFrame:
        """Process LLM-generated query parameters and execute query."""
        try:
            params = json.loads(re.search(r'\{.*\}', llm_response, re.DOTALL).group())
            query_pattern = params.get("query_pattern")
            query_params = params.get("parameters", {})
            return self.execute_query(query_pattern, query_params)
        except Exception as e:
            return pd.DataFrame()

    def summarize_data(self, df: pd.DataFrame) -> str:
        """Summarize DataFrame for LLM hypothesis generation."""
        if df.empty:
            return "No data available."
        summary = df.describe().to_markdown()
        sample = df.head(5).to_markdown()
        return f"## Data Summary\n{summary}\n## Sample Data\n{sample}"