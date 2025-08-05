import json
import os
import re
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from influxdb_client import InfluxDBClient
from neuralwealth.ai_lab.hypothesis.resources.llm_research_prompt import LLM_RESEARCH_PROMPT
from neuralwealth.ai_lab.hypothesis.resources.research_query_patterns import RESEARCH_QUERY_PATTERNS
from neuralwealth.ai_lab.utils.data_schema_generator import DataSchemaGenerator

class HypothesisInitializer:
    """Manages InfluxDB queries and initializes data for investment hypothesis generation."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        """Initialize with InfluxDB client and schema.

        Args:
            url: InfluxDB URL.
            token: InfluxDB authentication token.
            org: InfluxDB organization name.
            bucket: InfluxDB bucket name.
        """
        self.db = InfluxDBClient(url=url, token=token, org=org)
        self.data_bucket = bucket
        data_schema = DataSchemaGenerator(url=url, token=token, org=org)
        try:
            dir = os.path.dirname(__file__)
            with open(os.path.join(dir, 'resources/influxdb_schema.json'), 'r', encoding='utf-8') as file:
                self.schema = json.load(file)
        except FileNotFoundError:
            self.schema = data_schema.generate_schema(bucket)
        self.QUERY_PATTERNS = RESEARCH_QUERY_PATTERNS

    def _format_tickers(self, tickers: List[Dict[str, Any]]) -> str:
        """Formats ticker list for LLM input.

        Args:
            tickers: List of dict containing tickers with asset_class and market parameter.

        Returns:
            Formatted ticker list string for LLM input.
        """
        output = []
        for item in tickers:
            output.append(
                f"        - (ticker: {item['ticker']}, asset_class: {item['asset_class']}, market: {item['market']})"
            )
        return "\n".join(output)

    def get_llm_prompt(
        self, 
        tickers: List[Dict[str, Any]], 
        timeframe: str, 
        analysis_focus: str
    ) -> str:
        """Generate prompt for LLM to select query parameters.

        Args:
            tickers: List of ticker dictionaries.
            timeframe: Time range for queries (e.g., '2015-01-01 to 2019-12-31').
            analysis_focus: Analysis focus (e.g., 'fundamental_technical', 'market_sentiment').

        Returns:
            Formatted prompt string for LLM.
        """
        return LLM_RESEARCH_PROMPT.format(
            tickers=self._format_tickers(tickers),
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
            query = RESEARCH_QUERY_PATTERNS[query_pattern].format(
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
        except Exception:
            return pd.DataFrame()

    def process_llm_query_request(self, llm_response: str) -> List[Dict[str, Any]]:
        """Process LLM-generated query groups, execute queries, and return JSON with markdown results.

        Args:
            llm_response: JSON string containing query groups from LLM.

        Returns:
            List of dictionaries (query results in markdown format).
        """
        try:
            query_groups = json.loads(re.search(r"\[.*\]", llm_response, re.DOTALL).group())
            result_groups = []

            for group in query_groups:
                group_name = group.get("group_name", "Unnamed Group")
                explanation = group.get("explanation", "")
                associated_tickers = group.get("associated_tickers", [])
                queries = []

                for query in group.get("queries", []):
                    heading = query.get("heading", "Unnamed Query")
                    query_pattern = query.get("query_pattern")
                    query_params = query.get("parameters", {})

                    df = self.execute_query(query_pattern, query_params)
                    markdown_result = self.summarize_data(df)

                    query_result = {
                        "heading": heading,
                        "query_pattern": query_pattern,
                        "result": markdown_result
                    }
                    queries.append(query_result)

                group_result = {
                    "group_name": group_name,
                    "associated_tickers": associated_tickers,
                    "explanation": explanation,
                    "queries": queries
                }
                result_groups.append(group_result)
            return result_groups

        except Exception:
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
        return df.to_markdown()