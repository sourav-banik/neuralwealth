from typing import Dict, Any, List

# Scraper modules
from neuralwealth.data_layer.collectors.market_data import MarketDataCollector
from neuralwealth.data_layer.collectors.news_sentiment import NewsSentimentCollector
from neuralwealth.data_layer.collectors.macro_data import FREDCollector
from neuralwealth.data_layer.collectors.financials_data import FinancialsCollector
from neuralwealth.data_layer.collectors.ticker_collector import TickerCollector
from neuralwealth.data_layer.processors.cleaner import MarketDataCleaner
from neuralwealth.data_layer.processors.feature_engineer import FeatureEngineer

# Storage module
from neuralwealth.data_layer.storage.influxdb_storage import InfluxDBStorage

# Test tickers
from neuralwealth.data_layer.test_tickers import dummy_tickers

# Utils
from neuralwealth.data_layer.utils.yahoo_finance import get_yahoo_symbol

class DataPipeline:
    """Manages the data collection and storage pipeline for financial data."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the data pipeline with collectors, processors, and storage.

        Args:
            config (Dict[str, Any]): Configuration dictionary containing environment settings,
                InfluxDB credentials, Twitter bearer token, and FRED API key.
        """
        self.env = config['env']
        
        # Initialize data collectors
        self.market_collector = MarketDataCollector()
        self.news_collector = NewsSentimentCollector(config["twitter_bearer_token"])
        self.fred_collector = FREDCollector(config["fred_api_key"])
        self.financials_collector = FinancialsCollector()
        self.ticker_collector = TickerCollector()
        self.market_data_cleaner = MarketDataCleaner()
        self.feature_engineer = FeatureEngineer()
        
        # Initialize InfluxDB storage
        self.db_client = InfluxDBStorage(
            url=config["influxdb_url"],
            token=config["influxdb_token"],
            org=config["influxdb_org"],
            bucket=config["influxdb_bucket"]
        )

    def collect_pipeline_tickers(self) -> List[Dict[str, Any]]:
        """
        Retrieves ticker data based on environment setting.

        Returns:
            List[Dict[str, Any]]: List of ticker dictionaries with details.
                In production, returns all tickers; in testing, returns sample tickers.
        """
        return self.ticker_collector.collect_tickers() if self.env == 'production' else dummy_tickers

    def _process_ticker_data(self, ticker: Dict[str, Any]) -> None:
        """
        Processes and stores data for a single ticker, including basic info, market data,
        financials (for stocks), and news sentiment.

        Args:
            ticker (Dict[str, Any]): Ticker dictionary with 'ticker', 'asset_class', 'market', and 'symbol' keys.
        """
        # Convert ticker to Yahoo Finance symbol
        yf_symbol = get_yahoo_symbol(
            trading_symbol=ticker['ticker'],
            asset_class=ticker['asset_class'],
            market=ticker['market']
        )

        # Collect and store basic info
        basic_data = self.market_collector.get_basic_info(yf_symbol)
        self.db_client.write_unstructured(measurement="basic_info", data=basic_data, metadata=ticker)

        # Collect and store market data
        market_df = self.market_collector.get_market_data(yf_symbol, period="max")
        market_df = self.market_data_cleaner.clean_data(market_df)
        market_df = self.feature_engineer.add_ta_features(market_df)
        market_df.reset_index(inplace=True)
        # Preprocess DataFrame to ensure consistent numeric types
        market_df = self.db_client.preprocess_dataframe(market_df, "time")
        market_df['ticker'] = ticker['ticker']
        market_df['asset_class'] = ticker['asset_class']
        market_df['market'] = ticker['market']
        self.db_client.write_dataframe(
            df=market_df,
            measurement="market_info",
            tag_columns=['ticker', 'asset_class', 'market'],
            time_col='time'
        )

        # Collect and store financials for stocks
        if ticker['asset_class'] == 'stock':
            financials = self.financials_collector.get_financials(ticker['ticker'])
            for statement_type, df in financials.items():
                if not df.empty:
                    time_col = 'time' if '' in df.columns else None
                    if time_col:
                        df = df.rename(columns={'': 'time'})
                    # Preprocess DataFrame to ensure consistent numeric types
                    df = self.db_client.preprocess_dataframe(df, time_col)
                    df['ticker'] = ticker['ticker']
                    df['asset_class'] = ticker['asset_class']
                    df['market'] = ticker['market']
                    self.db_client.write_dataframe(
                        df=df,
                        measurement=statement_type,
                        tag_columns=['ticker', 'asset_class', 'market'],
                        time_col=time_col
                    )
        # Collect and store news sentiment
        news_df = self.news_collector.scrape_news_sentiment(yf_symbol)
        news_df = news_df.rename(columns={'timestamp': 'time'})
        news_df['ticker'] = ticker['ticker']
        news_df['asset_class'] = ticker['asset_class']
        news_df['market'] = ticker['market']
        self.db_client.write_dataframe(
            df=news_df,
            measurement="news_sentiment",
            tag_columns=['ticker', 'asset_class', 'market'],
            time_col='time'
        )

    def run_pipeline(self, tickers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Executes the full data pipeline, processing macro data and ticker-specific data.

        Args:
            tickers (List[Dict[str, Any]]): List of ticker dictionaries to process.

        Returns:
            Dict[str, Any]: Status dictionary with processing results, including the number
                of macro indicators and tickers processed.
        """
        try:
            # Process macro data in batches
            macro_data = self.fred_collector.fetch_all()
            for series_name, df in macro_data.items():
                if not df.empty:
                    df.columns = [series_name]
                    df.index.name = "time"
                    df.reset_index(inplace=True)
                    self.db_client.write_dataframe(
                        df,
                        "macro_data",
                        [],
                        "time"
                    )
        
            # Process ticker data
            for ticker in tickers:
                self._process_ticker_data(ticker)

            return {
                "status": "success",
                "macro_indicators_processed": len(macro_data),
                "tickers_processed": len(tickers)
            }
        
        finally:
            # Ensure InfluxDB client is closed to flush writes and prevent shutdown errors
            self.db_client.close()