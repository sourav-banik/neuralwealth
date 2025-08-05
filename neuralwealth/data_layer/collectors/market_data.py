import yfinance as yf
import pandas as pd
from typing import Dict

class MarketDataCollector:
    """A class to collect market data from yfinance for a given ticker.

    Collects OHLCV (Open, High, Low, Close, Volume) data and basic information
    such as exchange, company name, and other metadata. Ensures output DataFrame
    columns are in lowercase with underscores (e.g., 'stock_splits') and resets
    the index with the 'Date' column renamed to 'time'.

    Attributes:
        yf_client: The yfinance module used to fetch market data.
    """

    def __init__(self):
        """Initialize the MarketDataCollector with a yfinance client."""
        self.yf_client = yf

    def get_basic_info(self, ticker: str) -> Dict:
        """Fetch basic information for a given ticker.

        Args:
            ticker (str): Yahoo Finance ticker symbol (e.g., 'AAPL').

        Returns:
            Dict: Dictionary containing basic data for the ticker, such as exchange,
                  company name, directors, 52-week high/low, etc.

        Raises:
            ValueError: If the yfinance API call fails.
        """
        try:
            data = self.yf_client.Ticker(ticker)
            return data.info
        except Exception as e:
            raise ValueError(f"yFinance failed: {str(e)}")

    def get_market_data(self, ticker: str, period: str = "1mo") -> pd.DataFrame:
        """Fetch OHLCV data for a ticker with standardized column names and reset index.

        Args:
            ticker (str): Yahoo Finance ticker symbol (e.g., 'AAPL').
            period (str, optional): Period of data to fetch. Valid options include
                '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd',
                'max'. Defaults to '1mo'.

        Returns:
            pandas.DataFrame: DataFrame containing historical OHLCV data with
                lowercase, underscore-separated column names (e.g., 'open',
                'stock_splits') and the index reset with 'Date' renamed to 'time'.

        Raises:
            ValueError: If the yfinance API call fails.
        """
        try:
            data = self.yf_client.Ticker(ticker)
            history = data.history(period)
            history.columns = [
                col.lower().replace(" ", "_") for col in history.columns
            ]
            history.reset_index(inplace=True)
            history.rename(columns={'Date': 'time'}, inplace=True)
            return history
        except Exception as e:
            raise ValueError(f"yFinance failed: {str(e)}")