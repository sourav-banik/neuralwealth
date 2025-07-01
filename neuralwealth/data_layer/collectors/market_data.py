import yfinance as yf
import pandas as pd
from typing import Dict

class MarketDataCollector:
    """
    Engine that collects market data from sources (yfinance).
    - OHLCV data
    - Basic data related to exchange, company, trading etc.
    """
    def __init__(self):
        self.yf_client = yf

    def get_basic_info(self, ticker: str) -> Dict:
        """
        Fetch basic data for ticker

        Args:
            ticker: Yahoo finance symbol
        
        Returns:
            Dict containing basic data of the symbol i.e - exchange, company name, directors, 52 weeks high, low etc.
        """
        try:
            data = self.yf_client.Ticker(ticker)
            return data.info
        except Exception as e:
            raise ValueError(f"yFinance failed: {str(e)}")

    def get_market_data(self, ticker: str, period: str = "1mo") -> pd.DataFrame:
        """
        Fetch OHLCV data for a ticker.

        Args:
            tickers: yahoo finance symbol
            market_data: Period of data ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
            
        Returns:
            Dataframe contains historical data for the ticker
        """
        try:
            data = self.yf_client.Ticker(ticker)
            history = data.history(period)
            return history
        except Exception as e:
            raise ValueError(f"yFinance failed: {str(e)}")
    