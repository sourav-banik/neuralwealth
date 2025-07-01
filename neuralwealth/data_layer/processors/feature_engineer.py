import pandas as pd
import talib
from typing import Tuple

class FeatureEngineer:
    """
    Engine that adds technical analysis features to financial market data using TA-Lib and custom calculations.
    """
    @staticmethod
    def _calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """
        Calculates Volume Weighted Average Price (VWAP) for the given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with columns 'High', 'Low', 'Close', 'Volume'.

        Returns:
            pd.Series: VWAP values aligned with the DataFrame's index.
        """
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        volume_sum = df['Volume'].cumsum()
        price_volume = (typical_price * df['Volume']).cumsum()
        vwap = price_volume / volume_sum
        return vwap

    @staticmethod
    def _calculate_pivot_points(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculates classic Pivot Points, Support 1 (S1), and Resistance 1 (R1) for the given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with columns 'High', 'Low', 'Close'.

        Returns:
            Tuple[pd.Series, pd.Series, pd.Series]: Pivot Point, Support 1, and Resistance 1 series.
        """
        pivot = (df['High'] + df['Low'] + df['Close']) / 3
        support1 = (2 * pivot) - df['High']
        resistance1 = (2 * pivot) - df['Low']
        return pivot, support1, resistance1

    @staticmethod
    def add_ta_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds technical indicators to the input DataFrame for trading analysis, covering Trend, Momentum, Volume, Volatility, and Support/Resistance.

        Args:
            df (pd.DataFrame): Input DataFrame with columns 'Open', 'High', 'Low', 'Close', 'Volume'.

        Returns:
            pd.DataFrame: DataFrame with added technical indicator columns.

        Raises:
            ValueError: If required columns are missing or insufficient data is provided.
        """
        # Validate input DataFrame
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        if len(df) < 14:  # Minimum for RSI and other indicators
            raise ValueError("DataFrame must have at least 14 rows for technical indicators")

        # Create a copy to avoid modifying the input
        result = df.copy()

        # Trend Indicators
        result['SMA_20'] = talib.SMA(result['Close'], timeperiod=20)
        result['EMA_20'] = talib.EMA(result['Close'], timeperiod=20)
        result['ADX'] = talib.ADX(result['High'], result['Low'], result['Close'], timeperiod=14)

        # Momentum Indicators
        result['RSI'] = talib.RSI(result['Close'], timeperiod=14)
        result['MACD'], _, _ = talib.MACD(result['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
        result['STOCH_K'], result['STOCH_D'] = talib.STOCH(
            result['High'], result['Low'], result['Close'],
            fastk_period=14, slowk_period=3, slowd_period=3
        )

        # Volume Indicators
        result['OBV'] = talib.OBV(result['Close'], result['Volume'])
        result['VWAP'] = FeatureEngineer._calculate_vwap(result)

        # Volatility Indicators
        result['BB_UPPER'], result['BB_MIDDLE'], result['BB_LOWER'] = talib.BBANDS(
            result['Close'], timeperiod=20, nbdevup=2, nbdevdn=2
        )
        result['ATR'] = talib.ATR(result['High'], result['Low'], result['Close'], timeperiod=14)

        # Support/Resistance Indicators
        pivot, support1, resistance1 = FeatureEngineer._calculate_pivot_points(result)
        result['PIVOT'] = pivot
        result['S1'] = support1
        result['R1'] = resistance1

        return result