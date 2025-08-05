import pandas as pd
import talib
from typing import Tuple

class FeatureEngineer:
    """Engine that adds technical analysis features to financial market data using TA-Lib and custom calculations."""

    @staticmethod
    def _calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """
        Calculates Volume Weighted Average Price (VWAP) using lagged data to prevent look-ahead bias.

        Args:
            df (pd.DataFrame): DataFrame with columns 'high', 'low', 'close', 'volume'.

        Returns:
            pd.Series: VWAP values aligned with the DataFrame's index.
        """
        df_shifted = df.shift(1)
        typical_price = (df_shifted['high'] + df_shifted['low'] + df_shifted['close']) / 3
        volume_sum = df_shifted['volume'].cumsum()
        price_volume = (typical_price * df_shifted['volume']).cumsum()
        vwap = price_volume / volume_sum
        return vwap

    @staticmethod
    def _calculate_pivot_points(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculates classic Pivot Points, Support 1 (S1), and Resistance 1 (R1) using lagged data.

        Args:
            df (pd.DataFrame): DataFrame with columns 'high', 'low', 'close'.

        Returns:
            Tuple[pd.Series, pd.Series, pd.Series]: Pivot Point, Support 1, and Resistance 1 series.
        """
        df_shifted = df.shift(1)
        pivot = (df_shifted['high'] + df_shifted['low'] + df_shifted['close']) / 3
        support1 = (2 * pivot) - df_shifted['high']
        resistance1 = (2 * pivot) - df_shifted['low']
        return pivot, support1, resistance1

    @staticmethod
    def _calculate_hma(df: pd.DataFrame, period: int = 9) -> pd.Series:
        """
        Calculates Hull Moving Average (HMA) for trend analysis with reduced lag.

        Args:
            df (pd.DataFrame): DataFrame with 'close' column.
            period (int): Lookback period for HMA (default: 9).

        Returns:
            pd.Series: HMA values.
        """
        wma1 = 2 * talib.WMA(df['close'], timeperiod=period // 2)
        wma2 = talib.WMA(df['close'], timeperiod=period)
        raw_hma = wma1 - wma2
        hma = talib.WMA(raw_hma, timeperiod=int(period ** 0.5))
        return hma

    @staticmethod
    def _calculate_keltner_channels(
        df: pd.DataFrame, 
        ema_period: int = 20, 
        atr_period: int = 14, 
        multiplier: float = 2.0
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculates Keltner Channels using EMA and ATR.

        Args:
            df (pd.DataFrame): DataFrame with columns 'high', 'low', 'close'.
            ema_period (int): Period for EMA (default: 20).
            atr_period (int): Period for ATR (default: 14).
            multiplier (float): Multiplier for ATR (default: 2.0).

        Returns:
            Tuple[pd.Series, pd.Series, pd.Series]: Upper, Middle, and Lower Keltner Channels.
        """
        middle = talib.EMA(df['close'], timeperiod=ema_period)
        atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=atr_period)
        upper = middle + (multiplier * atr)
        lower = middle - (multiplier * atr)
        return upper, middle, lower

    @staticmethod
    def _calculate_cmo(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculates Chande Momentum Oscillator (CMO) for momentum analysis.

        Args:
            df (pd.DataFrame): DataFrame with 'close' column.
            period (int): Lookback period for CMO (default: 14).

        Returns:
            pd.Series: CMO values.
        """
        return talib.CMO(df['close'], timeperiod=period)

    @staticmethod
    def add_ta_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds technical indicators to the input DataFrame for trading analysis, covering trend, momentum, 
        volume, volatility, and support/resistance.

        Args:
            df (pd.DataFrame): Input DataFrame with columns 'open', 'high', 'low', 'close', 'volume', and 'time' as datetime index.

        Returns:
            pd.DataFrame: DataFrame with added technical indicator columns in snake_case.

        Raises:
            ValueError: If required columns are missing or insufficient data is provided.
        """
        # Validate input DataFrame
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        if len(df) < 26:
            raise ValueError("DataFrame must have at least 26 rows for technical indicators")
        if 'time' not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have a 'time' column or datetime index")

        # Create a copy to avoid modifying the input
        result = df.copy()

        # Set datetime index if 'time' column exists
        if 'time' in result.columns:
            result.set_index('time', inplace=True)

        # --- Trend Indicators ---
        result['sma_20'] = talib.SMA(result['close'], timeperiod=20)
        result['ema_20'] = talib.EMA(result['close'], timeperiod=20)
        result['hma_9'] = FeatureEngineer._calculate_hma(result, period=9)
        result['adx_14'] = talib.ADX(result['high'], result['low'], result['close'], timeperiod=14)
        # Multi-timeframe (daily) SMA
        daily_df = result.resample('D').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        })
        sma_20_daily = pd.Series(talib.SMA(daily_df['close'], timeperiod=20), index=daily_df.index).reindex(result.index, method='ffill')
        result['sma_20_daily'] = sma_20_daily

        # --- Momentum Indicators ---
        result['rsi_14'] = talib.RSI(result['close'], timeperiod=14)
        rsi_mean = result['rsi_14'].rolling(100, min_periods=1).mean()
        rsi_std = result['rsi_14'].rolling(100, min_periods=1).std().clip(lower=1e-6)
        result['rsi_z_score'] = (result['rsi_14'] - rsi_mean) / rsi_std
        result['rsi_z_score'] = result['rsi_z_score'].clip(lower=-3, upper=3).fillna(0)
        result['macd'], _, _ = talib.MACD(result['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        result['stoch_k'], result['stoch_d'] = talib.STOCH(
            result['high'], result['low'], result['close'],
            fastk_period=14, slowk_period=3, slowd_period=3
        )
        result['cmo_14'] = FeatureEngineer._calculate_cmo(result, period=14)

        # --- Volume Indicators ---
        result['obv'] = talib.OBV(result['close'], result['volume'])
        result['vwap'] = FeatureEngineer._calculate_vwap(result)

        # --- Volatility Indicators ---
        result['bb_upper'], result['bb_middle'], result['bb_lower'] = talib.BBANDS(
            result['close'], timeperiod=20, nbdevup=2, nbdevdn=2
        )
        result['atr_14'] = talib.ATR(result['high'], result['low'], result['close'], timeperiod=14)
        result['kc_upper'], result['kc_middle'], result['kc_lower'] = FeatureEngineer._calculate_keltner_channels(
            result, ema_period=20, atr_period=14, multiplier=2.0
        )

        # --- Support/Resistance Indicators ---
        pivot, support1, resistance1 = FeatureEngineer._calculate_pivot_points(result)
        result['pivot'] = pivot
        result['s1'] = support1
        result['r1'] = resistance1

        return result