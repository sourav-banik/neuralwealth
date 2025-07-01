import pandas as pd
import numpy as np

class DataCleaner:
    """
    Utility class for cleaning financial market data, handling missing values, outliers, and normalization.
    """
    @staticmethod
    def normalize_prices(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and normalizes OHLCV data by handling missing values, outliers, and scaling prices.

        Args:
            df (pd.DataFrame): Input DataFrame with columns 'Open', 'High', 'Low', 'Close', and optionally 'Volume'.

        Returns:
            pd.DataFrame: Cleaned and normalized DataFrame with same index and additional normalized columns.

        Raises:
            ValueError: If required columns are missing or DataFrame has fewer than 10 rows.
        """
        # Validate input
        required_columns = ['Open', 'High', 'Low', 'Close']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        if len(df) < 10:
            raise ValueError("DataFrame must have at least 10 rows for cleaning")

        # Create a copy to avoid modifying the input
        df = df.copy()

        # Handle missing values: forward-fill then backfill
        df = df.ffill()
        df = df.bfill()

        # Clean Volume column if present
        if 'Volume' in df.columns:
            # Replace negative or zero volumes with a small positive value
            df['Volume'] = df['Volume'].where(df['Volume'] > 0, 1.0)
            # Forward-fill and backfill Volume
            df['Volume'] = df['Volume'].ffill()
            df['Volume'] = df['Volume'].bfill()

        # Remove outliers for OHLC: cap values beyond 3 standard deviations from rolling mean
        for col in ['Open', 'High', 'Low', 'Close']:
            rolling_mean = df[col].rolling(window=10, min_periods=1).mean()
            rolling_std = df[col].rolling(window=10, min_periods=1).std()
            # Fill NaN in rolling_std with a small value to avoid clipping issues
            rolling_std = rolling_std.fillna(1.0)
            upper_bound = rolling_mean + 3 * rolling_std
            lower_bound = rolling_mean - 3 * rolling_std
            df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)

        # Normalize OHLC prices to [0, 1] range
        for col in ['Open', 'High', 'Low', 'Close']:
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val > min_val:  # Avoid division by zero
                df[f'{col}_norm'] = (df[col] - min_val) / (max_val - min_val)
            else:
                df[f'{col}_norm'] = df[col] * 0.0  # If constant, set normalized to 0

        return df