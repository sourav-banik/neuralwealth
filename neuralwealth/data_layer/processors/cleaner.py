import pandas as pd

class MarketDataCleaner:
    """Utility class for cleaning financial market data, handling missing values, outliers, and normalization."""

    @staticmethod
    def _validate_price_consistency(df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures price consistency: high >= close >= low and high >= open >= low.

        Args:
            df (pd.DataFrame): DataFrame with columns 'open', 'high', 'low', 'close'.

        Returns:
            pd.DataFrame: DataFrame with corrected price values.
        """
        df = df.copy()
        df['high'] = df[['open', 'high', 'low', 'close']].max(axis=1)
        df['low'] = df[['open', 'high', 'low', 'close']].min(axis=1)
        df['open'] = df['open'].clip(lower=df['low'], upper=df['high'])
        df['close'] = df['close'].clip(lower=df['low'], upper=df['high'])
        return df

    @staticmethod
    def _handle_missing_values(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Handles missing values in specified columns using interpolation, forward-fill, and backfill.

        Args:
            df (pd.DataFrame): DataFrame to clean.
            columns (list): Columns to handle missing values for.

        Returns:
            pd.DataFrame: DataFrame with missing values handled.
        """
        df = df.copy()
        for col in columns:
            df[col] = df[col].replace([float('inf'), -float('inf')], pd.NA)
            df[col] = df[col].interpolate(method='linear', limit_direction='both').ffill().bfill()
        return df

    @staticmethod
    def _handle_outliers(df: pd.DataFrame, columns: list, lower_percentile: float = 0.01, upper_percentile: float = 0.99) -> pd.DataFrame:
        """
        Caps outliers in specified columns using percentile-based bounds.

        Args:
            df (pd.DataFrame): DataFrame to clean.
            columns (list): Columns to handle outliers for.
            lower_percentile (float): Lower percentile for clipping (default: 0.01).
            upper_percentile (float): Upper percentile for clipping (default: 0.99).

        Returns:
            pd.DataFrame: DataFrame with outliers capped.
        """
        df = df.copy()
        for col in columns:
            lower_bound = df[col].quantile(lower_percentile)
            upper_bound = df[col].quantile(upper_percentile)
            df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        return df

    @staticmethod
    def _normalize_min_max(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Normalizes specified columns to [0, 1] range using min-max scaling.

        Args:
            df (pd.DataFrame): DataFrame to normalize.
            columns (list): Columns to normalize.

        Returns:
            pd.DataFrame: DataFrame with normalized columns (e.g., 'open_norm').
        """
        df = df.copy()
        for col in columns:
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val > min_val:
                df[f'{col}_norm'] = (df[col] - min_val) / (max_val - min_val)
            else:
                df[f'{col}_norm'] = 0.0
        return df

    @staticmethod
    def _normalize_z_score(df: pd.DataFrame, columns: list, window: int = 100) -> pd.DataFrame:
        """
        Normalizes specified columns using z-score (mean=0, std=1) over a rolling window.

        Args:
            df (pd.DataFrame): DataFrame to normalize.
            columns (list): Columns to normalize.
            window (int): Rolling window for mean and std (default: 100).

        Returns:
            pd.DataFrame: DataFrame with z-score normalized columns (e.g., 'open_z_score').
        """
        df = df.copy()
        for col in columns:
            rolling_mean = df[col].rolling(window=window, min_periods=1).mean()
            rolling_std = df[col].rolling(window=window, min_periods=1).std().fillna(1.0)
            df[f'{col}_z_score'] = (df[col] - rolling_mean) / rolling_std
        return df

    @staticmethod
    def clean_data(df: pd.DataFrame, normalize: bool = True, z_score: bool = True) -> pd.DataFrame:
        """
        Cleans and normalizes OHLCV data by handling missing values, outliers, and scaling prices.

        Args:
            df (pd.DataFrame): Input DataFrame with columns 'open', 'high', 'low', 'close', and optionally 'volume', 'time'.
            normalize (bool): Whether to apply min-max normalization (default: True).
            z_score (bool): Whether to apply z-score normalization (default: True).

        Returns:
            pd.DataFrame: Cleaned and normalized DataFrame with same index and additional normalized columns.

        Raises:
            ValueError: If required columns are missing or DataFrame has fewer than 26 rows.
        """
        required_columns = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        if len(df) < 26:
            raise ValueError("DataFrame must have at least 26 rows for cleaning and technical analysis")
        if 'time' not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have a 'time' column or datetime index")

        result = df.copy()

        if 'time' in result.columns:
            result.set_index('time', inplace=True)

        ohlcv_columns = ['open', 'high', 'low', 'close', 'volume'] if 'volume' in result.columns else ['open', 'high', 'low', 'close']
        result = MarketDataCleaner._handle_missing_values(result, ohlcv_columns)
        result = MarketDataCleaner._validate_price_consistency(result)

        if 'volume' in result.columns:
            result['volume'] = result['volume'].where(result['volume'] >= 0, pd.NA)
            result['volume'] = result['volume'].interpolate(method='linear', limit_direction='both').ffill().bfill()
            volume_upper = result['volume'].quantile(0.99)
            result['volume'] = result['volume'].astype(float).clip(upper=volume_upper).infer_objects(copy=False)

        result = MarketDataCleaner._handle_outliers(result, ['open', 'high', 'low', 'close'])

        if normalize:
            result = MarketDataCleaner._normalize_min_max(result, ['open', 'high', 'low', 'close'])

        if z_score:
            result = MarketDataCleaner._normalize_z_score(result, ['open', 'high', 'low', 'close'])

        return result