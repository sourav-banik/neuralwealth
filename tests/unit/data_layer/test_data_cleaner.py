import pandas as pd
import numpy as np
from neuralwealth.data_layer.processors.cleaner import DataCleaner

# Sample DataFrame for testing
def create_sample_df(rows=20):
    return pd.DataFrame({
        'Open': [100 + i for i in range(rows)],
        'High': [102 + i for i in range(rows)],
        'Low': [98 + i for i in range(rows)],
        'Close': [101 + i for i in range(rows)],
        'Volume': [1000 + i * 100 for i in range(rows)]
    })

def test_normalize_prices_success():
    """Test normalize_prices cleans and normalizes OHLCV data."""
    df = create_sample_df()
    # Add some NaNs and outliers
    df.loc[5, 'Close'] = np.nan
    df.loc[10, 'High'] = 1000  # Outlier
    df.loc[15, 'Volume'] = -100  # Negative volume
    result = DataCleaner.normalize_prices(df)
    assert isinstance(result, pd.DataFrame), "Output is not a DataFrame"
    expected_columns = [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'Open_norm', 'High_norm', 'Low_norm', 'Close_norm'
    ]
    assert all(col in result.columns for col in expected_columns), "Missing expected columns"
    assert len(result) == len(df), "Output row count does not match input"
    assert not result.isna().any().any(), "NaN values remain in output"
    assert result['Volume'].min() > 0, "Negative or zero volumes remain"
    # Calculate expected cap for High at index 10
    rolling_mean = df['High'].rolling(window=10, min_periods=1).mean()
    rolling_std = df['High'].rolling(window=10, min_periods=1).std().fillna(1.0)
    upper_bound = rolling_mean + 3 * rolling_std
    assert result.loc[10, 'High'] <= upper_bound.loc[10], f"Outlier in High not capped: {result.loc[10, 'High']} > {upper_bound.loc[10]}"
    assert result['Open_norm'].between(0, 1).all(), "Normalized Open not in [0, 1]"

def test_normalize_prices_missing_columns():
    """Test normalize_prices raises ValueError for missing columns."""
    df = pd.DataFrame({'Open': [100], 'High': [102], 'Low': [98]})  # Missing Close
    try:
        DataCleaner.normalize_prices(df)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "DataFrame must contain columns" in str(e), "Unexpected error message"

def test_normalize_prices_insufficient_rows():
    """Test normalize_prices raises ValueError for insufficient rows."""
    df = create_sample_df(rows=5)  # Less than 10 rows
    try:
        DataCleaner.normalize_prices(df)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "at least 10 rows" in str(e), "Unexpected error message"

def test_normalize_prices_constant_values():
    """Test normalize_prices handles constant OHLC values."""
    df = create_sample_df()
    df['Close'] = 100  # Constant Close
    result = DataCleaner.normalize_prices(df)
    assert (result['Close_norm'] == 0).all(), "Normalized constant Close not zero"