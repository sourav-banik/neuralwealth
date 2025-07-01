from unittest.mock import patch
import pandas as pd
import numpy as np
from neuralwealth.data_layer.processors.feature_engineer import FeatureEngineer

# Sample DataFrame for testing
def create_sample_df(rows=20):
    return pd.DataFrame({
        'Open': [100 + i for i in range(rows)],
        'High': [102 + i for i in range(rows)],
        'Low': [98 + i for i in range(rows)],
        'Close': [101 + i for i in range(rows)],
        'Volume': [1000 + i * 100 for i in range(rows)]
    })

@patch("talib.SMA")
@patch("talib.EMA")
@patch("talib.ADX")
@patch("talib.RSI")
@patch("talib.MACD")
@patch("talib.STOCH")
@patch("talib.OBV")
@patch("talib.BBANDS")
@patch("talib.ATR")
def test_add_ta_features_success(mock_atr, mock_bbands, mock_obv, mock_stoch, mock_macd, mock_rsi, mock_adx, mock_ema, mock_sma):
    """Test add_ta_features adds technical indicators to DataFrame."""
    # Mock TA-Lib functions to return simple arrays
    mock_sma.return_value = np.array([101.0] * 20)
    mock_ema.return_value = np.array([101.0] * 20)
    mock_adx.return_value = np.array([25.0] * 20)
    mock_rsi.return_value = np.array([60.0] * 20)
    mock_macd.return_value = (np.array([0.5] * 20), np.array([0.2] * 20), np.array([0.3] * 20))
    mock_stoch.return_value = (np.array([75.0] * 20), np.array([70.0] * 20))
    mock_obv.return_value = np.array([10000.0] * 20)
    mock_bbands.return_value = (np.array([105.0] * 20), np.array([100.0] * 20), np.array([95.0] * 20))
    mock_atr.return_value = np.array([2.0] * 20)

    df = create_sample_df()
    result = FeatureEngineer.add_ta_features(df)
    assert isinstance(result, pd.DataFrame), "Output is not a DataFrame"
    expected_columns = [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'SMA_20', 'EMA_20', 'ADX', 'RSI', 'MACD',
        'STOCH_K', 'STOCH_D', 'OBV', 'VWAP',
        'BB_UPPER', 'BB_MIDDLE', 'BB_LOWER', 'ATR',
        'PIVOT', 'S1', 'R1'
    ]
    assert all(col in result.columns for col in expected_columns), "Missing expected columns"
    assert len(result) == len(df), "Output row count does not match input"

@patch("talib.SMA")
def test_add_ta_features_missing_columns(mock_sma):
    """Test add_ta_features raises ValueError for missing columns."""
    df = pd.DataFrame({'Open': [100], 'High': [102], 'Low': [98], 'Close': [101]})  # Missing Volume
    try:
        FeatureEngineer.add_ta_features(df)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "DataFrame must contain columns" in str(e), "Unexpected error message"

@patch("talib.SMA")
def test_add_ta_features_insufficient_rows(mock_sma):
    """Test add_ta_features raises ValueError for insufficient rows."""
    df = create_sample_df(rows=10)  # Less than 14 rows
    try:
        FeatureEngineer.add_ta_features(df)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "at least 14 rows" in str(e), "Unexpected error message"

def test_calculate_vwap_success():
    """Test _calculate_vwap returns correct VWAP values."""
    df = create_sample_df(rows=2)
    vwap = FeatureEngineer._calculate_vwap(df)
    assert isinstance(vwap, pd.Series), "Output is not a Series"
    assert len(vwap) == len(df), "VWAP length does not match input"
    # Verify VWAP calculation: ((H + L + C) / 3) * Volume / cumulative Volume
    expected_vwap = ((df['High'] + df['Low'] + df['Close']) / 3 * df['Volume']).cumsum() / df['Volume'].cumsum()
    pd.testing.assert_series_equal(vwap, expected_vwap, check_names=False)

def test_calculate_pivot_points_success():
    """Test _calculate_pivot_points returns correct pivot, S1, and R1."""
    df = create_sample_df(rows=2)
    pivot, s1, r1 = FeatureEngineer._calculate_pivot_points(df)
    assert all(isinstance(x, pd.Series) for x in [pivot, s1, r1]), "Outputs are not Series"
    assert len(pivot) == len(df), "Pivot length does not match input"
    # Verify calculations: Pivot = (H + L + C) / 3, S1 = 2*Pivot - H, R1 = 2*Pivot - L
    expected_pivot = (df['High'] + df['Low'] + df['Close']) / 3
    expected_s1 = 2 * expected_pivot - df['High']
    expected_r1 = 2 * expected_pivot - df['Low']
    pd.testing.assert_series_equal(pivot, expected_pivot, check_names=False)
    pd.testing.assert_series_equal(s1, expected_s1, check_names=False)
    pd.testing.assert_series_equal(r1, expected_r1, check_names=False)