from unittest.mock import patch
import unittest
import pandas as pd
import numpy as np
from neuralwealth.data_layer.processors.feature_engineer import FeatureEngineer

def create_sample_df(rows=26):
    dates = pd.date_range(start='2025-06-30', periods=rows, freq='D')
    return pd.DataFrame({
        'time': dates,
        'open': 158.0 + np.random.normal(0, 2, rows),
        'high': 159.0 + np.random.normal(0, 2, rows),
        'low': 156.0 + np.random.normal(0, 2, rows),
        'close': 157.0 + np.random.normal(0, 2, rows),
        'volume': 200000000 + np.random.normal(0, 10000000, rows)
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
@patch("talib.WMA")
@patch("talib.CMO")
def test_add_ta_features_success(mock_cmo, mock_wma, mock_atr, mock_bbands, mock_obv, mock_stoch, mock_macd, mock_rsi, mock_adx, mock_ema, mock_sma):
    mock_sma.return_value = np.array([157.0] * 26)
    mock_ema.return_value = np.array([157.0] * 26)
    mock_adx.return_value = np.array([25.0] * 26)
    mock_rsi.return_value = np.array([60.0] * 26)
    mock_macd.return_value = (np.array([0.5] * 26), np.array([0.2] * 26), np.array([0.3] * 26))
    mock_stoch.return_value = (np.array([75.0] * 26), np.array([70.0] * 26))
    mock_obv.return_value = np.array([10000.0] * 26)
    mock_bbands.return_value = (np.array([160.0] * 26), np.array([157.0] * 26), np.array([154.0] * 26))
    mock_atr.return_value = np.array([2.0] * 26)
    mock_wma.return_value = np.array([157.0] * 26)
    mock_cmo.return_value = np.array([30.0] * 26)

    df = create_sample_df()
    result = FeatureEngineer.add_ta_features(df)
    assert isinstance(result, pd.DataFrame)
    expected_columns = [
        'open', 'high', 'low', 'close', 'volume',
        'sma_20', 'ema_20', 'hma_9', 'adx_14', 'sma_20_daily',
        'rsi_14', 'rsi_z_score', 'macd', 'stoch_k', 'stoch_d', 'cmo_14',
        'obv', 'vwap', 'bb_upper', 'bb_middle', 'bb_lower', 'atr_14',
        'kc_upper', 'kc_middle', 'kc_lower', 'pivot', 's1', 'r1'
    ]
    assert all(col in result.columns for col in expected_columns)
    assert len(result) == len(df)
    assert (result['rsi_z_score'] >= -3).all()
    assert (result['rsi_z_score'] <= 3).all()
    assert result['rsi_z_score'].notna().all()

@patch("talib.SMA")
def test_add_ta_features_missing_columns(mock_sma):
    df = create_sample_df()
    df = df.drop(columns=['volume'])
    with unittest.TestCase().assertRaises(ValueError) as context:
        FeatureEngineer.add_ta_features(df)
    assert "DataFrame must contain columns" in str(context.exception)

@patch("talib.SMA")
def test_add_ta_features_insufficient_rows(mock_sma):
    df = create_sample_df(rows=10)
    with unittest.TestCase().assertRaises(ValueError) as context:
        FeatureEngineer.add_ta_features(df)
    assert "at least 26 rows" in str(context.exception)

def test_calculate_vwap_success():
    df = create_sample_df(rows=2)
    df_shifted = df.shift(1)
    vwap = FeatureEngineer._calculate_vwap(df)
    assert isinstance(vwap, pd.Series)
    assert len(vwap) == len(df)
    expected_vwap = ((df_shifted['high'] + df_shifted['low'] + df_shifted['close']) / 3 * df_shifted['volume']).cumsum() / df_shifted['volume'].cumsum()
    pd.testing.assert_series_equal(vwap, expected_vwap, check_names=False)

def test_calculate_pivot_points_success():
    df = create_sample_df(rows=2)
    pivot, s1, r1 = FeatureEngineer._calculate_pivot_points(df)
    assert all(isinstance(x, pd.Series) for x in [pivot, s1, r1])
    assert len(pivot) == len(df)
    df_shifted = df.shift(1)
    expected_pivot = (df_shifted['high'] + df_shifted['low'] + df_shifted['close']) / 3
    expected_s1 = 2 * expected_pivot - df_shifted['high']
    expected_r1 = 2 * expected_pivot - df_shifted['low']
    pd.testing.assert_series_equal(pivot, expected_pivot, check_names=False)
    pd.testing.assert_series_equal(s1, expected_s1, check_names=False)
    pd.testing.assert_series_equal(r1, expected_r1, check_names=False)