import unittest
import pandas as pd
import numpy as np
from neuralwealth.data_layer.processors.cleaner import MarketDataCleaner

def setUp():
    dates = pd.date_range(start='2025-06-30', periods=26, freq='D')
    df = pd.DataFrame({
        'time': dates,
        'open': 158.0 + np.random.normal(0, 2, 26),
        'high': 159.0 + np.random.normal(0, 2, 26),
        'low': 156.0 + np.random.normal(0, 2, 26),
        'close': 157.0 + np.random.normal(0, 2, 26),
        'volume': 200000000 + np.random.normal(0, 10000000, 26),
        'dividends': [0.0] * 26,
        'stock_splits': [0.0] * 26
    })
    df.loc[5, 'open'] = np.nan
    df.loc[6, 'volume'] = -1000
    df.loc[7, 'high'] = 200.0
    return df

def test_missing_columns():
    df = setUp()
    df_invalid = df.drop(columns=['open'])
    with unittest.TestCase().assertRaises(ValueError):
        MarketDataCleaner.clean_data(df_invalid)

def test_insufficient_rows():
    df = setUp()
    df_short = df.iloc[:5]
    with unittest.TestCase().assertRaises(ValueError):
        MarketDataCleaner.clean_data(df_short)

def test_missing_time_column():
    df = setUp()
    df_no_time = df.drop(columns=['time'])
    df_no_time.index = range(len(df_no_time))
    with unittest.TestCase().assertRaises(ValueError):
        MarketDataCleaner.clean_data(df_no_time)

def test_price_consistency():
    df = setUp()
    cleaned_df = MarketDataCleaner.clean_data(df)
    assert (cleaned_df['high'] >= cleaned_df['close']).all()
    assert (cleaned_df['close'] >= cleaned_df['low']).all()
    assert (cleaned_df['high'] >= cleaned_df['open']).all()
    assert (cleaned_df['open'] >= cleaned_df['low']).all()

def test_missing_values_handled():
    df = setUp()
    cleaned_df = MarketDataCleaner.clean_data(df)
    assert not cleaned_df[['open', 'high', 'low', 'close', 'volume']].isna().any().any()

def test_outlier_handling():
    df = setUp()
    cleaned_df = MarketDataCleaner.clean_data(df)
    upper_bound = df['high'].quantile(0.99)
    assert (cleaned_df['high'] <= upper_bound).all()

def test_volume_cleaning():
    df = setUp()
    cleaned_df = MarketDataCleaner.clean_data(df)
    assert (cleaned_df['volume'] >= 0).all()
    volume_upper = df['volume'][df['volume'] >= 0].quantile(0.99)
    assert (cleaned_df['volume'] <= volume_upper).all()

def test_normalization():
    df = setUp()
    cleaned_df = MarketDataCleaner.clean_data(df, normalize=True, z_score=True)
    for col in ['open', 'high', 'low', 'close']:
        assert (cleaned_df[f'{col}_norm'] >= 0).all()
        assert (cleaned_df[f'{col}_norm'] <= 1).all()
        assert cleaned_df[f'{col}_z_score'].notna().all()
