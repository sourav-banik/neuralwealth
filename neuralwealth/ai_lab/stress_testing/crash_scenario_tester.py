import pandas as pd
from typing import Dict, Any, Type
from neuralwealth.ai_lab.backtesting.backtesting import BacktestEngine
from neuralwealth.ai_lab.utils.synthetic_data_client import SyntheticDataClient

class CrashScenarioTester:
    def __init__(
        self, 
        backtest_engine: Type[BacktestEngine],
        synthetic_client: Type[SyntheticDataClient]
    ):
        """
        Initialize the crash scenario tester with a BacktestEngine and synthetic data client.

        Args:
            backtest_engine: Instance of BacktestEngine to run backtests.
            ynthetic_client: Instance of SyntheticDataClient to retrieve crash data.
        """
        self.backtest_engine = backtest_engine
        self.synthetic_client = synthetic_client

    def test_strategy(
        self, 
        strategy: Dict, 
        data: Dict[str, pd.DataFrame]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Test strategy during historical crash periods.

        Args:
            strategy: Strategy hypothesis containing assets and parameters.
            data: Dictionary of {ticker: DataFrame} with market data.

        Returns:
            Dictionary of {crash_name: results_dict} for each crash period.
        """
        crash_results = {}
        synthetic_scenarios = self.synthetic_client.get_synthetic_scenarios(strategy['assets'])
        for scenario_type, synthetic_data in synthetic_scenarios.items():
            try:
                # Create crash test data by replacing OHLCV with synthetic values
                crash_data = self._create_crash_test_data(data, synthetic_data)
                # Run backtest on crash scenario
                scenario_result = self.backtest_engine.run(strategy, crash_data)
                crash_results[scenario_type] = scenario_result
                
            except Exception as e:
                print(f"Failed to test {scenario_type}: {e}")
                continue
        return crash_results
    
    def _create_crash_test_data(self, historical_data: Dict[str, pd.DataFrame], synthetic_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Merge synthetic crash data with historical data by matching exact datetime index for each key in the input dictionaries,
        updating only open, high, low, close, and volume columns without altering other columns or historical data points
        outside the synthetic period.

        Args:
            historical_data: Dictionary of {key: DataFrame} with historical market data, each DataFrame having a datetime index
                            and columns including open, high, low, close, volume, etc.
            synthetic_data: Dictionary of {key: DataFrame} with synthetic crash scenario data, each DataFrame having a datetime
                            index and columns open, high, low, close, volume.

        Returns:
            Dictionary of {key: DataFrame} with synthetic data merged into historical data for specified columns,
            retaining historical datetime index for each key.
        """
        # Define columns to merge from synthetic data
        merge_columns = ['open', 'high', 'low', 'close', 'volume']
        
        # Initialize output dictionary
        crash_test_data = {}
        
        # Iterate over keys in synthetic_data
        for key in synthetic_data:
            if key not in historical_data:
                # If key is not in historical_data, skip or use synthetic data as is
                crash_test_data[key] = synthetic_data[key][merge_columns].copy()
                continue
            
            # Get the DataFrames for this key
            hist_df = historical_data[key].copy()
            synth_df = synthetic_data[key][merge_columns].copy()
            
            # Find overlapping indices (exact datetime match)
            overlapping_indices = synth_df.index.intersection(hist_df.index)
            
            # Update overlapping indices with synthetic data for specified columns
            for col in merge_columns:
                if col in hist_df.columns:
                    hist_df.loc[overlapping_indices, col] = synth_df.loc[overlapping_indices, col]
            
            # Handle non-overlapping synthetic data (future indices)
            future_indices = synth_df.index.difference(hist_df.index)
            if not future_indices.empty:
                future_data = synth_df.loc[future_indices].copy()
                # Add missing columns from historical data, filling with pd.NA
                for col in hist_df.columns:
                    if col not in future_data.columns:
                        future_data[col] = pd.NA
                # Concatenate and sort by index to maintain chronological order
                hist_df = pd.concat([hist_df, future_data[hist_df.columns]], axis=0).sort_index()
            
            # Store the merged DataFrame
            crash_test_data[key] = hist_df
        
        return crash_test_data