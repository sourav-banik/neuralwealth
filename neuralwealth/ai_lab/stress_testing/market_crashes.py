import pandas as pd
from typing import Dict, Any, Type
from neuralwealth.ai_lab.backtesting.backtesting import BacktestEngine

class CrashScenarioTester:
    def __init__(
        self, 
        backtest_engine: Type[BacktestEngine], 
        historical_crashes: Dict = None
    ):
        """
        Initialize the crash scenario tester with a BacktestEngine and optional historical crashes.

        Args:
            backtest_engine: Instance of BacktestEngine to run backtests.
            historical_crashes: Dictionary of crash periods, e.g., {"name": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}}.
                               Defaults to a predefined set of historical crashes if None.
        """
        self.backtest_engine = backtest_engine
        self.crashes = historical_crashes or {
            "Dot-Com Bubble": {"start": "2000-03-01", "end": "2002-10-01"},
            "2008 Financial Crisis": {"start": "2008-09-01", "end": "2009-03-01"},
            "COVID-19 Crash": {"start": "2020-02-15", "end": "2020-03-31"}
        }

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
        results = {}
        for name, period in self.crashes.items():
            crash_data = {
                ticker: df.loc[period["start"]:period["end"]]
                for ticker, df in data.items()
                if period["start"] in df.index and period["end"] in df.index
            }
            if not crash_data:
                results[name] = {"error": f"No data available for {name} period"}
                continue
            try:
                results[name] = self.backtest_engine.run(strategy, crash_data)
            except Exception as e:
                results[name] = {"error": f"Backtest failed for {name}: {str(e)}"}
        return results