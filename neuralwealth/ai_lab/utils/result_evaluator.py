from typing import Dict, Any

class ResultEvaluator:
    """Evaluates backtest and crash scenario results against configurable criteria."""

    def __init__(self, criteria: Dict[str, float] = None):
        """
        Initialize the ResultEvaluator with performance criteria.

        Args:
            criteria: Dictionary of evaluation thresholds, e.g.:
                {
                    "min_sharpe": 1.0,  # Minimum Sharpe ratio
                    "max_drawdown": 10.0,  # Maximum drawdown percentage
                    "min_total_return": 0.0,  # Minimum total return
                    "max_crash_drawdown": 15.0,  # Maximum drawdown in crash periods
                    "min_crash_return": -0.05  # Minimum return in crash periods
                }
                Defaults to predefined values if None.
        """
        self.criteria = criteria or {
            "min_sharpe": -float('inf'),
            "max_drawdown": float('inf'),
            "min_total_return": -float('inf'),
            "max_crash_drawdown": float('inf'),
            "min_crash_return": -float('inf')
        }

    def evaluate_results(
        self, 
        backtest_results: Dict[str, Dict[str, Any]], 
        crash_results: Dict[str, Dict[str, Any]]
    ) -> bool:
        """
        Evaluate if backtest and crash scenario results meet performance criteria.

        Args:
            backtest_results: Dictionary of {ticker: results_dict} from backtesting.
            crash_results: Dictionary of {crash_name: results_dict} from crash testing.

        Returns:
            bool: True if results meet all criteria, False otherwise.
        """
        # Check backtest results for each ticker
        for ticker, results in backtest_results.items():
            if "error" in results:
                return False
            sharpe = results.get("sharpe", {}).get("sharperatio", 0.0) or 0.0
            max_drawdown = results.get("drawdown", {}).get("maxdrawdown", 0.0) or 0.0
            total_return = results.get("returns", {}).get("rtot", 0.0) or 0.0
            if (
                sharpe < self.criteria["min_sharpe"] or
                max_drawdown > self.criteria["max_drawdown"] or
                total_return < self.criteria["min_total_return"]
            ):
                return False

        # Check crash scenario results
        for crash_name, results in crash_results.items():
            if "error" in results:
                return False
            for ticker, ticker_results in results.items():
                if "error" in ticker_results:
                    return False
                crash_drawdown = ticker_results.get("drawdown", {}).get("maxdrawdown", 0.0) or 0.0
                crash_return = ticker_results.get("returns", {}).get("rtot", 0.0) or 0.0
                if (
                    crash_drawdown > self.criteria["max_crash_drawdown"] or
                    crash_return < self.criteria["min_crash_return"]
                ):
                    return False

        return True