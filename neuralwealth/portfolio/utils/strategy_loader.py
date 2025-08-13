from typing import Dict, List

class StrategyLoader:
    """Loads and ranks hypotheses for portfolio management."""

    def select_strategies(self, hypotheses: List[Dict], top_n: int = 3) -> List[Dict]:
        """
        Select top hypotheses based on Sharpe ratio and confidence.

        Args:
            hypotheses: List of hypothesis dictionaries.
            top_n: Number of hypotheses to select.

        Returns:
            List[Dict]: Selected hypotheses.
        """
        try:
            sorted_hypotheses = sorted(
                hypotheses,
                key=lambda x: (
                    x["backtest_results"].get(list(x["backtest_results"].keys())[0], {})
                    .get("sharpe", {}).get("sharperatio", 0.0) * x["hypothesis"].get("confidence", 0.0)
                ),
                reverse=True
            )
            selected = sorted_hypotheses[:top_n]
            return selected
        except Exception as e:
            return []