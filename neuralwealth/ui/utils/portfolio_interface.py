from abc import ABC, abstractmethod
from typing import Dict, List

class PortfolioInterface(ABC):
    """Abstract interface for portfolio management interactions."""

    @abstractmethod
    def rebalance(self, market_state: Dict, hypotheses: List[Dict]) -> Dict:
        """Rebalance portfolio and return result."""
        pass

    @abstractmethod
    def get_portfolio(self) -> Dict:
        """Return current portfolio holdings."""
        pass

    @abstractmethod
    def train_rl_agent(self) -> Dict:
        """Train or retrain RL agent."""
        pass

    @abstractmethod
    def get_asset_data(self, assets: List[str], fields: List[str], start_date: str, end_date: str) -> Dict:
        """Fetch asset data (e.g., sector, ESG score)."""
        pass