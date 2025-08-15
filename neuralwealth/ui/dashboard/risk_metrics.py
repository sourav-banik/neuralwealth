from typing import Dict
import numpy as np
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface

class RiskMetrics:
    """Calculates portfolio risk metrics."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize risk metrics.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.portfolio_interface = portfolio_interface

    def calculate_var(self, portfolio: Dict, confidence: float = 0.95) -> float:
        """Compute Value-at-Risk using historical simulation."""
        try:
            # Mock returns; replace with InfluxDBClient
            returns = np.random.normal(0, 0.01, 1000)
            var = np.percentile(returns, (1 - confidence) * 100)
            return float(abs(var))
        except Exception:
            return 0.0

    def calculate_sector_exposure(self) -> Dict:
        """Calculate sector exposure from portfolio weights."""
        try:
            portfolio = self.portfolio_interface.get_portfolio()
            assets = self.portfolio_interface.get_asset_data(
                list(portfolio.keys()), ["sector"], "2025-08-01", "2025-08-01"
            )
            exposure = {}
            for asset in portfolio:
                sector = assets.get(asset, {}).get("sector", "Unknown")
                exposure[sector] = exposure.get(sector, 0.0) + portfolio[asset]
            return exposure
        except Exception:
            return {"Unknown": 1.0}

    def calculate_esg_score(self) -> float:
        """Calculate average ESG score of portfolio."""
        try:
            portfolio = self.portfolio_interface.get_portfolio()
            assets = self.portfolio_interface.get_asset_data(
                list(portfolio.keys()), ["esg_score"], "2025-08-01", "2025-08-01"
            )
            total_weight = sum(portfolio.values())
            if total_weight == 0:
                return 0.0
            esg_score = sum(
                portfolio[asset] * assets.get(asset, {}).get("esg_score", 0.0)
                for asset in portfolio
            ) / total_weight
            return float(esg_score)
        except Exception:
            return 0.0