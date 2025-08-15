import cvxpy as cp
import pandas as pd
import numpy as np
from pypfopt import EfficientFrontier
from typing import Dict

class PortfolioOptimizer:
    """Optimizes portfolio weights using mean-variance optimization."""

    def __init__(self, max_risk: float = 0.2, transaction_cost: float = 0.001, max_turnover: float = 0.1):
        """
        Initialize the PortfolioOptimizer with risk and cost parameters.

        Args:
            max_risk: Maximum portfolio volatility (standard deviation).
            transaction_cost: Cost per unit of turnover.
            max_turnover: Maximum allowable turnover per rebalance.
        """
        self.max_risk = max_risk
        self.transaction_cost = transaction_cost
        self.max_turnover = max_turnover

    def optimize(self, expected_returns: Dict, cov_matrix: pd.DataFrame, current_weights: Dict, strategy: Dict) -> Dict:
        """
        Mean-variance optimization with CVXPY, respecting hypothesis constraints.

        Args:
            expected_returns: Dictionary of asset expected returns.
            cov_matrix: Covariance matrix of asset returns.
            current_weights: Current portfolio weights.
            strategy: Hypothesis strategy with constraints (e.g., holding_period).

        Returns:
            Dict: Optimized weights for each asset.
        """
        try:
            assets = list(expected_returns.keys())
            weights = cp.Variable(len(assets))
            expected_return = cp.sum([expected_returns[asset] * weights[i] for i, asset in enumerate(assets)])
            risk = cp.quad_form(weights, cp.psd_wrap(cov_matrix.values))
            current_weights_array = np.array([current_weights.get(asset, 0.0) for asset in assets])
            turnover = cp.sum(cp.abs(weights - current_weights_array))
            
            constraints = [
                cp.sum(weights) == 1,
                weights >= 0,  # No short selling
                risk <= self.max_risk,
                turnover <= self.max_turnover
            ]
            
            # Hypothesis-specific constraints (e.g., exclude restricted assets)
            restricted_assets = strategy.get("restricted_assets", [])
            for i, asset in enumerate(assets):
                if asset in restricted_assets:
                    constraints.append(weights[i] == 0)

            objective = cp.Maximize(expected_return - self.transaction_cost * turnover)
            problem = cp.Problem(objective, constraints)
            problem.solve(solver=cp.ECOS)
            
            if problem.status != cp.OPTIMAL:
                return {}
                
            optimized_weights = dict(zip(assets, weights.value))
            return optimized_weights
        except Exception as e:
            return {}

    def optimize_pypfopt(self, mu: pd.Series, S: pd.DataFrame) -> Dict:
        """
        Alternative optimization using PyPortfolioOpt.

        Args:
            mu: Series of expected returns.
            S: Covariance matrix.

        Returns:
            Dict: Optimized weights.
        """
        try:
            ef = EfficientFrontier(mu, S, weight_bounds=(0, 1))
            ef.efficient_risk(target_volatility=self.max_risk)
            weights = ef.clean_weights()
            return weights
        except Exception as e:
            return {}