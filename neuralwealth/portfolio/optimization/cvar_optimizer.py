import numpy as np
import pandas as pd
from typing import Dict, List
from scipy.stats import norm

class CVaROptimizer:
    """Simplified CVaR-based portfolio optimizer with basic risk constraints"""
    
    def __init__(self, cvar_alpha: float = 0.95, risk_aversion: float = 0.5):
        self.cvar_alpha = cvar_alpha
        self.risk_aversion = risk_aversion
    
    def optimize(self, 
                expected_returns: Dict[str, float], 
                cov_matrix: pd.DataFrame,
                current_weights: Dict[str, float],
                historical_returns: pd.DataFrame = None) -> Dict[str, float]:
        """
        Simplified CVaR optimization
        
        Args:
            expected_returns: Dictionary of expected returns by asset
            cov_matrix: Covariance matrix of asset returns
            current_weights: Current portfolio weights
            historical_returns: DataFrame of historical returns (optional)
            
        Returns:
            Optimized portfolio weights
        """
        try:
            assets = list(expected_returns.keys())
            
            if not assets:
                return current_weights
            
            # Convert to arrays
            mu = np.array([expected_returns[asset] for asset in assets])
            
            # Simple CVaR approximation using normal distribution
            if historical_returns is not None and not historical_returns.empty:
                # Use historical returns for CVaR calculation
                portfolio_returns = self._calculate_portfolio_returns(historical_returns, current_weights)
                cvar = self._calculate_historical_cvar(portfolio_returns)
            else:
                # Use parametric approach (simplified)
                portfolio_variance = self._calculate_portfolio_variance(current_weights, cov_matrix)
                cvar = self._calculate_parametric_cvar(mu, portfolio_variance)

            # Adjust weights based on CVaR
            optimized_weights = self._adjust_weights_for_cvar(
                current_weights, mu, cvar, assets
            )
            
            return optimized_weights   
        except Exception as e:
            print(f"CVaR optimization failed: {e}")
            return current_weights
    
    def _calculate_portfolio_variance(self, weights: Dict, cov_matrix: pd.DataFrame) -> float:
        """Calculate portfolio variance"""
        try:
            assets = list(weights.keys())
            w = np.array([weights[asset] for asset in assets])
            cov = cov_matrix.loc[assets, assets].values
            return w.T @ cov @ w
        except:
            return 0.01  # Default variance
    
    def _calculate_parametric_cvar(self, expected_returns: np.ndarray, variance: float) -> float:
        """Calculate CVaR using parametric (normal) approach"""
        z_alpha = norm.ppf(self.cvar_alpha)
        cvar = -expected_returns.mean() + (norm.pdf(z_alpha) / (1 - self.cvar_alpha)) * np.sqrt(variance)
        return max(cvar, 0.01)  # Minimum CVaR
    
    def _calculate_historical_cvar(self, returns: np.ndarray) -> float:
        """Calculate CVaR from historical returns"""
        if len(returns) == 0:
            return 0.05  # Default CVaR
        
        sorted_returns = np.sort(returns)
        var_index = int((1 - self.cvar_alpha) * len(sorted_returns))
        cvar = -np.mean(sorted_returns[:var_index])
        return max(cvar, 0.01)
    
    def _calculate_portfolio_returns(self, historical_returns: pd.DataFrame, weights: Dict) -> np.ndarray:
        """Calculate historical portfolio returns"""
        try:
            assets = [asset for asset in weights.keys() if asset in historical_returns.columns]
            if not assets:
                return np.array([])
                
            returns = historical_returns[assets].values
            w = np.array([weights[asset] for asset in assets])
            portfolio_returns = returns @ w
            return portfolio_returns
        except:
            return np.array([])
    
    def _adjust_weights_for_cvar(self, 
                               current_weights: Dict, 
                               expected_returns: np.ndarray,
                               cvar: float,
                               assets: List[str]) -> Dict[str, float]:
        """Adjust weights based on CVaR constraint"""
        # Simple heuristic: reduce weights of high-risk assets
        risk_adjusted_weights = current_weights.copy()
        
        # Calculate risk contribution (simplified)
        total_risk = sum(expected_returns)  # Using expected returns as risk proxy
        
        if total_risk > 0:
            for i, asset in enumerate(assets):
                risk_contribution = expected_returns[i] / total_risk
                # Reduce weight if risk contribution is high relative to CVaR
                if risk_contribution > cvar:
                    reduction = self.risk_aversion * (risk_contribution - cvar)
                    risk_adjusted_weights[asset] = max(current_weights[asset] * (1 - reduction), 0)
        
        # Renormalize weights
        total = sum(risk_adjusted_weights.values())
        if total > 0:
            return {asset: weight / total for asset, weight in risk_adjusted_weights.items()}
        
        return current_weights