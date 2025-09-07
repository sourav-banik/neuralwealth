import pandas as pd
import numpy as np
from typing import Dict, List, Any
from neuralwealth.portfolio.optimization.cvar_optimizer import CVaROptimizer
from neuralwealth.portfolio.execution.broker_connector import BrokerConnector
from neuralwealth.portfolio.execution.risk_manager import RiskManager
from neuralwealth.portfolio.execution.audit_logger import AuditLogger
from neuralwealth.portfolio.optimization.personalized_recommender import PersonalizedRecommender 

class PortfolioManager:
    """Main controller for portfolio management and rebalancing"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize portfolio manager
        
        Args:
            config: Dictionary of portfolio management configuration
        """
        self.user_constraints = config["constraints"]
        self.optimizer = CVaROptimizer()
        self.broker = BrokerConnector(broker_type=config["broker_type"])
        self.risk_manager = RiskManager(self.user_constraints)
        self.audit_logger = AuditLogger()
        self.recommender = PersonalizedRecommender(config["user_id"])
        
    def rebalance_portfolio(self, strategies: List[Dict], market_data: pd.DataFrame, 
                          historical_returns: pd.DataFrame = None) -> Dict:
        """
        Main rebalancing method using personalized recommendations
        """
        # 1. Get current portfolio state
        current_portfolio = self.broker.get_portfolio()
        current_weights = current_portfolio.get('weights', {})
        
        # 2. Get personalized recommendations
        fl_weights = self.recommender.get_personalized_recommendation(market_data, current_weights)
        
        # 3. Generate target weights from strategies
        strategy_weights = self._aggregate_strategies(strategies, market_data)
        
        # 4. Blend recommendations
        target_weights = self.recommender.blend_recommendations(fl_weights, strategy_weights)
        
        # 5. CVaR Optimization
        expected_returns = self._estimate_expected_returns(market_data)
        cov_matrix = self._estimate_covariance_matrix(market_data, historical_returns)
        
        optimized_weights = self.optimizer.optimize(
            expected_returns, cov_matrix, target_weights, historical_returns
        )
        
        # 6. Validate against constraints
        if not self.risk_manager.validate_weights(optimized_weights, market_data):
            print("Optimized weights violate constraints - using current weights")
            optimized_weights = current_weights
        
        # 7. Generate orders
        orders = self._generate_orders(current_portfolio, optimized_weights, market_data)
        
        # 8. Pre-trade checks
        validated_orders = self.risk_manager.pre_trade_check(orders, market_data)
        
        # 9. Execute approved orders
        approved_orders = [order for order in validated_orders if order.get('status') == 'APPROVED']
        execution_results = self.broker.execute_orders(approved_orders)
        
        # 10. Record learning experience
        market_features = self.recommender.extract_market_features(market_data, current_weights)
        self.recommender.record_learning_experience(market_features, optimized_weights, execution_results)
        
        # 11. Log audit trail
        audit_id = self.audit_logger.log_rebalance(
            current_weights, optimized_weights, execution_results, self.user_constraints
        )
        
        return {
            'success': True,
            'orders_approved': len(approved_orders),
            'orders_executed': len(execution_results),
            'new_weights': optimized_weights,
            'fl_weights': fl_weights,
            'strategy_weights': strategy_weights,
            'audit_id': audit_id
        }
    
    def _extract_market_features(self, market_data: pd.DataFrame, current_weights: Dict) -> Dict:
        """Extract features for federated learning model"""
        features = {}
        
        if not market_data.empty:
            for asset, data in market_data.iterrows():
                # Basic market features
                features[f"{asset}_price"] = data.get('price', 100)
                features[f"{asset}_volume"] = data.get('volume', 1000000)
                features[f"{asset}_volatility"] = data.get('volatility', 0.02)
                features[f"{asset}_momentum"] = data.get('momentum', 0.0)
                
                # Portfolio context features
                features[f"{asset}_current_weight"] = current_weights.get(asset, 0.0)
        
        # Market-wide features
        features['market_volatility'] = market_data.get('volatility', 0.015).mean() if not market_data.empty else 0.015
        features['market_momentum'] = market_data.get('momentum', 0.0).mean() if not market_data.empty else 0.0

        
        return features
    
    def _estimate_expected_returns(self, market_data: pd.DataFrame) -> Dict[str, float]:
        """Simple expected returns estimation"""
        expected_returns = {}
        if not market_data.empty:
            for asset in market_data.index.tolist():
                if 'expected_return' in market_data.index:
                    expected_returns[asset] = market_data.loc['expected_return', asset]
                else:
                    expected_returns[asset] = 0.0005
        return expected_returns
    
    def _estimate_covariance_matrix(self, market_data: pd.DataFrame, 
                                  historical_returns: pd.DataFrame = None) -> pd.DataFrame:
        """Estimate covariance matrix"""
        if historical_returns is not None and not historical_returns.empty:
            # Use historical returns if available
            return historical_returns.cov()
        else:
            # Simple diagonal covariance matrix
            assets = market_data.index.tolist() if not market_data.empty else []
            n_assets = len(assets)
            cov_matrix = pd.DataFrame(
                np.eye(n_assets) * 0.0004,  # 20% annual vol default
                index=assets,
                columns=assets
            )
            return cov_matrix
    
    def _generate_orders(self, current_portfolio: Dict, target_weights: Dict, market_data: pd.DataFrame) -> List[Dict]:
        """Generate buy/sell orders to reach target weights"""
        orders = []
        current_weights = current_portfolio.get('weights', {})
        total_value = current_portfolio.get('total_value', 100000)
        
        for asset, target_weight in target_weights.items():
            current_weight = current_weights.get(asset, 0)
            
            if abs(target_weight - current_weight) > 0.001:
                # Get current price from market data
                current_price = 100  # Default
                if not market_data.empty and asset in market_data.index:
                    current_price = market_data.loc[asset].get('price', 100)
                
                order_value = abs(target_weight - current_weight) * total_value
                quantity = order_value / current_price
                
                order = {
                    'asset': asset,
                    'action': 'BUY' if target_weight > current_weight else 'SELL',
                    'quantity': quantity,
                    'current_price': current_price
                }
                orders.append(order)
        
        return orders
    
    def _aggregate_strategies(self, strategies: List[Dict], market_data: pd.DataFrame) -> Dict[str, float]:
        """Simple strategy aggregation - average of all strategy weights"""
        if not strategies:
            return {}
            
        # Simple average of strategy weights
        all_weights = {}
        weight_count = {}
        
        for strategy in strategies:
            # Extract weights from strategy (simplified)
            strategy_weights = self._extract_weights_from_strategy(strategy, market_data)
            
            for asset, weight in strategy_weights.items():
                if asset in all_weights:
                    all_weights[asset] += weight
                    weight_count[asset] += 1
                else:
                    all_weights[asset] = weight
                    weight_count[asset] = 1
        
        # Average the weights
        avg_weights = {asset: weight / weight_count[asset] for asset, weight in all_weights.items()}
        
        # Normalize to sum to 1
        total = sum(avg_weights.values())
        if total > 0:
            return {asset: weight / total for asset, weight in avg_weights.items()}
        
        return avg_weights
    
    def _extract_weights_from_strategy(self, strategy: Dict, market_data: pd.DataFrame) -> Dict[str, float]:
        """Extract portfolio weights from strategy object"""
        # Simplified extraction - in real implementation, this would use strategy logic
        assets = strategy.get('assets', [])
        if isinstance(assets, list) and len(assets) > 0:
            # Equal weight for all assets in strategy
            weight = 1.0 / len(assets)
            return {asset: weight for asset in assets}
        return {}
    
    def get_portfolio_status(self) -> Dict:
        """Get current portfolio status"""
        return self.broker.get_portfolio()
    
    def get_audit_logs(self, log_type: str = None, limit: int = 100) -> List[Dict]:
        """Get audit log entries"""
        return self.audit_logger.get_logs(log_type, limit)
    
    def get_recommendation_info(self) -> Dict:
        """Get information about personalized recommendations"""
        return self.recommender.get_model_info()
    
    def cleanup(self):
        """Clean up resources"""
        if hasattr(self.broker, 'disconnect'):
            self.broker.disconnect()