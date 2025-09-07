import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime
from neuralwealth.portfolio.optimization.federated import get_user_model, update_user_experience, get_global_predictions

class PersonalizedRecommender:
    """Handles personalized portfolio recommendations using federated learning"""
    
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.local_model = get_user_model(user_id)
    
    def extract_market_features(self, market_data: pd.DataFrame, current_weights: Dict) -> Dict:
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
        if not market_data.empty:
            features['market_volatility'] = market_data.get('volatility', 0.015).mean()
            features['market_momentum'] = market_data.get('momentum', 0.0).mean()
        else:
            features['market_volatility'] = 0.015
            features['market_momentum'] = 0.0
        
        return features
    
    def get_personalized_recommendation(self, market_data: pd.DataFrame, current_weights: Dict) -> Dict:
        """Get personalized portfolio recommendation from federated model"""
        market_features = self.extract_market_features(market_data, current_weights)
        return self.local_model.predict_with_cvar(market_features)
    
    def get_global_recommendation(self, market_data: pd.DataFrame) -> Dict:
        """Get global portfolio recommendation"""
        market_features = self.extract_market_features(market_data, {})
        return get_global_predictions(market_features)
    
    def blend_recommendations(self, fl_weights: Dict, strategy_weights: Dict, 
                            blend_ratio: float = 0.7) -> Dict:
        """Blend federated learning weights with strategy weights"""
        blended_weights = {}
        all_assets = set(fl_weights.keys()) | set(strategy_weights.keys())
        
        for asset in all_assets:
            fl_weight = fl_weights.get(asset, 0.0)
            strategy_weight = strategy_weights.get(asset, 0.0)
            
            # Blend with preference for FL weights
            blended_weight = blend_ratio * fl_weight + (1 - blend_ratio) * strategy_weight
            blended_weights[asset] = blended_weight
        
        # Normalize
        total = sum(blended_weights.values())
        if total > 0:
            return {asset: weight / total for asset, weight in blended_weights.items()}
        return blended_weights
    
    def record_learning_experience(self, market_features: Dict, executed_weights: Dict, 
                                 execution_results: Dict) -> bool:
        """Record trading experience for federated learning"""
        try:
            # Calculate portfolio performance
            portfolio_return = self.estimate_portfolio_performance(execution_results)
            
            experience = {
                'market_features': market_features,
                'executed_weights': executed_weights,
                'portfolio_return': portfolio_return,
                'timestamp': datetime.now().isoformat(),
                'success_rate': len([r for r in execution_results if r.get('status') == 'success']) / max(1, len(execution_results))
            }
            
            # Update user's local model
            update_user_experience(self.user_id, [experience])
            return True
            
        except Exception as e:
            print(f"Failed to record learning experience: {e}")
            return False
    
    def estimate_portfolio_performance(self, execution_results: List[Dict]) -> float:
        """Estimate portfolio performance from execution results"""
        successful_trades = [r for r in execution_results if r.get('status') == 'success']
        if not successful_trades:
            return 0.0
        
        # Use slippage as proxy for performance (less slippage = better execution)
        avg_slippage = np.mean([r.get('slippage', 0.02) for r in successful_trades])
        return -avg_slippage  # Negative because less slippage is better
    
    def get_model_info(self) -> Dict:
        """Get information about the user's local model"""
        return {
            'user_id': self.user_id,
            'model_type': type(self.local_model).__name__,
            'last_update': getattr(self.local_model, 'last_update', 'unknown'),
            'experience_count': len(getattr(self.local_model, 'experience_buffer', []))
        }