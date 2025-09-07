import numpy as np
from typing import List, Dict
from neuralwealth.portfolio.optimization.federated.base_model import BasePortfolioModel

class LocalClientModel:
    """User-specific model with CVaR constraints and local training"""
    
    def __init__(self, user_id: str, input_dim: int = 10):
        self.user_id = user_id
        self.model = BasePortfolioModel(input_dim)
        self.experience_buffer = []
        self.cvar_alpha = 0.95  # CVaR confidence level
        
    def update(self, experiences: List[Dict], learning_rate: float = 0.01):
        """
        Update local model with user's trading experiences
        Simplified PPO-style update with CVaR constraint
        """
        if not experiences:
            return
            
        # Store experiences
        self.experience_buffer.extend(experiences)
        
        # Simple training: adjust weights based on portfolio performance
        for experience in experiences[-100:]:  # Use recent 100 experiences
            returns = experience.get('returns', 0)
            cvar = experience.get('cvar', 0)
            
            # CVaR-constrained update
            if returns < cvar:  # Bad outcome, penalize
                self._penalize_weights(experience, learning_rate * 0.5)
            else:  # Good outcome, reinforce
                self._reinforce_weights(experience, learning_rate)
    
    def _reinforce_weights(self, experience: Dict, learning_rate: float):
        """Reinforce weights that led to good outcomes"""
        market_features = experience.get('market_features', {})
        if not market_features:
            return
            
        # Simple heuristic: increase weights for assets that performed well
        feature_vector = np.array([market_features.get(f, 0) for f in sorted(market_features.keys())])
        if len(feature_vector) == self.model.input_dim:
            gradient = feature_vector * learning_rate
            self.model.weights['w2'] += gradient.reshape(-1, 1) * 0.1
    
    def _penalize_weights(self, experience: Dict, learning_rate: float):
        """Penalize weights that led to bad outcomes"""
        market_features = experience.get('market_features', {})
        if not market_features:
            return
            
        # Simple heuristic: decrease weights for assets that performed poorly
        feature_vector = np.array([market_features.get(f, 0) for f in sorted(market_features.keys())])
        if len(feature_vector) == self.model.input_dim:
            gradient = feature_vector * learning_rate
            self.model.weights['w2'] -= gradient.reshape(-1, 1) * 0.1
    
    def predict_with_cvar(self, market_features: Dict) -> Dict[str, float]:
        """Predict weights with CVaR constraint validation"""
        weights = self.model.predict(market_features)
        
        # Simple CVaR constraint: ensure no single asset > 20%
        constrained_weights = {}
        total = 0
        for asset, weight in weights.items():
            constrained_weights[asset] = min(weight, 0.2)
            total += constrained_weights[asset]
        
        # Renormalize
        if total > 0:
            constrained_weights = {k: v/total for k, v in constrained_weights.items()}
        
        return constrained_weights
    
    def get_model_data(self) -> Dict:
        """Get model data for aggregation"""
        return {
            'user_id': self.user_id,
            'weights': self.model.get_weights(),
            'experience_count': len(self.experience_buffer)
        }
    
    def load_model_data(self, model_data: Dict):
        """Load model data from aggregation"""
        if 'weights' in model_data:
            self.model.set_weights(model_data['weights'])