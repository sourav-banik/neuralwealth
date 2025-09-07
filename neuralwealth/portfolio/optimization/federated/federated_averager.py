import numpy as np
from typing import Dict, List
import time, json
from neuralwealth.portfolio.optimization.federated.base_model import BasePortfolioModel
from neuralwealth.portfolio.optimization.federated.local_client import LocalClientModel
from neuralwealth.portfolio.optimization.federated.differential_privacy import LaplaceMechanism

class FederatedAverager:
    """Enhanced federated averaging server with privacy protections"""
    
    def __init__(self, input_dim: int = 10, privacy_epsilon: float = 1.0):
        self.input_dim = input_dim
        self.global_model = BasePortfolioModel(input_dim)
        self.local_models: Dict[str, LocalClientModel] = {}
        self.aggregation_interval = 86400  # 24 hours in seconds
        self.last_aggregation = time.time()
        self.privacy_epsilon = privacy_epsilon  # Privacy budget

    def register_user(self, user_id: str) -> LocalClientModel:
        """Register a new user with local model"""
        if user_id not in self.local_models:
            self.local_models[user_id] = LocalClientModel(user_id, self.input_dim)
            # Initialize with global model weights
            self.local_models[user_id].model.set_weights(self.global_model.get_weights())
        return self.local_models[user_id]
        
    def update_user_model(self, user_id: str, experiences: List[Dict]):
        """Update user model with privacy-preserving gradient updates"""
        if user_id not in self.local_models:
            self.register_user(user_id)
        
        # Apply differential privacy to experiences
        private_experiences = self._apply_differential_privacy(experiences)
        
        # Update local model
        self.local_models[user_id].update(private_experiences)
        
        # Check if it's time to aggregate
        if time.time() - self.last_aggregation >= self.aggregation_interval:
            self.aggregate_models()
    
    def _apply_differential_privacy(self, experiences: List[Dict]) -> List[Dict]:
        """Apply differential privacy to experiences"""
        private_experiences = []
        
        for experience in experiences:
            private_experience = experience.copy()
            
            # Add noise to sensitive numerical features
            if 'portfolio_return' in private_experience:
                private_experience['portfolio_return'] = LaplaceMechanism.add_noise(
                    private_experience['portfolio_return'], 
                    sensitivity=0.1, 
                    epsilon=self.privacy_epsilon
                )
            
            # Anonymize market features (simplified)
            if 'market_features' in private_experience:
                noisy_features = {}
                for key, value in private_experience['market_features'].items():
                    if isinstance(value, (int, float)):
                        noisy_features[key] = LaplaceMechanism.add_noise(
                            value, 
                            sensitivity=abs(value) * 0.1, 
                            epsilon=self.privacy_epsilon
                        )
                    else:
                        noisy_features[key] = value
                private_experience['market_features'] = noisy_features
            
            private_experiences.append(private_experience)
        
        return private_experiences
    
    def aggregate_models(self):
        """Privacy-preserving federated averaging"""
        if not self.local_models:
            return
            
        print("Performing privacy-preserving federated aggregation...")
        
        # Collect model weights with privacy protection
        all_weights = []
        experience_counts = []
        
        for user_id, local_model in self.local_models.items():
            model_data = local_model.get_model_data()
            
            # Add noise to model weights for privacy
            noisy_weights = self._add_noise_to_weights(model_data['weights'])
            all_weights.append(noisy_weights)
            experience_counts.append(model_data['experience_count'])
        
        # Perform weighted average
        total_experiences = sum(experience_counts)
        if total_experiences == 0:
            return
            
        new_global_weights = {}
        for param_name in all_weights[0].keys():
            param_sum = np.zeros_like(np.array(all_weights[0][param_name]))
            
            for i, weights in enumerate(all_weights):
                weight = experience_counts[i] / total_experiences
                param_sum += np.array(weights[param_name]) * weight
            
            new_global_weights[param_name] = param_sum.tolist()
        
        # Update global model
        self.global_model.set_weights(new_global_weights)
        
        # Distribute global model to all users
        for local_model in self.local_models.values():
            local_model.model.set_weights(new_global_weights)
        
        self.last_aggregation = time.time()
        print("Privacy-preserving aggregation completed")
    
    def _add_noise_to_weights(self, weights: Dict) -> Dict:
        """Add differential privacy noise to model weights"""
        noisy_weights = {}
        for param_name, param_values in weights.items():
            noisy_param = []
            for value in param_values:
                noisy_value = LaplaceMechanism.add_noise(
                    value, 
                    sensitivity=0.01, 
                    epsilon=self.privacy_epsilon
                )
                noisy_param.append(noisy_value)
            noisy_weights[param_name] = noisy_param
        return noisy_weights
    
    def get_global_predictions(self, market_features: Dict) -> Dict[str, float]:
        """Get predictions from global model"""
        return self.global_model.predict(market_features)
    
    def get_user_model(self, user_id: str) -> LocalClientModel:
        """Get a user's local model"""
        return self.local_models.get(user_id)
    
    def save_state(self, filepath: str):
        """Save federated learning state"""
        state = {
            'global_model': self.global_model.get_weights(),
            'last_aggregation': self.last_aggregation,
            'local_models': {uid: model.get_model_data() for uid, model in self.local_models.items()}
        }
        
        with open(filepath, 'w') as f:
            json.dump(state, f)
    
    def load_state(self, filepath: str):
        """Load federated learning state"""
        try:
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            self.global_model.set_weights(state['global_model'])
            self.last_aggregation = state['last_aggregation']
            
            for user_id, model_data in state['local_models'].items():
                if user_id not in self.local_models:
                    self.local_models[user_id] = LocalClientModel(user_id, self.input_dim)
                self.local_models[user_id].load_model_data(model_data)
                
        except FileNotFoundError:
            print("No saved state found, starting fresh")