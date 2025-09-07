from typing import Dict, List
from neuralwealth.portfolio.optimization.federated.federated_averager import FederatedAverager
from neuralwealth.portfolio.optimization.federated.local_client import LocalClientModel

# Global federated averager instance
federated_averager = FederatedAverager(input_dim=15)

def get_user_model(user_id: str) -> LocalClientModel:
    """Get or create user model"""
    return federated_averager.register_user(user_id)

def update_user_experience(user_id: str, experiences: List[Dict]):
    """Update user model with new trading experiences"""
    federated_averager.update_user_model(user_id, experiences)

def get_global_predictions(market_features: Dict) -> Dict[str, float]:
    """Get portfolio predictions from global model"""
    return federated_averager.get_global_predictions(market_features)

def get_federated_status() -> Dict:
    """Get status of federated learning system"""
    return {
        'total_users': len(federated_averager.local_models),
        'last_aggregation': federated_averager.last_aggregation,
        'privacy_epsilon': federated_averager.privacy_epsilon
    }