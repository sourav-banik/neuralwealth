import numpy as np
from typing import Dict, List

class BasePortfolioModel:
    """Simple neural network for portfolio optimization with CVaR"""
    
    def __init__(self, input_dim: int = 10, hidden_dim: int = 16):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.weights = self._initialize_weights()
        
    def _initialize_weights(self) -> Dict[str, np.ndarray]:
        """Initialize model weights"""
        return {
            'w1': np.random.randn(self.input_dim, self.hidden_dim) * 0.1,
            'b1': np.zeros(self.hidden_dim),
            'w2': np.random.randn(self.hidden_dim, 1) * 0.1,
            'b2': np.zeros(5)
        }
    
    def forward(self, x: np.ndarray) -> np.ndarray:
        """Forward pass through the network"""
        # Layer 1
        z1 = np.dot(x, self.weights['w1']) + self.weights['b1']
        a1 = np.tanh(z1)
        
        # Layer 2 (output portfolio weights)
        z2 = np.dot(a1, self.weights['w2']) + self.weights['b2']
        weights = np.exp(z2) / np.sum(np.exp(z2))  # Softmax for valid weights
        
        return weights
    
    def predict(self, market_features: Dict) -> Dict[str, float]:
        """Predict portfolio weights from market features"""
        # Define assets explicitly
        assets = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        # Convert features to numpy array
        feature_vector = np.array([market_features.get(f, 0) for f in sorted(market_features.keys())])
        
        # Ensure feature vector matches input_dim
        if len(feature_vector) != self.input_dim:
            feature_vector = np.pad(feature_vector, (0, max(0, self.input_dim - len(feature_vector))))
            feature_vector = feature_vector[:self.input_dim]
        
        # Forward pass to get weights
        weights = self.forward(feature_vector)
        
        # Ensure weights length matches number of assets
        if len(weights) != len(assets):
            raise ValueError(f"Expected {len(assets)} weights, got {len(weights)}")
        
        return dict(zip(assets, weights.flatten()))
    
    def get_weights(self) -> Dict[str, np.ndarray]:
        """Get model weights as serializable dict"""
        return {k: v.tolist() for k, v in self.weights.items()}
    
    def set_weights(self, weights: Dict[str, List]):
        """Set model weights from serialized dict"""
        for key in self.weights:
            if key in weights:
                self.weights[key] = np.array(weights[key])
    
    def copy(self) -> 'BasePortfolioModel':
        """Create a copy of the model"""
        new_model = BasePortfolioModel(self.input_dim, self.hidden_dim)
        new_model.set_weights(self.get_weights())
        return new_model