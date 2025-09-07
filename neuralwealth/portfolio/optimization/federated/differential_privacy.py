import numpy as np

class LaplaceMechanism:
    """Simplified differential privacy mechanism"""
    
    @staticmethod
    def add_noise(value: float, sensitivity: float, epsilon: float) -> float:
        """
        Add Laplace noise for differential privacy
        
        Args:
            value: Original value
            sensitivity: Maximum change one data point can cause
            epsilon: Privacy budget
            
        Returns:
            Noisy value
        """
        scale = sensitivity / epsilon
        noise = np.random.laplace(0, scale)
        return value + noise
    
    @staticmethod
    def add_noise_to_array(values: np.ndarray, sensitivity: float, epsilon: float) -> np.ndarray:
        """Add noise to an array of values"""
        scale = sensitivity / epsilon
        noise = np.random.laplace(0, scale, values.shape)
        return values + noise