import numpy as np
from ray.rllib.algorithms.algorithm import Algorithm
from typing import Dict, Any
import os

class PortfolioInferencer:
    """Generates portfolio actions using a trained RL model."""

    def __init__(self, model_path: str, env: Any):
        """
        Initialize the PortfolioInferencer with a trained RL model.

        Args:
            model_path: Path to the trained RL model checkpoint.
            env: RL environment for live predictions.
        """
        self.env = env
        self.env_config = env.get_config() if hasattr(env, 'get_config') else {"assets": [], "state_size": 0}
        self.model = self._load_model(model_path)

    def _load_model(self, model_path: str) -> Any:
        """
        Load the trained RL model using Ray RLlib.

        Args:
            model_path: Path to the model checkpoint directory or file.

        Returns:
            Trained RL model (Algorithm instance) or None if loading fails.

        Raises:
            Exception: If model loading fails and no fallback is possible.
        """
        try:
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Checkpoint path {model_path} does not exist.")
            model = Algorithm.from_checkpoint(model_path)
            return model
        except Exception as e:
            # Fallback: Return None and let get_action handle the failure
            return None

    def build_state(self, portfolio: Dict, market_data: Dict, macro_data: Dict, hypothesis: Dict) -> np.ndarray:
        """
        Build state for RL model from portfolio, market, macro, and hypothesis data.

        Args:
            portfolio: Current holdings and cash balance.
            market_data: Price, volume, and indicator data.
            macro_data: Economic indicators (e.g., fed_funds_rate).
            hypothesis: Hypothesis metadata (e.g., confidence, expected_return).

        Returns:
            np.ndarray: Flattened state vector.
        """
        try:
            state = []
            # Portfolio: weights and cash
            state.extend([portfolio.get(asset, 0.0) for asset in self.env_config["assets"]])
            state.append(portfolio.get("CASH", 0.0) / 100000.0)  # Normalize cash
            # Market: prices and indicators
            for asset in self.env_config["assets"]:
                state.extend([
                    market_data.get(asset, {}).get("close", 0.0) / 100.0,  # Normalize price
                    market_data.get(asset, {}).get("rsi", 0.0) / 100.0,
                    market_data.get(asset, {}).get("volume", 0.0) / 1e6
                ])
            # Macro: economic indicators
            state.extend([macro_data.get(key, 0.0) for key in ["fed_funds_rate", "cpi_inflation"]])
            # Hypothesis: metadata
            state.extend([hypothesis.get("confidence", 0.0), hypothesis.get("expected_return", 0.0)])
            state_array = np.array(state, dtype=np.float32)
            return state_array
        except Exception:
            return np.zeros(self.env_config["state_size"], dtype=np.float32)

    def get_action(self, state: np.ndarray) -> Dict:
        """
        Generate buy/sell/hold signals based on current state.

        Args:
            state: State vector from build_state.

        Returns:
            Dict: Action dictionary with type and assets.
        """
        try:
            if self.model is None:
                return {"type": "hold", "assets": []}
            action = self.model.compute_single_action(state)
            decoded_action = self._decode_action(action)
            return decoded_action
        except Exception:
            return {"type": "hold", "assets": []}

    def _decode_action(self, action: int) -> Dict:
        """
        Convert RL action to trade instructions.

        Args:
            action: Integer action from RL model.

        Returns:
            Dict: Trade instructions (type and assets).
        """
        try:
            action_map = {
                0: {"type": "hold", "assets": []},
                1: {"type": "buy", "assets": self.env_config["assets"][:2]},  # Top 2 assets
                2: {"type": "sell", "assets": self.env_config["assets"][-1:]}  # Last asset
            }
            return action_map.get(action, {"type": "hold", "assets": []})
        except Exception:
            return {"type": "hold", "assets": []}