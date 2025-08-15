import numpy as np
from gymnasium import Env
from typing import Dict, Any, Tuple
from gymnasium import spaces

class PortfolioEnv(Env):
    """RL environment for portfolio management, compatible with Ray RLlib."""

    def __init__(self, env_config: Dict[str, Any]):
        """
        Initialize the PortfolioEnv.

        Args:
            env_config: Dictionary containing:
                - assets: List of asset tickers (e.g., ["AAPL", "MSFT", "EURUSD"]).
                - state_size: Size of the state vector (must match observation space).
                - initial_cash: Initial portfolio cash (default: 100000).
                - transaction_cost: Cost per unit of turnover (default: 0.001).
        """
        super().__init__()
        self.assets = env_config.get("assets", [])
        self.state_size = env_config.get("state_size", 0)
        self.initial_cash = env_config.get("initial_cash", 100000.0)
        self.transaction_cost = env_config.get("transaction_cost", 0.001)
        if not self.assets or self.state_size <= 0:
            raise ValueError("Invalid env_config: 'assets' must be non-empty and 'state_size' must be positive")

        # Define action and observation spaces
        self.action_space = spaces.Discrete(3)  # 0: hold, 1: buy, 2: sell
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=(self.state_size,), dtype=np.float32)

        # Initialize portfolio and state
        self.portfolio = {"CASH": self.initial_cash}
        for asset in self.assets:
            self.portfolio[asset] = 0.0
        self.current_step = 0
        self.max_steps = 100  # Max steps per episode
        self.hypothesis = {}  # To be updated during step
        self.market_data = {}
        self.macro_data = {}

    def validate(self):
        try:
            obs, _ = self.reset()
            action = self.action_space.sample()
            obs, reward, terminated, truncated, info = self.step(action)
            return True
        except Exception as e:
            print(f"Environment validation failed: {str(e)}")
            return False
        
    def get_config(self) -> Dict[str, Any]:
        """
        Return the environment configuration.

        Returns:
            Dict: Configuration with assets and state_size.
        """
        return {"assets": self.assets, "state_size": self.state_size}

    def reset(self, *, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """
        Reset the environment to initial state.
        
        Args:
            seed: Optional seed for random number generation
            options: Additional options for environment reset
        
        Returns:
            Tuple containing:
            - np.ndarray: Initial state vector
            - Dict: Additional information (can be empty)
        """
        try:
            # Handle seed if provided
            if seed is not None:
                np.random.seed(seed)
                
            self.portfolio = {"CASH": self.initial_cash}
            for asset in self.assets:
                self.portfolio[asset] = 0.0
            self.current_step = 0
            self.hypothesis = {}
            self.market_data = {asset: {"close": 100.0, "rsi": 50.0, "volume": 100000} 
                            for asset in self.assets}
            self.macro_data = {"fed_funds_rate": 0.02, "cpi_inflation": 0.03}
            
            # Gymnasium requires returning both observation and info dict
            return self._build_state(), {}
            
        except Exception as e:
            # Return zeros for observation and empty dict for info
            return np.zeros(self.state_size, dtype=np.float32), {}

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """
        Execute one step in the environment.

        Args:
            action: Integer action (0: hold, 1: buy, 2: sell).

        Returns:
            Tuple: (state, reward, terminated, truncated, info).
        """
        try:
            # Decode action
            action_map = {
                0: "hold",
                1: "buy",
                2: "sell"
            }
            action_type = action_map.get(action, "hold")
            assets_to_trade = self.assets[:2] if action_type == "buy" else self.assets[-1:] if action_type == "sell" else []

            # Update portfolio
            if action_type in ["buy", "sell"]:
                for asset in assets_to_trade:
                    price = self.market_data.get(asset, {}).get("close", 100.0)
                    quantity = 1000  # Fixed quantity (replace with dynamic sizing)
                    cost = quantity * price * self.transaction_cost
                    if action_type == "buy" and self.portfolio["CASH"] >= price * quantity + cost:
                        self.portfolio[asset] = self.portfolio.get(asset, 0.0) + quantity
                        self.portfolio["CASH"] -= (price * quantity + cost)
                    elif action_type == "sell" and self.portfolio.get(asset, 0.0) >= quantity:
                        self.portfolio[asset] -= quantity
                        self.portfolio["CASH"] += (price * quantity - cost)

            # Simulate market update (mock)
            for asset in self.assets:
                self.market_data[asset]["close"] *= (1 + np.random.normal(0, 0.01))  # Random price change
                self.market_data[asset]["rsi"] = min(max(self.market_data[asset]["rsi"] + np.random.normal(0, 5), 0), 100)
                self.market_data[asset]["volume"] *= (1 + np.random.normal(0, 0.05))

            # Compute reward
            reward = self._compute_reward()
            self.current_step += 1
            terminated = self.current_step >= self.max_steps
            truncated = False  # Gymnasium requires this, set to False if not using early termination
            state = self._build_state()
            info = {"action": action_type, "portfolio": self.portfolio.copy()}

            return state, reward, terminated, truncated, info
            
        except Exception:
            return np.zeros(self.state_size, dtype=np.float32), 0.0, True, False, {}

    def _build_state(self) -> np.ndarray:
        """
        Build state vector from portfolio, market, macro, and hypothesis data.

        Returns:
            np.ndarray: State vector.
        """
        try:
            state = []
            # Portfolio: weights and cash
            state.extend([self.portfolio.get(asset, 0.0) for asset in self.assets])
            state.append(self.portfolio.get("CASH", 0.0) / 100000.0)  # Normalize cash
            # Market: prices and indicators
            for asset in self.assets:
                state.extend([
                    self.market_data.get(asset, {}).get("close", 0.0) / 100.0,  # Normalize price
                    self.market_data.get(asset, {}).get("rsi", 0.0) / 100.0,
                    self.market_data.get(asset, {}).get("volume", 0.0) / 1e6
                ])
            # Macro: economic indicators
            state.extend([self.macro_data.get(key, 0.0) for key in ["fed_funds_rate", "cpi_inflation"]])
            # Hypothesis: metadata
            state.extend([self.hypothesis.get("confidence", 0.0), self.hypothesis.get("expected_return", 0.0)])
            state_array = np.array(state, dtype=np.float32)
            if state_array.shape[0] != self.state_size:
                raise ValueError(f"State size mismatch: expected {self.state_size}, got {state_array.shape[0]}")
            return state_array
        except Exception:
            return np.zeros(self.state_size, dtype=np.float32)

    def _compute_reward(self) -> float:
        """
        Compute reward based on portfolio performance and hypothesis results.

        Returns:
            float: Weighted reward combining Sharpe, drawdown, and crash return.
        """
        try:
            results = self.hypothesis.get("backtest_results", {}).get(self.assets[0], {}) if self.hypothesis else {}
            sharpe = results.get("sharpe", {}).get("sharperatio", 0.0)
            drawdown = results.get("drawdown", {}).get("maxdrawdown", 0.0)
            crash_return = min(
                [res.get("returns", {}).get("rtot", 0.0) for res in self.hypothesis.get("crash_results", {}).values()],
                default=0.0
            )
            reward = (
                0.5 * sharpe -  # reward_weights["sharpe"]
                0.3 * drawdown +  # reward_weights["drawdown"]
                0.2 * crash_return  # reward_weights["crash_return"]
            )
            return reward
        except Exception:
            return 0.0

    def update_hypothesis(self, hypothesis: Dict):
        """
        Update the hypothesis used for reward computation.

        Args:
            hypothesis: Hypothesis dictionary with backtest_results and crash_results.
        """
        self.hypothesis = hypothesis