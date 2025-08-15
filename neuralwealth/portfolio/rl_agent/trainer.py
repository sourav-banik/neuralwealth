import numpy as np
from ray.rllib.algorithms.ppo import PPOConfig
from typing import Dict, Any

class PortfolioTrainer:
    """Trains an RL agent for portfolio management using PPO."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the PortfolioTrainer with RL configuration.

        Args:
            config: Dictionary containing:
                - env: RL environment class.
                - env_config: Environment configuration (e.g., asset universe, transaction costs).
                - reward_weights: Weights for reward components (sharpe, drawdown, crash_return).
                - num_workers: Number of rollout workers for parallel training.
        """
        self.reward_weights = config.get("reward_weights", {
            "sharpe": 0.5,
            "drawdown": 0.3,
            "crash_return": 0.2
        })
        self.config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True
            )
            .environment(
                env=config["env"],
                env_config={
                    "assets": ["AAPL", "MSFT"],
                    "state_size": 20,
                    "initial_cash": 100000,
                    "transaction_cost": 0.001
                }
            )
            .framework("torch")
            .env_runners(
                num_env_runners=config.get("num_workers", 4)
            )
            .training(
                gamma=0.99,
                lr=0.001,
                train_batch_size=4000
            )
        )
        self.env_config = config["env_config"]

    def compute_reward(self, results: Dict) -> float:
        """
        Compute multi-objective reward based on portfolio performance.

        Args:
            results: Dictionary with backtest and crash test results.

        Returns:
            float: Weighted reward combining Sharpe, drawdown, and crash return.
        """
        try:
            sharpe = results.get("sharpe", {}).get("sharperatio", 0.0)
            drawdown = results.get("drawdown", {}).get("maxdrawdown", 0.0)
            crash_return = min([res.get("returns", {}).get("rtot", 0.0) for res in results.get("crash_results", {}).values()], default=0.0)
            reward = (
                self.reward_weights["sharpe"] * sharpe -
                self.reward_weights["drawdown"] * drawdown +
                self.reward_weights["crash_return"] * crash_return
            )
            return reward
        except Exception:
            return 0.0

    def train(self, episodes: int = 10, checkpoint_path: str = None) -> Dict:
        try:
            # Updated configuration with new API stack settings
            self.config = (
                self.config
                .api_stack(
                    enable_rl_module_and_learner=True,
                    enable_env_runner_and_connector_v2=True
                )
                .reporting(
                    metrics_num_episodes_for_smoothing=10,
                    min_sample_timesteps_per_iteration=1000,
                )
            )
            
            # Build the algorithm with the new method
            algo = self.config.build()  # Or build_algo() depending on Ray version
            
            results = []
            for i in range(episodes):
                # Train and get results
                result = algo.train()
                
                # Handle different metric names in new API
                reward_metric = result.get('env_runners/episode_return_mean', 
                                        result.get('episode_reward_mean', 0.0))
                print(f"Iteration {i}: Reward={reward_metric}")
                results.append(reward_metric)
                
                # Periodic saving
                if checkpoint_path and i % 10 == 0:
                    algo.save(checkpoint_path)
                    
            return {
                "mean_reward": np.mean(results),
                "algo": algo,
                "final_metrics": result
            }
            
        except Exception as e:
            print(f"Training failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "failed", "reason": str(e)}