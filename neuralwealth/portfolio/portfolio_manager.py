import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime, timedelta
from ray import tune
from neuralwealth.portfolio.rl_agent.inferencer import PortfolioInferencer
from neuralwealth.portfolio.rl_agent.trainer import PortfolioTrainer
from neuralwealth.portfolio.rl_agent.environment import PortfolioEnv
from neuralwealth.portfolio.rebalancer.optimizer import PortfolioOptimizer
from neuralwealth.portfolio.rebalancer.compliance import ComplianceEngine
from neuralwealth.portfolio.execution.broker_api import InteractiveBrokersClient
from neuralwealth.portfolio.execution.paper_trading import PaperTradingEngine
from neuralwealth.portfolio.utils.influxdb_client import InfluxDBQuery
from neuralwealth.portfolio.utils.strategy_loader import StrategyLoader

def env_creator(env_config: Dict):
    return PortfolioEnv(env_config)

tune.register_env("PortfolioEnv", env_creator)

class PortfolioManager:
    """Manages portfolio rebalancing using RL, optimization, and execution."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the PortfolioManager with configuration.

        Args:
            config: Dictionary containing:
                - model_path: Path to trained RL model.
                - env: RL environment.
                - env_config: Environment config (e.g., assets, state_size).
                - max_risk: Maximum portfolio volatility.
                - transaction_cost: Cost per unit of turnover.
                - max_turnover: Maximum allowable turnover.
                - constraints: Compliance constraints (sector, liquidity, ESG, leverage).
                - live: Boolean for live vs. paper trading.
                - rebalance_schedule: Rebalancing frequency (e.g., "weekly", "monthly").
                - retrain_schedule: Retraining frequency (e.g., "monthly", "quarterly").
                - train_on_init: Boolean to train RL agent on initialization.
                - episodes: Number of training episodes for RL agent.
                - influxdb_url, influxdb_token, influxdb_org, influxdb_bucket: InfluxDB details.
        """
        self.rl_agent = PortfolioInferencer(config["model_path"], config["env"])
        self.optimizer = PortfolioOptimizer(
            max_risk=config.get("max_risk", 0.2),
            transaction_cost=config.get("transaction_cost", 0.001),
            max_turnover=config.get("max_turnover", 0.1)
        )
        self.compliance = ComplianceEngine(config["constraints"])
        self.executor = InteractiveBrokersClient() if config.get("live", False) else PaperTradingEngine()
        self.data_client = InfluxDBQuery(
            url=config["influxdb_url"],
            token=config["influxdb_token"],
            org=config["influxdb_org"],
            bucket=config["influxdb_bucket"]
        )
        self.strategy_loader = StrategyLoader()
        self.rebalance_schedule = config.get("rebalance_schedule", "weekly")
        self.retrain_schedule = config.get("retrain_schedule", "monthly")
        self.train_on_init = config.get("train_on_init", False)
        self.episodes = config.get("episodes", 100)
        self.model_path = config["model_path"]
        self.env_config = config["env_config"]
        self.last_rebalance = None
        self.last_retrain = None

        # Train RL agent on initialization if specified
        if self.train_on_init:
            self.train_rl_agent()

    def _is_rebalance_due(self, schedule: str) -> bool:
        """
        Check if rebalancing is due based on the schedule.

        Args:
            schedule: Rebalancing frequency ("daily", "weekly", "monthly").

        Returns:
            bool: True if rebalancing is due.
        """
        try:
            if self.last_rebalance is None:
                return True
            now = datetime.now()
            delta_map = {"daily": timedelta(days=1), "weekly": timedelta(weeks=1), "monthly": timedelta(days=30)}
            if now - self.last_rebalance >= delta_map.get(schedule, timedelta(weeks=1)):
                return True
            return False
        except Exception as e:
            return False

    def _is_retrain_due(self, schedule: str) -> bool:
        """
        Check if RL agent retraining is due based on the schedule.

        Args:
            schedule: Retraining frequency ("monthly", "quarterly").

        Returns:
            bool: True if retraining is due.
        """
        try:
            if self.last_retrain is None:
                return True
            now = datetime.now()
            delta_map = {"monthly": timedelta(days=30), "quarterly": timedelta(days=90)}
            if now - self.last_retrain >= delta_map.get(schedule, timedelta(days=30)):
                return True
            return False
        except Exception as e:
            return False

    def train_rl_agent(self) -> Dict:
        """
        Train or retrain the RL agent using PortfolioTrainer.

        Returns:
            Dict: Training metrics (e.g., mean_reward).
        """
        try:
            trainer = PortfolioTrainer({
                "env": self.rl_agent.env,
                "env_config": self.env_config,
                "reward_weights": {"sharpe": 0.5, "drawdown": 0.3, "crash_return": 0.2},
                "num_workers": 4
            })
            result = trainer.train(episodes=self.episodes)
            # Save trained model
            checkpoint_path = self.model_path
            result["algo"].save(checkpoint_path)
            # Reload RL agent with new model
            self.rl_agent = PortfolioInferencer(self.model_path, self.rl_agent.env)
            self.last_retrain = datetime.now()
            return {"status": "success", "metrics": {"mean_reward": result["mean_reward"]}}
        except Exception as e:
            return {"status": "failed", "reason": str(e)}

    def prepare_strategy_inputs(self, hypothesis: Dict) -> Dict:
        """
        Prepare inputs for optimization from hypothesis data.

        Args:
            hypothesis: Hypothesis dictionary with assets, backtest_results, and crash_results.

        Returns:
            Dict: Strategy inputs with expected returns, covariance matrix, and assets.
        """
        try:
            assets = [asset["ticker"] for asset in hypothesis["hypothesis"]["assets"]]
            # Fetch market data
            fields = ["close", "rsi_14", "volume", "esg_score"]
            data = self.data_client.get_asset_data(assets, fields, "2023-01-01", "2025-07-31")
            
            # Derive returns from backtest_results
            returns = {}
            for asset in assets:
                backtest_result = hypothesis["backtest_results"].get(asset, {})
                returns[asset] = backtest_result.get("returns", {}).get("rtot", 0.0)
            # Simulate covariance matrix (mock: replace with historical data if needed)
            n = len(assets)
            cov_matrix = pd.DataFrame(np.eye(n) * 0.1, index=assets, columns=assets)  # Diagonal covariance
            for i, asset1 in enumerate(assets):
                for j, asset2 in enumerate(assets):
                    if i != j:
                        cov_matrix.loc[asset1, asset2] = 0.02  # Mock correlation
            
            strategy_inputs = {
                "expected_returns": returns,
                "cov_matrix": cov_matrix,
                "assets": hypothesis["hypothesis"]["assets"],
                "hypothesis_id": hypothesis["hypothesis"]["id"],
                "restricted_assets": hypothesis["hypothesis"].get("restricted_assets", [])
            }
            return strategy_inputs
        except Exception as e:
            return {}

    def rebalance(self, market_state: Dict, hypotheses: List[Dict]) -> Dict:
        """
        End-to-end rebalancing workflow.

        Args:
            market_state: Current market data (prices, indicators, macro).
            hypotheses: List of hypotheses with backtest_results and crash_results.

        Returns:
            Dict: Rebalancing result with status and weights.
        """
        try:
            # Check if retraining is due
            if self._is_retrain_due(self.retrain_schedule):
                retrain_result = self.train_rl_agent()
                if retrain_result["status"] != "success":
                    return {"status": "skipped", "reason": retrain_result['reason']}

            if not self._is_rebalance_due(self.rebalance_schedule):
                return {"status": "skipped", "reason": "not_due"}

            # Select top hypotheses
            selected_hypotheses = self.strategy_loader.select_strategies(hypotheses, top_n=3)
            if not selected_hypotheses:
                return {"status": "failed", "reason": "no_hypotheses"}

            # Combine strategy weights
            all_weights = {}
            for hypothesis in selected_hypotheses:
                strategy = self.prepare_strategy_inputs(hypothesis)
                if not strategy:
                    continue

                # Build RL state
                current_portfolio = self.executor.get_portfolio()
                state = self.rl_agent.build_state(
                    portfolio=current_portfolio,
                    market_data=market_state.get("market", {}),
                    macro_data=market_state.get("macro", {}),
                    hypothesis=hypothesis["hypothesis"]
                )
                # RL Agent proposes action
                action = self.rl_agent.get_action(state)
                if action["type"] == "hold":
                    continue

                # Optimize weights
                weights = self.optimizer.optimize(
                    strategy["expected_returns"],
                    strategy["cov_matrix"],
                    current_weights=current_portfolio,
                    strategy=strategy
                )
                if not weights:
                    continue

                # Validate against constraints
                assets_df = pd.DataFrame(strategy["assets"]).set_index("ticker")
                assets_df["volume"] = [market_state.get("market", {}).get(ticker, {}).get("volume", 0) for ticker in assets_df.index]
                assets_df["esg_score"] = [market_state.get("market", {}).get(ticker, {}).get("esg_score", 0) for ticker in assets_df.index]
                assets_df["sector"] = [asset.get("sector", "Unknown") for asset in strategy["assets"]]
                if not self.compliance.validate(weights, assets_df):
                    continue

                # Execute trades
                for asset, weight in weights.items():
                    if weight > 0:
                        action_type = "buy" if asset not in current_portfolio or current_portfolio[asset] < weight else "sell"
                        quantity = abs(weight - current_portfolio.get(asset, 0)) * 1000  # Mock position sizing
                        price = market_state.get("market", {}).get(asset, {}).get("close", 100.0)
                        trade_result = self.executor.execute(asset, action_type, quantity, price)
                        if trade_result["status"] != "success":
                            continue
                    all_weights[asset] = weight

            if not all_weights:
                return {"status": "failed", "reason": "no_valid_weights"}

            self.last_rebalance = datetime.now()
            return {"status": "success", "weights": all_weights}
        except Exception as e:
            return {"status": "failed", "reason": str(e)}