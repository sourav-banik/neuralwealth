from typing import List, Dict, Any
from neuralwealth.ai_lab.agents.base_agent import BaseAgent
from neuralwealth.ai_lab.utils.backtest_client import BackTestDataClient
from neuralwealth.ai_lab.utils.synthetic_data_client import SyntheticDataClient
from neuralwealth.ai_lab.backtesting.backtesting import BacktestEngine
from neuralwealth.ai_lab.stress_testing.crash_scenario_tester import CrashScenarioTester

class BacktestingAgent(BaseAgent):
    """Agent 2: Tests strategies against historical and synthetic data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("backtesting_agent", config)
        self.data_client = BackTestDataClient(
            config["influxdb_url"],
            config["influxdb_token"],
            config["influxdb_org"],
            config["influxdb_bucket"]
        )
        self.synthetic_client = SyntheticDataClient(
            config["influxdb_url"],
            config["influxdb_token"],
            config["influxdb_org"],
            config["influxdb_bucket"]
        )
        self.backtester = BacktestEngine(
            cash=config.get("initial_cash", 100000.0),
            commission=config.get("commission", 0.001),
            risk_free_rate=config.get("risk_free_rate", 0.01)
        )
        self.crash_tester = CrashScenarioTester(self.backtester, self.synthetic_client)
        
    def execute(self, hypotheses: List[Dict]) -> List[Dict]:
        """Test hypotheses against multiple regimes"""
        print(f"{self.agent_id}: Testing {len(hypotheses)} hypotheses...")
        
        tested_hypotheses = []
        
        for hypothesis in hypotheses:
            assets = hypothesis["assets"]
            fields = hypothesis["strategy"]["data_feed"]
            
            # Get historical data
            historical_data = self.data_client.get_asset_data(
                assets, fields, "2000-01-01", "2025-01-01"
            )

            # Test on historical data
            historical_results = self.backtester.run(hypothesis, historical_data)
            
            # Test on synthetic crash scenarios
            crash_results = self.crash_tester.test_strategy(hypothesis, historical_data)
            
            hypothesis["test_results"] = {
                "historical": historical_results,
                "synthetic_crashes": crash_results
            }
            
            tested_hypotheses.append(hypothesis)
        
        print(f"{self.agent_id}: Completed testing")
        return tested_hypotheses