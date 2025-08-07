from typing import Dict, Any, List
from neuralwealth.ai_lab.hypothesis.hypothesis_orchestrator import HypothesisOrchestrator
from neuralwealth.ai_lab.hypothesis.hypothesis_refiner import HypothesisRefiner
from neuralwealth.ai_lab.hypothesis.rule_filter import RuleBasedFilter
from neuralwealth.ai_lab.utils.backtest_client import BackTestDataClient
from neuralwealth.ai_lab.backtesting.backtesting import BacktestEngine
from neuralwealth.ai_lab.stress_testing.market_crashes import CrashScenarioTester
from neuralwealth.ai_lab.knowledge_graph.neo4j_connector import Neo4jConnector
from neuralwealth.ai_lab.utils.llm_client import LLMClient
from neuralwealth.ai_lab.utils.result_evaluator import ResultEvaluator

class RobotScientist:
    """Generates, tests, refines, and stores investment hypotheses."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the RobotScientist with configuration for hypothesis generation, testing, and storage.

        Args:
            config: Dictionary containing:
                - openai_sdk_base_url: OpenAI API base URL.
                - open_ai_sdk_api_key: OpenAI API key.
                - llm_model: LLM model name.
                - influxdb_url: InfluxDB URL.
                - influxdb_token: InfluxDB token.
                - influxdb_org: InfluxDB organization.
                - influxdb_bucket: InfluxDB bucket.
                - neo4j_uri: Neo4j database URI.
                - neo4j_user: Neo4j username.
                - neo4j_password: Neo4j password.
                - initial_cash: Initial cash for backtesting (default: 100000.0).
                - commission: Broker commission per trade (default: 0.001).
                - risk_free_rate: Risk-free rate for Sharpe ratio (default: 0.01).
                - evaluation_criteria: Dictionary of performance thresholds (optional).
        """
        self.llm_client = LLMClient( 
            base_url=config["openai_sdk_base_url"],
            api_key=config["open_ai_sdk_api_key"],
            model=config["llm_model"]
        )
        self.hypothesis_generator = HypothesisOrchestrator(
            self.llm_client,
            db_url=config["influxdb_url"],
            db_token=config["influxdb_token"],
            db_org=config["influxdb_org"],
            db_bucket=config["influxdb_bucket"]
        )
        self.rule_filter = RuleBasedFilter()
        self.backtest_feed = BackTestDataClient(
            url=config["influxdb_url"],
            token=config["influxdb_token"],
            org=config["influxdb_org"],
            bucket=config["influxdb_bucket"]
        )
        self.backtest_engine = BacktestEngine(
            cash=config.get("initial_cash", 100000.0),
            commission=config.get("commission", 0.001),
            risk_free_rate=config.get("risk_free_rate", 0.01)
        )
        self.crash_scenario_tester = CrashScenarioTester(self.backtest_engine)
        self.neo4j = Neo4jConnector(
            uri=config["neo4j_uri"],
            user=config["neo4j_user"],
            password=config["neo4j_password"]
        )
        self.evaluator = ResultEvaluator(config.get("evaluation_criteria"))
        self.refiner = HypothesisRefiner(self.llm_client)

    def run_research_cycle(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Run a research cycle: generate, filter, test, refine, and store hypotheses.

        Args:
            params: Dictionary containing:
                - tickers: List of asset tickers.
                - excluded_assets: List of assets to exclude.
                - rules: Filtering rules for hypotheses.

        Returns:
            List of dictionaries containing hypothesis and test results for stored hypotheses.
        """
        stored_results = []

        # 1. Generate hypotheses
        raw_hypotheses = self.hypothesis_generator.generate_hypotheses(
            tickers = params["tickers"],
            timeframe = params["timeframe"],
            analysis_focus  = params["analysis_focus"],
            constraints = params["constraints"]
        )

        # 2. Apply fundamental filters
        hypotheses = self.rule_filter.apply(raw_hypotheses, params["excluded_assets"], params["rules"])

        # 3. Backtest and refine
        for hypothesis in hypotheses:
            assets = hypothesis["assets"]
            fields = hypothesis["strategy"]["data_feed"]
            data = self.backtest_feed.get_asset_data(assets, fields, "2001-01-01", "2025-01-01")

            # Run initial backtest and crash test
            backtest_results = self.backtest_engine.run(hypothesis, data)

            crash_results = self.crash_scenario_tester.test_strategy(hypothesis, data)

            # Evaluate results
            is_satisfactory = self.evaluator.evaluate_results(backtest_results, crash_results)

            if is_satisfactory:
                # Attempt LLM-based refinement
                refined_hypothesis = self.refiner.refine(hypothesis, backtest_results, crash_results, self.evaluator.criteria)
                if refined_hypothesis:
                    # Re-run tests for refined hypothesis
                    backtest_results = self.backtest_engine.run(refined_hypothesis, data)
                    crash_results = self.crash_scenario_tester.test_strategy(refined_hypothesis, data)

                    if self.evaluator.evaluate_results(backtest_results, crash_results):
                        # Store refined hypothesis if satisfactory
                        self.neo4j.store_strategy(refined_hypothesis, backtest_results, crash_results)
                        stored_results.append({
                            "hypothesis": refined_hypothesis,
                            "backtest_results": backtest_results,
                            "crash_results": crash_results
                        })
                continue 
            else:
                # Store original hypothesis if satisfactory
                self.neo4j.store_strategy(hypothesis, backtest_results, crash_results)
                stored_results.append({
                    "hypothesis": hypothesis,
                    "backtest_results": backtest_results,
                    "crash_results": crash_results
                })
    
        return stored_results