from typing import Dict, List, Any
from neuralwealth.ai_lab.agents.base_agent import BaseAgent
from neuralwealth.ai_lab.hypothesis.hypothesis_orchestrator import HypothesisOrchestrator
from neuralwealth.ai_lab.hypothesis.rule_filter import RuleBasedFilter
from neuralwealth.ai_lab.hypothesis.z3_prover import Z3TheoremProver

class HypothesisEngine(BaseAgent):
    """Agent 1: Generates and validates hypotheses"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("hypothesis_engine", config)
        
        self.orchestrator = HypothesisOrchestrator(
            config["llm_client"],
            config["influxdb_url"],
            config["influxdb_token"], 
            config["influxdb_org"],
            config["influxdb_bucket"]
        )
        self.prover = Z3TheoremProver()
        self.rule_filter = RuleBasedFilter()
        
    def execute(self, research_params: Dict) -> List[Dict]:
        """Generate and validate hypotheses"""
        print(f"{self.agent_id}: Generating hypotheses...")
        
        # 1. Generate hypotheses
        raw_hypotheses = self.orchestrator.generate_hypotheses(
            tickers = research_params["tickers"],
            timeframe = research_params["timeframe"],
            analysis_focus  = research_params["analysis_focus"],
            constraints = research_params["constraints"]
        )

        # 2. Apply fundamental filters
        valid_hypotheses = self.rule_filter.apply(raw_hypotheses, research_params["excluded_assets"], research_params["rules"])
        
        print(f"{self.agent_id}: Generated {len(valid_hypotheses)} valid hypotheses")
        return valid_hypotheses