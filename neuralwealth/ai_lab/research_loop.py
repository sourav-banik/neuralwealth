from typing import Dict, List, Any
from neuralwealth.ai_lab.agents.hypothesis_engine import HypothesisEngine
from neuralwealth.ai_lab.agents.backtesting_agent import BacktestingAgent
from neuralwealth.ai_lab.agents.kg_curator import KGCurator
from neuralwealth.ai_lab.hypothesis.hypothesis_refiner import HypothesisRefiner
from neuralwealth.ai_lab.utils.llm_client import LLMClient
from neuralwealth.ai_lab.utils.result_evaluator import ResultEvaluator

class RobotScientist:
    """Coordinates the three-agent system for research cycles"""
    
    def __init__(self, config: Dict[str, Any]):
        self.llm_client = LLMClient( 
            base_url=config["openai_sdk_base_url"],
            api_key=config["open_ai_sdk_api_key"],
            model=config["llm_model"]
        )
        config['llm_client'] = self.llm_client
        self.agents = {
            "hypothesis_engine": HypothesisEngine(config),
            "backtesting_agent": BacktestingAgent(config),
            "kg_curator": KGCurator(config)
        }
        self.refiner = HypothesisRefiner(self.llm_client)
        self.evaluator = ResultEvaluator()
        
    def run_research_cycle(self, research_params: Dict) -> List[Dict]:
        """Execute full research cycle through all agents"""
        print("=== Starting Multi-Agent Research Cycle ===")
        
        # Agent 1: Generate hypotheses
        hypotheses = self.agents["hypothesis_engine"].execute(research_params)
        
        if not hypotheses:
            print("No valid hypotheses generated")
            return []
        
        # Agent 2: Test hypotheses
        tested_hypotheses = self.agents["backtesting_agent"].execute(hypotheses)
        
        if not tested_hypotheses:
            print("No hypotheses passed backtesting")
            return []

        # refine loop; currently one loop, increase number of refinement for better strategies
        refined_results = []
        for strategy in tested_hypotheses:
            is_satisfactory = self.evaluator.evaluate_results(
                strategy["test_results"]["historical"], 
                strategy["test_results"]["synthetic_crashes"]
            )
            if not is_satisfactory:
                refined = self.refiner.refine(
                    strategy,
                    self.evaluator.criteria
                )
                if refined:
                    refined_results.append(refined)
        # Agent 2: Test Refined hypotheses
        retested_hypotheses = self.agents["backtesting_agent"].execute(refined_results)
        
        # Agent 3: Store successful strategies
        successful_strategies = self.agents["kg_curator"].execute(retested_hypotheses)
        
        print(f"=== Research Cycle Complete: {len(successful_strategies)} Strategies Found ===")
        return successful_strategies
    
    def get_agent_status(self) -> Dict[str, str]:
        """Get status of all agents"""
        return {agent_id: "active" for agent_id in self.agents.keys()}