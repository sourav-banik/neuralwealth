from typing import List, Dict, Any
from neuralwealth.ai_lab.agents.base_agent import BaseAgent
from neuralwealth.ai_lab.knowledge_graph.neo4j_connector import Neo4jConnector
from neuralwealth.ai_lab.utils.result_evaluator import ResultEvaluator

class KGCurator(BaseAgent):
    """Agent 3: Builds causal knowledge graphs from validated strategies"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("kg_curator", config)
        self.neo4j = Neo4jConnector(
            config["neo4j_uri"],
            config["neo4j_user"],
            config["neo4j_password"]
        )
        
    def execute(self, tested_hypotheses: List[Dict]) -> List[Dict]:
        """Evaluate and store successful strategies as causal graphs"""
        print(f"{self.agent_id}: Evaluating and storing strategies...")
        
        successful_strategies = []
        
        for hypothesis in tested_hypotheses:
            results = hypothesis["test_results"]
            
            # Store as causal subgraph
            self.neo4j.store_strategy(
                hypothesis, 
                results["historical"], 
                results["synthetic_crashes"]
            )
            successful_strategies.append(hypothesis)
        
        print(f"{self.agent_id}: Stored {len(successful_strategies)} successful strategies")
        return successful_strategies