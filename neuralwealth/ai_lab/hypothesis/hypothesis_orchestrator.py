from datetime import datetime
from typing import List, Dict, Any, Type
from neuralwealth.ai_lab.hypothesis.hypothesis_initializer import HypothesisInitializer
from neuralwealth.ai_lab.hypothesis.hypothesis_optimizer import HypothesisOptimizer
from neuralwealth.ai_lab.utils.llm_client import LLMClient

class HypothesisOrchestrator:
    """Orchestrates hypothesis initialization and optimization for investment strategies."""

    def __init__(
        self, 
        llmClient: Type[LLMClient], 
        db_url: str, 
        db_token: str, 
        db_org: str, 
        db_bucket: str
    ) -> None:
        """Initialize with LLM and InfluxDB configurations.

        Args:
            openai_base_url: OpenAI SDK base URL.
            openai_api_key: OpenAI SDK API key.
            openai_model: OpenAI LLM model for hypothesis generation.
            db_url: InfluxDB server URL.
            db_token: InfluxDB authentication token.
            db_org: InfluxDB organization.
            db_bucket: InfluxDB bucket.
        """
        self.initializer = HypothesisInitializer(db_url, db_token, db_org, db_bucket)
        self.llm_client = llmClient
        self.optimizer = HypothesisOptimizer(self.initializer.schema)

    def _optimize_hypotheses_for_phase(
        self, 
        input_data: Any, 
        context: str, 
        constraints: str, 
        phase: str, 
        group_name: str
    ) -> List[Dict]:
        """Optimize hypotheses for a single phase using LLM.

        Args:
            input_data: Query results or hypotheses for the phase.
            context: Group name and explanation for context.
            constraints: Constraints for hypothesis generation.
            phase: Stage of optimization ('preliminary', 'validated', 'final').
            group_name: Group identifier for conversation cache.

        Returns:
            List of hypothesis dictionaries for the phase.
        """
        prompt = self.optimizer.prepare_hypothesis_prompt(input_data, context, constraints, phase)
        response = self.llm_client.call(prompt, group_name + f"_{phase}")
        return self.optimizer.parse_hypothesis_response(response, phase)

    def generate_hypotheses(self, 
        tickers: List[Dict], 
        timeframe: str , 
        analysis_focus: str, 
        constraints: str,
    ) -> List[Dict]:
        """Generate and optimize investment hypotheses.

        Args:
            tickers: List of ticker dictionaries.
            timeframe: Time range for analysis.
            analysis_focus: Focus of analysis.
            constraints: Constraints for hypothesis generation.

        Returns:
            Sorted list of hypotheses with metadata.
        """
        # Generate initial plan
        plan_prompt = self.initializer.get_llm_prompt(tickers, timeframe, analysis_focus)
        plan_result = self.llm_client.call(plan_prompt, "initial_plan")
        query_results = self.initializer.process_llm_query_request(plan_result)

        hypotheses = []
        phases = ["preliminary", "validated", "final"]

        for group in query_results:
            group_name = group["group_name"]
            context = f"""{group_name}: {group['explanation']}
            Tickers Analyzed:
            {self.initializer._format_tickers(group['associated_tickers'])}   
            """

            # Start with query results for the first phase
            phase_input = group["queries"]

            # Iterate through phases
            for phase in phases:
                phase_input = self._optimize_hypotheses_for_phase(
                    phase_input, context, constraints, phase, group_name
                )

            hypotheses.extend(phase_input)

        return self._format_output(hypotheses)

    def _format_output(self, hypotheses: List[Dict]) -> List[Dict]:
        """Format hypotheses with metadata and confidence scoring.

        Args:
            hypotheses: List of hypothesis dictionaries.

        Returns:
            Sorted list of hypotheses with metadata.
        """
        scored = []
        for idx, h in enumerate(hypotheses):
            confidence = h.get("confidence", self._estimate_confidence(h.get("strength", "")))
            scored.append({
                **h,
                "id": f"H{datetime.now().strftime('%Y%m%d')}-{idx}",
                "confidence": confidence,
                "last_updated": datetime.now().isoformat()
            })
        return sorted(scored, key=lambda x: x["confidence"], reverse=True)

    def _estimate_confidence(self, strength: str) -> float:
        """Estimate confidence score based on strength or rationale.

        Args:
            strength: Strength or rationale text.

        Returns:
            Confidence score between 0.1 and 0.99.
        """
        strength = strength.lower()
        indicators = {
            "strong": ["p < 0.05", "r² > 0.8", "consistent across"],
            "medium": ["p < 0.1", "r² > 0.6", "multiple instances"],
            "weak": ["anecdotal", "single occurrence", "requires validation"]
        }
        score = 0.5
        for key, terms in indicators.items():
            for term in terms:
                if term in strength:
                    score += 0.2 if key == "strong" else 0.1 if key == "medium" else -0.1
        return min(max(score, 0.1), 0.99)