import json
import re
from typing import List, Dict, Any
from neuralwealth.ai_lab.hypothesis.resources.llm_strategy_prompts import LLM_STRATEGY_PROMPTS

class HypothesisOptimizer:
    """Optimizes investment hypotheses through iterative stages."""

    def __init__(self, data_schema: Dict) -> None:
        """Initialize with data schema.

        Args:
            data_schema: InfluxDB schema for context.
        """
        self.data_schema = data_schema

    def prepare_hypothesis_prompt(
        self, 
        input_data: Any, 
        context: str, 
        constraints: str, 
        phase: str
    ) -> str:
        """Prepare prompt for LLM hypothesis optimization.

        Args:
            input_data: Query results (for preliminary) or hypotheses (for validated/final).
            context: Group name and explanation for context.
            constraints: Constraints for hypothesis generation.
            phase: Stage of optimization ('preliminary', 'validated', 'final').

        Returns:
            Formatted prompt string for LLM.
        """
        data_samples = "\n".join(
            q["result"] for q in input_data if isinstance(input_data, list) and "result" in q
        ) if phase == "preliminary" else json.dumps([
            {
                "pattern": h["thesis"],
                "assets": h["assets"],
                "strength": h.get("strength", "MEDIUM")
            } for h in input_data
        ], indent=2)

        return LLM_STRATEGY_PROMPTS[phase].format(
            context=context,
            data_schema=self.data_schema,
            constraints=constraints,
            data_samples=data_samples,
            patterns=data_samples
        )

    def parse_hypothesis_response(self, text: str, phase: str) -> List[Dict]:
        """Parse LLM JSON response into structured hypotheses.

        Args:
            text: Raw LLM response text (JSON).
            phase: Stage of optimization ('preliminary', 'validated', 'final').

        Returns:
            List of hypothesis dictionaries.
        """
        try:
            json_match = re.search(r'\[\s*{.*}\s*\]', text, re.DOTALL)
            if not json_match:
                return []

            hypotheses = json.loads(json_match.group(0))
            if not isinstance(hypotheses, list):
                return []

            if phase in ["preliminary", "validated"]:
                return [
                    {
                        "thesis": h["pattern"],
                        "assets": h["assets"],
                        "strength": h["strength"]
                    } for h in hypotheses if "pattern" in h and "assets" in h and "strength" in h
                ]
            else:
                return [
                    {
                        "thesis": h["hypothesis"],
                        "assets": h["assets"],
                        "trigger": h["trigger"],
                        "timeframe": h["timeframe"],
                        "strategy": h["strategy"],
                        "confidence": float(h["confidence"]),
                        "risks": h["risks"]
                    } for h in hypotheses
                    if all(k in h for k in ["hypothesis", "assets", "trigger", "timeframe", "confidence", "risks", "strategy"])
                ]
        except Exception:
            return []