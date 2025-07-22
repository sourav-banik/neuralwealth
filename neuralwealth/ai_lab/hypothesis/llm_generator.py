import json, re
from datetime import datetime
from typing import List, Dict, Any
from openai import OpenAI
from neuralwealth.ai_lab.utils.data_feeder import DataFeeder


class LLMGenerator:
    """Generates investment hypotheses from DataFeeder results with context caching."""

    def __init__(self, 
                 openai__base_url: str, openai_api_key: str, openai_model: str ,
                 db_url: str, db_token: str, db_org: str, db_bucket: str
                ) -> None:
        """Initialize with LLM API and DataFeeder.

        Args:
            openai__base_url: OpenAI sdk base url,
            openai_api_key: OpenAI sdk API key,
            openai_model: OpenAI sdk LLM model to generate hypothesis with,
            db_url: InfluxDB server url,
            db_token: InfluxDB authentication token,
            db_org: InfluxDB organization, 
            db_bucket: InfluxDB bucket
        """
        self.client = OpenAI(
            base_url=openai__base_url,
            api_key=openai_api_key,
        )
        self.datafeed = DataFeeder(db_url, db_token, db_org, db_bucket)
        self.llm_model = openai_model
        self.conversation_cache: Dict[str, List[Dict]] = {}

    def generate_hypotheses(self, 
                            tickers: List[Dict] = [], 
                            timeframe: str = "2015-01-01 to 2019-12-31", 
                            analysis_focus: str = "fundamental_technical", 
                            constraints: str = """The hypotheses should be ticker specific, not group focused like sector, industry etc. 
                            Don't use any other indicator or parameter that are not mentioned in the data schema. The hypotheses should be
                            testable using python backtrader framework."""
                            ) -> List[Dict]:
        """Generate investment hypotheses with context caching.

        Args:
            tickers: List of tickers used in the analysis
            timeframe:  Timeframe of analysis
            analysis_focus: Focus of analysis
            constraints: Constraints of analysis

        Returns:
            List of dictionaries containing scored hypotheses.
        """
        
        # Step 1: Generate analysis plan with DataFeeder
        plan_prompt = self.datafeed.get_llm_prompt(tickers, timeframe, analysis_focus)
        plan_result = self._llm_call(plan_prompt, group_name="initial_plan")
        query_results = self.datafeed.process_llm_query_request(plan_result)
        
        # Step 2: Iterative hypothesis generation with context caching
        hypotheses = []
        for group in query_results:
            group_name = group["group_name"]
            context = f"""{group_name}: {group['explanation']}
            Tickers Analyzed:
            {self.datafeed._format_tickers(group['associated_tickers'])}   
            """
            # Initialize conversation cache for this group
            self.conversation_cache[group_name] = [
                {"role": "user", "content": plan_prompt},
                {"role": "assistant", "content": plan_result}
            ]
            
            # Refine hypotheses through stages
            group_hypotheses = self._refine_hypotheses(
                group["queries"], context, constraints, phase="preliminary", group_name=group_name
            )
            group_hypotheses = self._refine_hypotheses(
                group_hypotheses, context, constraints, phase="validated", group_name=group_name
            )
            group_hypotheses = self._refine_hypotheses(
                group_hypotheses, context, constraints, phase="final", group_name=group_name
            )
            hypotheses.extend(group_hypotheses)
        return self._format_output(hypotheses)

    def _refine_hypotheses(self, input_data: Any, context: str, 
                         constraints: str, phase: str, group_name: str) -> List[Dict]:
        """Refine hypotheses through preliminary, validated, and final stages.

        Args:
            input_data: Query results (for preliminary) or hypotheses (for validated/final).
            context: Group name and explanation for context.
            constraints: Constraints for hypothesis generation.
            phase: Stage of refinement ('preliminary', 'validated', 'final').
            group_name: Group identifier for conversation cache.

        Returns:
            List of hypothesis dictionaries.
        """
        phase_prompts = {
            "preliminary": """
            Based on {context} and constraints: {constraints}
            Identify 5-10 raw patterns from:
            {data_samples}
            Return JSON:
            [
                {{
                    "pattern": "DESCRIPTION",
                    "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                    "strength": "HIGH|MEDIUM|LOW"
                }}
            ]
            """,
            "validated": """
            Based on {context} and constraints: {constraints}
            Validate these patterns:
            {patterns}
            Reject statistically weak ones (p>0.05). Keep 3-5 strongest.
            Return JSON:
            [
                {{
                    "pattern": "DESCRIPTION",
                    "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                    "strength": "HIGH|MEDIUM"
                }}
            ]
            """,
            "final": """
            Based on {context} and constraints: {constraints}
            Convert these patterns into investment hypotheses:
            {patterns}
            The backtesting data schema is following -
            {data_schema}
            Return JSON:
            [
                {{
                    "hypothesis": "ACTIONABLE STATEMENT",
                    "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                    "trigger": "CONDITION",
                    "timeframe": "PERIOD",
                    "confidence": 0.0-1.0,
                    "risks": "FACTORS",
                    "strategy": {{
                        "indicators": [{{"name": "INDICATOR_NAME", "params": {{...}}}}],
                        "buy_conditions": ["CONDITION"],
                        "sell_conditions": ["CONDITION"],
                        "holding_period": DAYS_NUMBER,
                        "data_feed": {{
                            "MEASUREMENT": ["FIELD1", "FIELD2"],
                        }}
                    }}
                }}
            ]
            Ensure the strategy includes python Backtrader-compatible strategy and the data are from data schema I've provided. 
            The data_feed must specify measurements (e.g., market_info, financial_ratios, macro_data) and fields (e.g., Close, 
            Volume, roe, CPI Inflation) from the InfluxDB schema, matching the strategy's requirements.
            """
        }
        
        data_samples = "\n".join(
            q["result"] for q in input_data if isinstance(input_data, list) and "result" in q
        ) if phase == "preliminary" else json.dumps([
            {
                "pattern": h["thesis"],
                "assets": h["assets"],
                "strength": h.get("strength", "MEDIUM")
            } for h in input_data
        ], indent=2)
        
        prompt = phase_prompts[phase].format(
            context=context,
            data_schema = self.datafeed.schema,
            constraints=constraints,
            data_samples=data_samples,
            patterns=data_samples
        )
        
        response = self._llm_call(prompt, group_name)
        return self._parse_hypotheses(response, phase)

    def _llm_call(self, prompt: str, group_name: str) -> str:
        """Call LLM with context caching.

        Args:
            prompt: Prompt string for LLM.
            group_name: Group identifier for conversation cache.

        Returns:
            LLM response text.
        """
        try:
            # Initialize conversation cache for group if not exists
            if group_name not in self.conversation_cache:
                self.conversation_cache[group_name] = []
            
            # Append new user message to conversation
            self.conversation_cache[group_name].append({"role": "user", "content": prompt})
            
            response = self.client.chat.completions.create(
                model=self.llm_model,
                messages=self.conversation_cache[group_name]
            )
            
            # Append assistant response to conversation
            response_content = response.choices[0].message.content
            self.conversation_cache[group_name].append({"role": "assistant", "content": response_content})
            
            return response_content
        except Exception as e:
            return []

    def _parse_hypotheses(self, text: str, phase: str) -> List[Dict]:
        """Parse LLM JSON response into structured hypotheses.

        Args:
            text: Raw LLM response text (JSON).
            phase: Stage of refinement ('preliminary', 'validated', 'final').

        Returns:
            List of hypothesis dictionaries.
        """
        try:
            # Extract JSON block from text using regex
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
        except Exception as e:
            return []

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