from typing import Any, Dict, Optional, Type
import json, re
import pandas as pd
from neuralwealth.ai_lab.hypothesis.resources.llm_refinement_prompts import LLM_REFINEMENT_PROMPT
from neuralwealth.ai_lab.utils.llm_client import LLMClient

class HypothesisRefiner:
    """Refines hypotheses using an LLM client with a dedicated prompt."""

    def __init__(self, llm_client: Type[LLMClient]):
        """
        Initialize the HypothesisRefiner with an LLM client.

        Args:
            llm_client: LLM client with a 'call' method that accepts a prompt and returns a JSON response.
        """
        self.llm_client = llm_client

    def refine(
            self, 
            hypothesis: Dict, 
            backtest_results: Dict[str, Dict[str, Any]], 
            crash_results: Dict[str, Dict[str, Any]], 
            criteria: Dict[str, float]
        ) -> Optional[Dict]:
        """
        Refine a hypothesis using the LLM client.

        Args:
            hypothesis: Original hypothesis dictionary.
            backtest_results: Dictionary of {ticker: results_dict} from backtesting.
            crash_results: Dictionary of {crash_name: results_dict} from crash testing.
            criteria: Performance criteria for refinement constraints.

        Returns:
            Optional[Dict]: Refined hypothesis or None if refinement fails.
        """
        # Prepare context for refinement
        context = {
            "hypothesis": hypothesis,
            "backtest_results": {
                ticker: {
                    "sharpe_ratio": results.get("sharpe", {}).get("sharperatio", 0.0) or 0.0,
                    "max_drawdown": results.get("drawdown", {}).get("maxdrawdown", 0.0) or 0.0,
                    "total_return": results.get("returns", {}).get("rtot", 0.0) or 0.0,
                    "num_trades": results.get("trade_analyzer", {}).get("total", {}).get("total", 0) or 0,
                    "win_rate": (
                        results.get("trade_analyzer", {})
                        .get("won", {})
                        .get("total", 0) / max(1, results.get("trade_analyzer", {}).get("total", {}).get("total", 1))
                    ) or 0.0
                }
                for ticker, results in backtest_results.items()
                if "error" not in results
            },
            "crash_results": {
                crash_name: {
                    ticker: {
                        "crash_drawdown": ticker_results.get("drawdown", {}).get("maxdrawdown", 0.0) or 0.0,
                        "crash_return": ticker_results.get("returns", {}).get("rtot", 0.0) or 0.0
                    }
                    for ticker, ticker_results in results.items()
                    if isinstance(ticker_results, dict) and "error" not in ticker_results
                }
                for crash_name, results in crash_results.items()
                if isinstance(results, dict) and "error" not in results
            }
        }

        # Format prompt with criteria and context
        prompt = LLM_REFINEMENT_PROMPT.format(
            min_sharpe=criteria["min_sharpe"],
            max_drawdown=criteria["max_drawdown"],
            min_total_return=criteria["min_total_return"],
            max_crash_drawdown=criteria["max_crash_drawdown"],
            min_crash_return=criteria["min_crash_return"],
            hypothesis=json.dumps(hypothesis, indent=2),
            backtest_results=json.dumps(context["backtest_results"], indent=2),
            crash_results=json.dumps(context["crash_results"], indent=2)
        )

        # Call LLM client
        try:
            response = self.llm_client.call(prompt, "refinement")
            refined_hypothesis = json.loads(re.search(r"\{.*\}", response, re.DOTALL).group())
            if refined_hypothesis:
                # Ensure required fields are updated
                refined_hypothesis["id"] = f"{hypothesis.get('id', 'unknown')}_refined"
                refined_hypothesis["thesis"] = f"Refined: {hypothesis.get('thesis', '')}"
                refined_hypothesis["trigger"] = f"Refined: {hypothesis.get('trigger', '')}"
                refined_hypothesis["confidence"] = max(0.0, hypothesis.get("confidence", 0.9) - 0.1)
                refined_hypothesis["last_updated"] = pd.Timestamp.now().isoformat()
                return refined_hypothesis
        except Exception as e:
            print(f"Refinement failed: {str(e)}")
            return None

        return None