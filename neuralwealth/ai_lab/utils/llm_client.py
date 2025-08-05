from typing import Dict, List
from openai import OpenAI

class LLMClient:
    """Client for interacting with LLM, managing conversation cache."""

    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        """Initialize LLM client.

        Args:
            base_url: OpenAI SDK base URL.
            api_key: OpenAI SDK API key.
            model: OpenAI LLM model.
        """
        self.client = OpenAI(base_url=base_url, api_key=api_key)
        self.model = model
        self.conversation_cache: Dict[str, List[Dict]] = {}

    def call(self, prompt: str, group_name: str) -> str:
        """Call LLM with context caching.

        Args:
            prompt: Prompt string for LLM.
            group_name: Group identifier for conversation cache.

        Returns:
            LLM response text.
        """
        try:
            if group_name not in self.conversation_cache:
                self.conversation_cache[group_name] = []

            self.conversation_cache[group_name].append({"role": "user", "content": prompt})

            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.conversation_cache[group_name]
            )

            response_content = response.choices[0].message.content
            self.conversation_cache[group_name].append({"role": "assistant", "content": response_content})

            return response_content
        except Exception:
            return []