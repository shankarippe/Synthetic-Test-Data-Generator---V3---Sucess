"""
intelligence/llm_client.py
--------------------------
LangChain + Groq LLM client.
Single place to configure the model — swap to any provider here.
"""

from __future__ import annotations
import json
import os
import re
from typing import Union, Any
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage


class LLMClient:
    """
    Thin wrapper around LangChain ChatGroq.
    Provides structured JSON output with automatic retry on parse failure.
    """

    def __init__(self, api_key: str, model: str = "llama-3.3-70b-versatile"):
        self.llm: Any = ChatGroq(
            api_key=api_key,  # type: ignore
            model=model,
            temperature=0.1,        # Low temp = consistent, structured output
            max_tokens=4096,
        )
        self.model = model
        self.call_count = 0

    def ask(self, system_prompt: str, user_prompt: str) -> str:
        """Send a prompt, return raw text response."""
        self.call_count += 1
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response: Any = self.llm.invoke(messages)
        return str(response.content) if response.content else ""

    def ask_json(self, system_prompt: str, user_prompt: str, retries: int = 3) -> Union[dict, list]:
        """
        Send a prompt expecting JSON back.
        Automatically strips markdown fences and retries on parse failure.
        """
        full_system = (
            system_prompt
            + "\n\nCRITICAL: Respond ONLY with valid JSON. "
            "No explanation, no markdown fences, no extra text. "
            "Just the raw JSON object or array."
        )

        raw: str = ""
        for attempt in range(retries):
            raw = self.ask(full_system, user_prompt)

            # Strip markdown code fences if present
            cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()

            # Find first { or [ and last } or ]
            start = min(
                (cleaned.find("{") if "{" in cleaned else len(cleaned)),
                (cleaned.find("[") if "[" in cleaned else len(cleaned)),
            )
            end_brace = cleaned.rfind("}")
            end_bracket = cleaned.rfind("]")
            end = max(end_brace, end_bracket)

            if start <= end and start < len(cleaned):
                cleaned = cleaned[start:end + 1]

            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                if attempt < retries - 1:
                    user_prompt = (
                        user_prompt
                        + f"\n\nPrevious response was not valid JSON: {raw[:200]}"
                        + "\nRespond ONLY with valid JSON this time."
                    )

        raise ValueError(f"LLM failed to return valid JSON after {retries} attempts.\nLast response: {raw[:500]}")