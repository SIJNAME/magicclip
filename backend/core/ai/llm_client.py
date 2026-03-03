from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from openai import OpenAI

from core.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class LLMUsage:
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: float


class LLMClient:
    def __init__(self, api_key: str | None = None, client: OpenAI | None = None) -> None:
        self.client = client or OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
        self.config = CONFIG.ai

    @staticmethod
    def _estimate_token_count(messages: list[dict[str, str]]) -> int:
        return int(sum(len(m.get("content", "").split()) for m in messages) * 1.3)

    @staticmethod
    def _cost_estimate(model: str, total_tokens: int) -> float:
        rate = 0.35 if "mini" in model else 1.2
        return round((total_tokens / 1_000_000) * rate, 6)

    def _split_messages(self, messages: list[dict[str, str]]) -> list[list[dict[str, str]]]:
        max_tokens = self.config.max_input_tokens
        if self._estimate_token_count(messages) <= max_tokens:
            return [messages]

        user_message = messages[-1]["content"]
        chunks = []
        current = ""
        for line in user_message.splitlines():
            candidate = f"{current}\n{line}" if current else line
            probe = messages[:-1] + [{"role": "user", "content": candidate}]
            if current and self._estimate_token_count(probe) > max_tokens:
                chunks.append(messages[:-1] + [{"role": "user", "content": current}])
                current = line
            else:
                current = candidate
        if current:
            chunks.append(messages[:-1] + [{"role": "user", "content": current}])
        return chunks or [messages]

    def chat_json(self, messages: list[dict[str, str]], temperature: float = 0.2) -> tuple[dict[str, Any], list[LLMUsage]]:
        model_chain = (self.config.primary_model, *self.config.fallback_models)
        payload_parts = self._split_messages(messages)
        merged: dict[str, Any] = {}
        usages: list[LLMUsage] = []

        for part in payload_parts:
            last_exc: Exception | None = None
            for model in model_chain:
                try:
                    response = self.client.chat.completions.create(
                        model=model,
                        messages=part,
                        temperature=temperature,
                        max_tokens=self.config.max_output_tokens,
                        response_format={"type": "json_object"},
                    )
                    parsed = json.loads(response.choices[0].message.content)
                    usage = response.usage
                    total_tokens = getattr(usage, "total_tokens", 0) if usage else 0
                    usages.append(
                        LLMUsage(
                            model=model,
                            prompt_tokens=getattr(usage, "prompt_tokens", 0) if usage else 0,
                            completion_tokens=getattr(usage, "completion_tokens", 0) if usage else 0,
                            total_tokens=total_tokens,
                            estimated_cost_usd=self._cost_estimate(model, total_tokens),
                        )
                    )
                    for key, value in parsed.items():
                        if isinstance(value, list):
                            merged.setdefault(key, []).extend(value)
                        else:
                            merged[key] = value
                    break
                except Exception as exc:  # fallback path
                    last_exc = exc
                    continue
            else:
                raise RuntimeError(f"LLM request failed for all models: {last_exc}")

        logger.info(
            "llm_request_complete",
            extra={
                "token_usage": sum(item.total_tokens for item in usages),
                "ai_cost_estimate": sum(item.estimated_cost_usd for item in usages),
                "models": [item.model for item in usages],
            },
        )
        return merged, usages
