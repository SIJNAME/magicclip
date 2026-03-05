from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PromptTemplate:
    name: str
    version: str
    system: str
    user_template: str

    def render(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.user_template.format(**payload)},
        ]


class PromptRegistry:
    def __init__(self) -> None:
        self._registry: dict[tuple[str, str], PromptTemplate] = {}

    def register(self, prompt: PromptTemplate) -> None:
        self._registry[(prompt.name, prompt.version)] = prompt

    def get(self, name: str, version: str = "v1") -> PromptTemplate:
        key = (name, version)
        if key not in self._registry:
            raise KeyError(f"Prompt not found: {name}@{version}")
        return self._registry[key]


PROMPTS = PromptRegistry()

PROMPTS.register(
    PromptTemplate(
        name="clip_candidates",
        version="v1",
        system="You are a professional short-form video strategist.",
        user_template=(
            "Select high-quality short-form clip candidates from transcript segments. "
            "Return strict JSON with key 'clips' containing an array of objects with: "
            "s (float), e (float), llm_score (0-100), t (string), summary, curiosity_score (0-100).\n"
            "Generate maximum 5 clips only.\n"
            "Use 20-60 second ranges when possible.\n"
            "Transcript segments:\n{segments_json}\n"
            "IMPORTANT:\n"
            "If the output may exceed limits, shorten all strings.\n"
            "Ensure the final character is }."
        ),
    )
)

PROMPTS.register(
    PromptTemplate(
        name="word_enrichment",
        version="v1",
        system="You annotate transcript words for emotional emphasis.",
        user_template=(
            "Analyze the word list and mark emotionally strong or important words. "
            "Add emoji only when useful. Return JSON only with key 'items'.\n"
            "Input:\n{words_json}"
        ),
    )
)


def serialize_segments(segments: list[dict[str, Any]]) -> str:
    return json.dumps(segments, ensure_ascii=False)
