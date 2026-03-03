from __future__ import annotations

from typing import Dict, List

from core.ai.llm_client import LLMClient
from core.ai.prompt_registry import PROMPTS
from core.clip_service import select_clips

BATCH_SIZE = 350


def enrich_words_with_ai(words: list[dict], llm_client: LLMClient | None = None):
    if not words:
        return words

    compact = [{"id": w["id"], "word": w["word"]} for w in words]
    client = llm_client or LLMClient()
    prompt = PROMPTS.get("word_enrichment", "v1")

    enriched_all: List[dict] = []
    for start in range(0, len(compact), BATCH_SIZE):
        batch = compact[start : start + BATCH_SIZE]
        messages = prompt.render({"words_json": str(batch)})
        parsed, _ = client.chat_json(messages, temperature=0.2)
        items = parsed.get("items", [])
        if isinstance(items, list):
            enriched_all.extend(items)

    by_id: Dict[int, dict] = {}
    for item in enriched_all:
        item_id = item.get("id")
        if isinstance(item_id, int):
            by_id[item_id] = item

    for word in words:
        result = by_id.get(word["id"])
        if not result:
            continue
        word["important"] = bool(result.get("important", word.get("important", False)))
        word["emoji"] = result.get("emoji", word.get("emoji"))

    return words


__all__ = ["enrich_words_with_ai", "select_clips"]
