import json
import os
from typing import Dict, List

from dotenv import load_dotenv
from openai import BadRequestError, OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
BATCH_SIZE = 350
MIN_BATCH_SIZE = 50


def _build_prompt(compact_words: List[dict]) -> str:
    return f"""
Analyze the word list and:

- Mark emotionally strong or important words
- Add a suitable emoji if it enhances engagement
- Do NOT change timing
- Return JSON only

Input:
{json.dumps(compact_words, ensure_ascii=False)}

Output format:
{{
  "items": [
    {{
      "id": number,
      "important": true/false,
      "emoji": "🔥" or null
    }}
  ]
}}
"""


def _enrich_batch(compact_words: List[dict]) -> List[dict]:
    prompt = _build_prompt(compact_words)
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
    except BadRequestError:
        if len(compact_words) <= MIN_BATCH_SIZE:
            raise
        mid = len(compact_words) // 2
        return _enrich_batch(compact_words[:mid]) + _enrich_batch(compact_words[mid:])

    content = response.choices[0].message.content
    parsed = json.loads(content)
    items = parsed.get("items", [])
    if isinstance(items, list):
        return items
    return []


def enrich_words_with_ai(words):
    if not words:
        return words

    compact = [{"id": w["id"], "word": w["word"]} for w in words]

    enriched_all: List[dict] = []
    for start in range(0, len(compact), BATCH_SIZE):
        batch = compact[start : start + BATCH_SIZE]
        enriched_all.extend(_enrich_batch(batch))

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


def generate_clips(segments):
    return enrich_words_with_ai(segments)
