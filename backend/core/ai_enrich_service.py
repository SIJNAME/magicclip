import json
import os
from typing import Dict, List

from dotenv import load_dotenv
from openai import BadRequestError, OpenAI

from core.clip_service import generate_clip_suggestions

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


def _group_words_to_segments(words: List[dict], max_words_per_segment: int = 40) -> List[dict]:
    segments: List[dict] = []
    if not words:
        return segments

    current_words: List[dict] = []
    segment_id = 1

    for word in words:
        if word.get("type") != "word":
            continue
        current_words.append(word)

        text_word = str(word.get("word", ""))
        has_sentence_end = any(ch in text_word for ch in [".", "!", "?"])
        if len(current_words) >= max_words_per_segment or has_sentence_end:
            segments.append(
                {
                    "segment_id": segment_id,
                    "start": current_words[0]["startTime"],
                    "end": current_words[-1]["endTime"],
                    "text": " ".join(w["word"] for w in current_words),
                }
            )
            segment_id += 1
            current_words = []

    if current_words:
        segments.append(
            {
                "segment_id": segment_id,
                "start": current_words[0]["startTime"],
                "end": current_words[-1]["endTime"],
                "text": " ".join(w["word"] for w in current_words),
            }
        )
    return segments


def compute_overlap(clip_a: dict, clip_b: dict) -> float:
    try:
        start_a = float(clip_a.get("start"))
        end_a = float(clip_a.get("end"))
        start_b = float(clip_b.get("start"))
        end_b = float(clip_b.get("end"))
    except (TypeError, ValueError):
        return 0.0

    duration_a = max(0.0, end_a - start_a)
    duration_b = max(0.0, end_b - start_b)
    smaller_duration = min(duration_a, duration_b)
    if smaller_duration <= 0:
        return 0.0

    intersection = max(0.0, min(end_a, end_b) - max(start_a, start_b))
    return intersection / smaller_duration


def get_dynamic_cap(total_duration: float) -> int:
    if total_duration < 600:
        return 8
    if total_duration <= 1800:
        return 15
    if total_duration <= 5400:
        return 20
    return 30


def filter_and_rank_clips(raw_clips: List[dict], total_duration: float) -> List[dict]:
    if not raw_clips:
        return []

    valid_clips: List[dict] = []
    for clip in raw_clips:
        if not isinstance(clip, dict):
            continue
        try:
            start = float(clip.get("start"))
            end = float(clip.get("end"))
            score = int(clip.get("score"))
            title = clip.get("title")
            summary = clip.get("summary")
        except (TypeError, ValueError):
            continue

        if end <= start:
            continue
        if not isinstance(title, str) or not isinstance(summary, str):
            continue
        if score < 70:
            continue

        valid_clips.append(
            {
                "start": start,
                "end": end,
                "score": score,
                "title": title,
                "summary": summary,
            }
        )

    valid_clips.sort(key=lambda c: c["score"], reverse=True)

    kept: List[dict] = []
    for clip in valid_clips:
        overlaps_existing = any(compute_overlap(clip, existing) > 0.6 for existing in kept)
        if not overlaps_existing:
            kept.append(clip)

    cap = get_dynamic_cap(max(0.0, float(total_duration)))
    return kept[:cap]


def select_clips(words: List[dict]) -> List[dict]:
    segments = _group_words_to_segments(words)
    if not segments:
        return []

    raw_clips = generate_clip_suggestions(segments)
    if not words:
        total_duration = 0.0
    else:
        try:
            total_duration = float(words[-1]["endTime"]) - float(words[0]["startTime"])
        except (KeyError, TypeError, ValueError):
            total_duration = 0.0

    final_clips = filter_and_rank_clips(raw_clips, total_duration)
    return final_clips
