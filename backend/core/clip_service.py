from __future__ import annotations

import json
import logging
import time
from typing import Any

from backend.core.ai.llm_client import LLMClient
from backend.core.ai.prompt_registry import PROMPTS, serialize_segments
from backend.core.config import CONFIG
from backend.core.intelligence.deduplication import filter_semantic_duplicates
from backend.core.intelligence.retention import predict_retention
from backend.core.intelligence.retention_model import RetentionModel
from backend.core.intelligence.scoring import build_retention_features, combine_with_retention_model, compute_clip_score
from backend.core.intelligence.segmentation import semantic_segment

logger = logging.getLogger(__name__)
llm_client: LLMClient | None = None
retention_model = RetentionModel()


def _get_llm_client() -> LLMClient:
    global llm_client
    if llm_client is None:
        llm_client = LLMClient()
    return llm_client


def generate_clip_suggestions(transcript_segments: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prompt = PROMPTS.get("clip_candidates", "v1")
    messages = prompt.render({"segments_json": serialize_segments(transcript_segments)})
    parsed, usage = _get_llm_client().chat_json(messages, temperature=0.35)
    return parsed.get("clips", []), {
        "token_usage": sum(u.total_tokens for u in usage),
        "ai_cost_estimate": round(sum(u.estimated_cost_usd for u in usage), 6),
    }


def _find_segment_for_clip(clip: dict[str, Any], segments: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    target = None
    prev = None
    for idx, segment in enumerate(segments):
        if float(segment["start"]) <= float(clip["start"]) <= float(segment["end"]):
            target = segment
            prev = segments[idx - 1] if idx > 0 else None
            break
    return target, prev


def get_dynamic_cap(total_duration: float) -> int:
    if total_duration < 600:
        return 8
    if total_duration <= 1800:
        return 15
    if total_duration <= 5400:
        return 20
    return 30


def select_clips(words: list[dict]) -> list[dict]:
    started = time.time()
    segments = semantic_segment(words)
    if not segments:
        return []

    raw_clips, llm_observability = generate_clip_suggestions(segments)
    total_duration = max(0.0, float(words[-1].get("endTime", 0.0)) - float(words[0].get("startTime", 0.0))) if words else 0.0

    scored: list[dict[str, Any]] = []
    min_score = CONFIG.ai.min_clip_score
    for clip in raw_clips:
        if not isinstance(clip, dict):
            continue
        try:
            clip["start"] = float(clip.get("start"))
            clip["end"] = float(clip.get("end"))
        except (TypeError, ValueError):
            continue
        if clip["end"] <= clip["start"]:
            continue

        segment, prev_segment = _find_segment_for_clip(clip, segments)
        if not segment:
            continue

        hybrid_score, scoring_breakdown = compute_clip_score(clip, segment, prev_segment)
        retention = predict_retention(segment, scoring_breakdown)

        predicted_retention = retention_model.predict(
            build_retention_features(clip, segment, scoring_breakdown),
            heuristic_fallback=retention["retention_score"],
        )
        scoring_breakdown["predicted_retention"] = predicted_retention

        if retention_model.model is not None:
            score = combine_with_retention_model(
                hybrid_score=hybrid_score,
                predicted_retention=predicted_retention,
                rewatch_score=retention["rewatch_score"],
                hook_score=scoring_breakdown["hook_score"],
            )
        else:
            score = hybrid_score

        if score < min_score:
            continue
        scored.append(
            {
                "start": clip["start"],
                "end": clip["end"],
                "score": score,
                "title": str(clip.get("title", "Untitled clip")),
                "summary": str(clip.get("summary", "")),
                "scoring_breakdown": scoring_breakdown,
                "retention": retention,
            }
        )

    deduped = filter_semantic_duplicates(scored)
    deduped.sort(key=lambda c: c["score"], reverse=True)
    final_clips = deduped[: get_dynamic_cap(total_duration)]

    logger.info(
        "clip_selection_complete",
        extra={
            "clip_generation_time": round(time.time() - started, 3),
            "token_usage": llm_observability["token_usage"],
            "AI_cost_estimate": llm_observability["ai_cost_estimate"],
            "selected_clips": len(final_clips),
            "scoring_breakdown": json.dumps([c.get("scoring_breakdown", {}) for c in final_clips]),
        },
    )
    return final_clips
