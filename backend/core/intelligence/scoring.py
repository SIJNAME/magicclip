from __future__ import annotations

from typing import Any

from core.config import CONFIG
from core.intelligence.retention_model import RetentionModel


MODEL = RetentionModel()


def _clamp_100(value: float) -> float:
    return max(0.0, min(100.0, value))


def _to_100(value: float) -> float:
    return _clamp_100(value * 100 if value <= 1 else value)


def speech_speed_spike(segment: dict[str, Any]) -> float:
    duration = max(0.1, float(segment["end"]) - float(segment["start"]))
    words = max(1, len(segment.get("text", "").split()))
    wps = words / duration
    return _clamp_100((wps - 2.0) * 18)


def topic_transition_weight(segment: dict[str, Any], prev_segment: dict[str, Any] | None) -> float:
    if not prev_segment:
        return 50.0
    curr_words = set(segment.get("text", "").lower().split())
    prev_words = set(prev_segment.get("text", "").lower().split())
    overlap = len(curr_words & prev_words) / max(1, len(curr_words | prev_words))
    return _clamp_100((1 - overlap) * 100)


def build_retention_features(clip: dict[str, Any], segment: dict[str, Any], breakdown: dict[str, float]) -> dict[str, float]:
    text = segment.get("text", "")
    segment_length = max(0.1, float(segment.get("end", 0.0)) - float(segment.get("start", 0.0)))
    return {
        "llm_score": breakdown["llm_score"],
        "emotion_score": breakdown["emotion_score"],
        "hook_score": breakdown["hook_score"],
        "topic_score": _to_100(float(segment.get("topic_score", 0))),
        "speech_rate_spike": breakdown["speech_speed_spike"],
        "curiosity_score": breakdown["curiosity_score"],
        "segment_length": segment_length,
        "rhetorical_question_flag": 1.0 if "?" in text else 0.0,
    }


def compute_clip_score(clip: dict[str, Any], segment: dict[str, Any], prev_segment: dict[str, Any] | None = None) -> tuple[int, dict[str, float]]:
    weights = CONFIG.scoring
    breakdown = {
        "llm_score": _to_100(float(clip.get("llm_score", clip.get("score", 0)))),
        "emotion_score": _to_100(float(segment.get("emotion_score", 0))),
        "hook_score": _to_100(float(segment.get("hook_score", 0))),
        "topic_transition_weight": topic_transition_weight(segment, prev_segment),
        "speech_speed_spike": speech_speed_spike(segment),
        "curiosity_score": _to_100(float(clip.get("curiosity_score", 50))),
    }
    hybrid_score = (
        weights.llm_score * breakdown["llm_score"]
        + weights.emotion_score * breakdown["emotion_score"]
        + weights.hook_score * breakdown["hook_score"]
        + weights.topic_transition_weight * breakdown["topic_transition_weight"]
        + weights.speech_speed_spike * breakdown["speech_speed_spike"]
        + weights.curiosity_score * breakdown["curiosity_score"]
    )

    breakdown["hybrid_score"] = _clamp_100(hybrid_score)
    return int(round(_clamp_100(hybrid_score))), breakdown


def combine_with_retention_model(
    hybrid_score: float,
    predicted_retention: float,
    rewatch_score: float,
    hook_score: float,
) -> int:
    score = (
        0.30 * _clamp_100(hybrid_score)
        + 0.40 * _clamp_100(predicted_retention)
        + 0.15 * _clamp_100(rewatch_score)
        + 0.15 * _clamp_100(hook_score)
    )
    return int(round(_clamp_100(score)))
