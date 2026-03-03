from __future__ import annotations

from typing import Any


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def predict_retention(segment: dict[str, Any], score_breakdown: dict[str, float]) -> dict[str, float]:
    text = segment.get("text", "")
    duration = max(0.1, float(segment.get("end", 0)) - float(segment.get("start", 0)))
    words = max(1, len(text.split()))
    speech_rate = words / duration
    rhetorical = 1.0 if "?" in text else 0.0
    emotional_density = score_breakdown.get("emotion_score", 0) / 100
    topic_shift = score_breakdown.get("topic_transition_weight", 0) / 100
    speed_signal = min(1.0, abs(speech_rate - 2.7) / 2.7)

    predicted_watch_time = duration * (0.45 + emotional_density * 0.25 + rhetorical * 0.15 + topic_shift * 0.15)
    dropoff_risk = _clamp(100 - (emotional_density * 35 + rhetorical * 20 + topic_shift * 20 + (1 - speed_signal) * 25))
    rewatch_probability = _clamp((emotional_density * 45 + rhetorical * 30 + topic_shift * 25) * 100 / 100)
    retention_score = _clamp((predicted_watch_time / duration) * 100)

    return {
        "retention_score": round(retention_score, 2),
        "dropoff_risk": round(dropoff_risk, 2),
        "rewatch_score": round(rewatch_probability, 2),
    }
