from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

try:
    from sklearn.linear_model import LinearRegression
except Exception:  # pragma: no cover
    LinearRegression = None

from core.config import CONFIG, persist_scoring_weights
from core.project_service import list_clip_training_samples
from db import get_connection

logger = logging.getLogger(__name__)

FEATURE_MAP = {
    "llm_score": "llm_score",
    "emotion_score": "emotion_score",
    "hook_score": "hook_score",
    "topic_transition_weight": "topic_transition_weight",
    "speech_speed_spike": "speech_speed_spike",
    "curiosity_score": "curiosity_score",
}


def _clamp_and_normalize(weights: dict[str, float]) -> dict[str, float]:
    capped = {k: min(max(v, 0.01), 0.5) for k, v in weights.items()}
    total = sum(capped.values()) or 1.0
    normalized = {k: v / total for k, v in capped.items()}
    # second pass to enforce <=0.5 after normalization
    for _ in range(3):
        over = {k: v for k, v in normalized.items() if v > 0.5}
        if not over:
            break
        excess = sum(v - 0.5 for v in over.values())
        for k in over:
            normalized[k] = 0.5
        under_keys = [k for k in normalized if k not in over]
        under_total = sum(normalized[k] for k in under_keys) or 1.0
        for k in under_keys:
            normalized[k] += excess * (normalized[k] / under_total)
    total = sum(normalized.values()) or 1.0
    return {k: v / total for k, v in normalized.items()}


def optimize_weights(max_samples: int = 5000, learning_rate: float = 0.2) -> dict[str, float] | None:
    samples = list_clip_training_samples(limit=max_samples)
    if len(samples) < 30 or LinearRegression is None:
        return None

    x_rows = []
    y_vals = []
    for s in samples:
        b = s["breakdown"]
        x_rows.append([float(b.get(col, 0.0)) for col in FEATURE_MAP.values()])
        y_vals.append(float(s["engagement_score"]) * 100.0)

    reg = LinearRegression()
    reg.fit(x_rows, y_vals)
    importance = [abs(v) for v in reg.coef_]
    importance_sum = sum(importance)
    if importance_sum == 0:
        return None

    target = {k: importance[i] / importance_sum for i, k in enumerate(FEATURE_MAP.keys())}
    current = asdict(CONFIG.scoring)
    blended = {
        k: float(current[k]) * (1 - learning_rate) + float(target[k]) * learning_rate
        for k in FEATURE_MAP
    }
    optimized = _clamp_and_normalize(blended)

    CONFIG.scoring.llm_score = optimized["llm_score"]
    CONFIG.scoring.emotion_score = optimized["emotion_score"]
    CONFIG.scoring.hook_score = optimized["hook_score"]
    CONFIG.scoring.topic_transition_weight = optimized["topic_transition_weight"]
    CONFIG.scoring.speech_speed_spike = optimized["speech_speed_spike"]
    CONFIG.scoring.curiosity_score = optimized["curiosity_score"]
    persist_scoring_weights(CONFIG.scoring)

    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO scoring_weight_history (id, weights_json, mse, created_at) VALUES (?, ?, ?, ?)",
            (str(uuid4()), json.dumps(optimized), None, now),
        )
        conn.commit()

    logger.info("weight_adjustments", extra={"weights": optimized, "learning_rate": learning_rate})
    return optimized
