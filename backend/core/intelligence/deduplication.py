from __future__ import annotations

import math
import re
from typing import Any

from backend.core.config import CONFIG

TOKEN_RE = re.compile(r"[a-zA-Z0-9']+")


def _embed(text: str, dim: int = 128) -> list[float]:
    vec = [0.0] * dim
    for token in TOKEN_RE.findall(text.lower()):
        vec[hash(token) % dim] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def filter_semantic_duplicates(clips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    threshold = CONFIG.ai.similarity_threshold
    kept: list[dict[str, Any]] = []
    embeddings: list[list[float]] = []

    for clip in sorted(clips, key=lambda c: c.get("score", 0), reverse=True):
        text = f"{clip.get('title', '')} {clip.get('summary', '')}".strip()
        emb = _embed(text)
        is_dup = False
        for prior_clip, prior_emb in zip(kept, embeddings):
            sem_sim = cosine_similarity(emb, prior_emb)
            overlap = max(0.0, min(float(clip["end"]), float(prior_clip["end"])) - max(float(clip["start"]), float(prior_clip["start"])))
            duration = min(float(clip["end"]) - float(clip["start"]), float(prior_clip["end"]) - float(prior_clip["start"]))
            temporal_overlap = overlap / max(duration, 0.001)
            if sem_sim >= threshold or temporal_overlap > 0.7:
                is_dup = True
                break
        if not is_dup:
            kept.append(clip)
            embeddings.append(emb)
    return kept
