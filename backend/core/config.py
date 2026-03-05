from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

CONFIG_STORE = Path(__file__).resolve().parents[1] / "storage" / "scoring_weights.json"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class ScoringWeights:
    llm_score: float = field(default_factory=lambda: _env_float("MC_WEIGHT_LLM", 0.35))
    emotion_score: float = field(default_factory=lambda: _env_float("MC_WEIGHT_EMOTION", 0.20))
    hook_score: float = field(default_factory=lambda: _env_float("MC_WEIGHT_HOOK", 0.15))
    topic_transition_weight: float = field(default_factory=lambda: _env_float("MC_WEIGHT_TOPIC", 0.15))
    speech_speed_spike: float = field(default_factory=lambda: _env_float("MC_WEIGHT_SPEED", 0.10))
    curiosity_score: float = field(default_factory=lambda: _env_float("MC_WEIGHT_CURIOSITY", 0.05))


@dataclass
class AIEngineConfig:
    primary_model: str = os.getenv("MC_PRIMARY_MODEL", "gpt-4o-mini")
    fallback_models: tuple[str, ...] = tuple(filter(None, os.getenv("MC_FALLBACK_MODELS", "gpt-4.1-mini").split(",")))
    max_input_tokens: int = _env_int("MC_MAX_INPUT_TOKENS", 3000)
    max_output_tokens: int = _env_int("MC_MAX_OUTPUT_TOKENS", 3000)
    segmentation_batch_size: int = _env_int("MC_SEGMENT_BATCH", 24)
    clip_batch_size: int = _env_int("MC_CLIP_BATCH", 20)
    similarity_threshold: float = _env_float("MC_SIMILARITY_THRESHOLD", 0.80)
    min_clip_score: int = _env_int("MC_MIN_CLIP_SCORE", 70)


@dataclass
class AppConfig:
    scoring: ScoringWeights = field(default_factory=ScoringWeights)
    ai: AIEngineConfig = field(default_factory=AIEngineConfig)


def _load_persisted_weights() -> dict[str, float]:
    if not CONFIG_STORE.exists():
        return {}
    try:
        return json.loads(CONFIG_STORE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def persist_scoring_weights(weights: ScoringWeights) -> None:
    CONFIG_STORE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_STORE.write_text(json.dumps(asdict(weights), indent=2), encoding="utf-8")


CONFIG = AppConfig()
_persisted = _load_persisted_weights()
for _k, _v in _persisted.items():
    if hasattr(CONFIG.scoring, _k):
        setattr(CONFIG.scoring, _k, float(_v))
