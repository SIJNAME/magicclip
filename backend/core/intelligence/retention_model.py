from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Any

try:
    from sklearn.ensemble import GradientBoostingRegressor
except Exception:  # pragma: no cover
    GradientBoostingRegressor = None

logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parents[2] / "storage" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_KEYS = [
    "llm_score",
    "emotion_score",
    "hook_score",
    "topic_score",
    "speech_rate_spike",
    "curiosity_score",
    "segment_length",
    "rhetorical_question_flag",
]


class RetentionModel:
    def __init__(self, model_path: Path | None = None) -> None:
        self.model_path = model_path or self._latest_model_path()
        self.model: Any | None = None
        if self.model_path and self.model_path.exists():
            self.load(self.model_path)

    @staticmethod
    def _latest_model_path() -> Path | None:
        candidates = sorted(MODEL_DIR.glob("retention_model_v*.pkl"))
        return candidates[-1] if candidates else None

    @staticmethod
    def _to_matrix(rows: list[dict[str, float]]) -> list[list[float]]:
        return [[float(row.get(k, 0.0)) for k in FEATURE_KEYS] for row in rows]

    def train(self, features: list[dict[str, float]], targets: list[float], version: int | None = None) -> Path | None:
        if GradientBoostingRegressor is None:
            logger.warning("retention_model_training_skipped", extra={"reason": "scikit-learn unavailable"})
            return None
        if not features or len(features) != len(targets):
            raise ValueError("Invalid training dataset")

        x = self._to_matrix(features)
        y = [float(v) for v in targets]
        model = GradientBoostingRegressor(random_state=42)
        model.fit(x, y)
        self.model = model

        if version is None:
            latest = self._latest_model_path()
            version = (int(latest.stem.split("_v")[-1]) + 1) if latest else 1
        out_path = MODEL_DIR / f"retention_model_v{version}.pkl"
        with out_path.open("wb") as handle:
            pickle.dump(model, handle)
        self.model_path = out_path
        return out_path

    def load(self, model_path: Path | None = None) -> bool:
        path = model_path or self.model_path
        if not path or not path.exists():
            return False
        with path.open("rb") as handle:
            self.model = pickle.load(handle)
        self.model_path = path
        return True

    def predict(self, feature_row: dict[str, float], heuristic_fallback: float) -> float:
        if self.model is None and self.model_path:
            self.load(self.model_path)
        if self.model is None:
            return float(max(0.0, min(100.0, heuristic_fallback)))
        x = self._to_matrix([feature_row])
        pred = float(self.model.predict(x)[0])
        return float(max(0.0, min(100.0, pred)))
