from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.evaluation.evaluator import evaluate_prediction_accuracy
from core.intelligence.retention_model import RetentionModel
from core.intelligence.weight_optimizer import optimize_weights
from core.project_service import list_clip_training_samples

logger = logging.getLogger(__name__)
LAST_RUN_FILE = Path(__file__).resolve().parents[2] / "storage" / "models" / "training_last_run.txt"


def _should_train_weekly(force: bool) -> bool:
    if force or not LAST_RUN_FILE.exists():
        return True
    last = datetime.fromisoformat(LAST_RUN_FILE.read_text().strip())
    return datetime.now(timezone.utc) - last >= timedelta(days=7)


def train_retention_model(force: bool = False) -> dict[str, float | str | None]:
    samples = list_clip_training_samples(limit=20000)
    if len(samples) <= 100 or not _should_train_weekly(force):
        return {"status": "skipped", "samples": len(samples)}

    started = time.time()
    model = RetentionModel()
    features = []
    targets = []
    for sample in samples:
        b = sample["breakdown"]
        features.append(
            {
                "llm_score": float(b.get("llm_score", sample.get("score", 0.0))),
                "emotion_score": float(b.get("emotion_score", 0.0)),
                "hook_score": float(b.get("hook_score", 0.0)),
                "topic_score": float(b.get("topic_score", 0.0)),
                "speech_rate_spike": float(b.get("speech_speed_spike", 0.0)),
                "curiosity_score": float(b.get("curiosity_score", 0.0)),
                "segment_length": float(sample.get("segment_length", 0.0)),
                "rhetorical_question_flag": float(b.get("rhetorical_question_flag", 0.0)),
            }
        )
        targets.append(float(sample["engagement_score"]) * 100.0)

    model_path = model.train(features, targets)
    preds = [model.predict(f, heuristic_fallback=50.0) for f in features]
    metrics = evaluate_prediction_accuracy(targets, preds)
    optimize_weights()

    LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN_FILE.write_text(datetime.now(timezone.utc).isoformat())

    logger.info(
        "retention_model_training",
        extra={
            "training_time": round(time.time() - started, 3),
            "prediction_accuracy": metrics,
            "model_path": str(model_path) if model_path else None,
            "samples": len(samples),
        },
    )
    return {
        "status": "trained",
        "model_path": str(model_path) if model_path else None,
        "samples": len(samples),
        **metrics,
    }


if __name__ == "__main__":
    print(train_retention_model(force=True))
