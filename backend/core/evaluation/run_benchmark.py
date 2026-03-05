from __future__ import annotations

from core.evaluation.evaluator import evaluate_prediction_accuracy
from core.intelligence.retention_model import RetentionModel
from core.project_service import list_clip_training_samples


def main() -> None:
    samples = list_clip_training_samples(limit=10000)
    if not samples:
        print("No historical clip samples available.")
        return

    model = RetentionModel()
    actual: list[float] = []
    predicted: list[float] = []
    for sample in samples:
        breakdown = sample["breakdown"]
        feature_row = {
            "llm_score": breakdown.get("llm_score", sample.get("score", 0.0)),
            "emotion_score": breakdown.get("emotion_score", 0.0),
            "hook_score": breakdown.get("hook_score", 0.0),
            "topic_score": breakdown.get("topic_score", 0.0),
            "speech_rate_spike": breakdown.get("speech_speed_spike", 0.0),
            "curiosity_score": breakdown.get("curiosity_score", 0.0),
            "segment_length": sample.get("segment_length", 0.0),
            "rhetorical_question_flag": 1.0 if breakdown.get("rhetorical_question_flag", 0.0) else 0.0,
        }
        pred = model.predict(feature_row, heuristic_fallback=breakdown.get("hybrid_score", sample.get("score", 0.0)))
        predicted.append(pred)
        actual.append(float(sample["engagement_score"]) * 100.0)

    metrics = evaluate_prediction_accuracy(actual, predicted)
    print("Benchmark results")
    for key, value in metrics.items():
        print(f"- {key}: {value:.4f}")


if __name__ == "__main__":
    main()
