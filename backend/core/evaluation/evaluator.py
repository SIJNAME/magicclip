from __future__ import annotations

import math
from typing import Iterable


def compute_mse_retention_prediction(actual: list[float], predicted: list[float]) -> float:
    if not actual or len(actual) != len(predicted):
        return 0.0
    return sum((a - p) ** 2 for a, p in zip(actual, predicted)) / len(actual)


def compute_ranking_correlation(actual: list[float], predicted: list[float]) -> float:
    if not actual or len(actual) != len(predicted):
        return 0.0

    def rank(values: list[float]) -> list[int]:
        indexed = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0] * len(values)
        for r, (idx, _) in enumerate(indexed):
            ranks[idx] = r + 1
        return ranks

    ra, rp = rank(actual), rank(predicted)
    n = len(actual)
    diff_sq = sum((a - p) ** 2 for a, p in zip(ra, rp))
    return 1 - (6 * diff_sq) / (n * (n * n - 1)) if n > 1 else 0.0


def evaluate_prediction_accuracy(actual: list[float], predicted: list[float]) -> dict[str, float]:
    mse = compute_mse_retention_prediction(actual, predicted)
    rmse = math.sqrt(mse)
    corr = compute_ranking_correlation(actual, predicted)
    return {"mse": mse, "rmse": rmse, "ranking_correlation": corr}


def compare_engine_versions(version_a_scores: Iterable[float], version_b_scores: Iterable[float]) -> dict[str, float]:
    a = list(version_a_scores)
    b = list(version_b_scores)
    if not a or not b:
        return {"mean_delta": 0.0}
    min_len = min(len(a), len(b))
    delta = [b[i] - a[i] for i in range(min_len)]
    return {"mean_delta": sum(delta) / len(delta)}
