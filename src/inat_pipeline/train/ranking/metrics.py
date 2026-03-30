"""
Ranking metrics for the iNaturalist RG scorer.

These metrics evaluate the model as a triage/prioritisation tool:
given a finite review budget (K% of observations), how well does
the score help experts allocate their time?

Metrics
-------
Recall@K    : fraction of all true RG observations captured in the top K%
Precision@K : fraction of the top K% that are actually RG
Lift@K      : Precision@K / overall RG rate  (1.0 = random baseline)
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd


def compute_ranking_curves(
    y_true: np.ndarray | Sequence,
    y_score: np.ndarray | Sequence,
    k_values: np.ndarray | None = None,
) -> pd.DataFrame:
    if k_values is None:
        k_values = np.linspace(0.01, 1.0, 100)

    y_true = np.asarray(y_true, dtype=int)
    y_score = np.asarray(y_score, dtype=float)

    if y_true.shape != y_score.shape:
        raise ValueError("y_true and y_score must have the same length.")

    total = len(y_true)
    total_pos = int(y_true.sum())

    if total_pos == 0:
        raise ValueError("y_true contains no positive labels.")

    baseline_rate = total_pos / total

    # Sort observations by descending score once; reuse for all K values.
    order = np.argsort(y_score)[::-1]
    y_sorted = y_true[order]

    # Cumulative positives at each position
    cumulative_tp = np.cumsum(y_sorted)

    rows = []
    for k in k_values:
        n_top = max(1, int(np.ceil(k * total)))
        tp = int(cumulative_tp[n_top - 1])

        precision = tp / n_top
        recall = tp / total_pos
        lift = precision / baseline_rate

        rows.append(
            {
                "k": k,
                "n_reviewed": n_top,
                "recall_at_k": recall,
                "precision_at_k": precision,
                "lift_at_k": lift,
                "baseline_precision": baseline_rate,
                "random_recall": k,
            }
        )

    return pd.DataFrame(rows)


def ranking_summary(
    y_true: np.ndarray | Sequence,
    y_score: np.ndarray | Sequence,
    k_values: Sequence[float] = (0.05, 0.10, 0.20, 0.30, 0.50),
) -> pd.DataFrame:
    """
    Human-readable summary table at a small set of K values.

    Useful for README tables and model cards.

    Example output
    --------------
      K%   n_reviewed  recall_at_k  precision_at_k  lift_at_k
      5%          600        0.188           0.626       2.81
     10%         1200        0.338           0.563       2.52
     20%         2400        0.562           0.468       2.10
     30%         3600        0.726           0.403       1.81
     50%         6000        0.893           0.298       1.34
    """
    curves = compute_ranking_curves(y_true, y_score, np.asarray(k_values))

    summary = curves[
        ["k", "n_reviewed", "recall_at_k", "precision_at_k", "lift_at_k"]
    ].copy()
    summary.insert(
        0, "K%", (summary.pop("k") * 100).round(1).astype(float).astype(str) + "%"
    )

    return summary.reset_index(drop=True)


def area_under_recall_curve(
    y_true: np.ndarray | Sequence,
    y_score: np.ndarray | Sequence,
) -> float:
    """
    Scalar summary: area under the Recall@K curve, normalised to [0, 1].

    A perfect ranker scores 1.0; random baseline scores 0.5.
    Equivalent to ROC-AUC — included here for completeness and to make
    the connection between ranking and classification metrics explicit.
    """
    curves = compute_ranking_curves(y_true, y_score)
    return float(np.trapz(curves["recall_at_k"], curves["k"]))
