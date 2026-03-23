"""
Plots for ranking / triage evaluation metrics.

Produces a three-panel figure (Recall@K, Precision@K, Lift@K) plus an
optional cumulative gain curve.  All functions accept the DataFrame
returned by ``ranking_metrics.compute_ranking_curves``.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import mlflow
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

# ── palette ──────────────────────────────────────────────────────────────
MODEL_COLOR = "#2563EB"  # blue
BASELINE_COLOR = "#9CA3AF"  # gray
FILL_ALPHA = 0.10
HIGHLIGHT_COLOR = "#F59E0B"  # amber — dot + label at specific K values
GRID_ALPHA = 0.25


def _annotate_k(ax, k_pct: float, y: float, fmt: str, offset_x: float = 3.5):
    """Drop a dot + text annotation at a specific K value."""
    ax.scatter([k_pct], [y], color=HIGHLIGHT_COLOR, s=45, zorder=6, clip_on=False)
    ax.annotate(
        fmt.format(y),
        xy=(k_pct, y),
        xytext=(k_pct + offset_x, y),
        fontsize=7.5,
        color=HIGHLIGHT_COLOR,
        va="center",
        fontweight="bold",
    )


def plot_ranking_curves(
    curves: pd.DataFrame,
    title: str = "Ranking / triage performance",
    highlight_k: list[float] = (0.10, 0.20, 0.30),
    figsize: tuple[float, float] = (15, 4.8),
) -> Figure:
    """
    Three-panel figure: Recall@K · Precision@K · Lift@K.

    Parameters
    ----------
    curves       : DataFrame from ``compute_ranking_curves``.
    title        : Figure suptitle.
    highlight_k  : List of K fractions at which to annotate the curves.
    figsize      : Matplotlib figure size.

    Returns
    -------
    matplotlib Figure (call .savefig() or .show() on the result).
    """
    fig, axes = plt.subplots(1, 3, figsize=figsize)
    fig.subplots_adjust(top=0.88, bottom=0.18, left=0.07, right=0.97, wspace=0.38)

    k_pct = curves["k"] * 100
    rg_rate = curves["baseline_precision"].iloc[0]

    panels = [
        dict(
            ax=axes[0],
            y=curves["recall_at_k"] * 100,
            baseline=k_pct,  # random recall@K = K itself
            ylabel="Recall@K (%)",
            title="Recall@K",
            subtitle=(
                "Of all observations that became RG,\n"
                "% captured by reviewing the top K%"
            ),
            baseline_label="Random (diagonal)",
            fmt="{:.1f}%",
            ylim=(0, 105),
        ),
        dict(
            ax=axes[1],
            y=curves["precision_at_k"] * 100,
            baseline=np.full(len(k_pct), rg_rate * 100),
            ylabel="Precision@K (%)",
            title="Precision@K",
            subtitle=("Of the top K% reviewed,\n" "% that actually became RG"),
            baseline_label=f"Random ({rg_rate*100:.1f}% overall RG rate)",
            fmt="{:.1f}%",
            ylim=(0, 105),
        ),
        dict(
            ax=axes[2],
            y=curves["lift_at_k"],
            baseline=np.ones(len(k_pct)),
            ylabel="Lift@K",
            title="Lift@K",
            subtitle=(
                "How many × more RG per review slot\n"
                "vs random sampling (1.0 = random)"
            ),
            baseline_label="Random (1.0)",
            fmt="{:.2f}×",
            ylim=(0, None),
        ),
    ]

    for panel in panels:
        ax: plt.Axes = panel["ax"]
        y = np.asarray(panel["y"])
        baseline = np.asarray(panel["baseline"])

        ax.fill_between(k_pct, baseline, y, alpha=FILL_ALPHA, color=MODEL_COLOR)
        ax.plot(
            k_pct,
            baseline,
            color=BASELINE_COLOR,
            lw=1.5,
            ls="--",
            label=panel["baseline_label"],
        )
        ax.plot(k_pct, y, color=MODEL_COLOR, lw=2, label="Model")

        # Annotate at requested K values
        for hk in highlight_k:
            hk_pct = hk * 100
            idx = (curves["k"] - hk).abs().idxmin()
            _annotate_k(ax, k_pct.iloc[idx], float(y[idx]), panel["fmt"])
            ax.axvline(hk_pct, color=HIGHLIGHT_COLOR, lw=0.8, ls=":", alpha=0.5)

        ax.set_xlabel("K — top % of observations reviewed", fontsize=9)
        ax.set_ylabel(panel["ylabel"], fontsize=9)
        ax.set_title(panel["title"], fontsize=11, fontweight="bold", pad=6)
        ax.text(
            0.5,
            -0.22,
            panel["subtitle"],
            ha="center",
            va="top",
            transform=ax.transAxes,
            fontsize=8,
            color="#6B7280",
            style="italic",
        )
        ax.set_xlim(0, 100)
        if panel["ylim"][1] is not None:
            ax.set_ylim(*panel["ylim"])
        ax.grid(True, alpha=GRID_ALPHA, lw=0.5)
        ax.spines[["top", "right"]].set_visible(False)
        ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
        ax.legend(
            fontsize=7.5,
            loc="lower right" if panel["title"] != "Lift@K" else "upper right",
        )

    fig.suptitle(title, fontsize=13, fontweight="bold")

    mlflow.log_figure(fig, title)
    plt.close(fig)
    return fig


def log_plot_score_distribution(y_score: pd.DataFrame):
    fig, ax = plt.subplots(3, 3, figsize=(5, 5))

    ax.hist(y_score[y_score == 1], bins=50, alpha=0.6, label="True RG")
    ax.hist(y_score[y_score == 0], bins=50, alpha=0.6, label="Not RG")
    ax.xlabel("Predicted score")
    mlflow.log_figure(fig, "score_distribution.png")
    plt.close(fig)


def log_ranking(
    curves: pd.DataFrame,
    model_name: str = "LightGBM scorer",
    highlight_k: list[float] = (0.10, 0.20, 0.30),
) -> None:
    """
    Convenience wrapper: renders and optionally saves both figures.
    """
    rg_rate = curves["baseline_precision"].iloc[0]
    suffix = f"  ·  overall RG rate {rg_rate*100:.1f}%"

    title = f"{model_name} — ranking metrics{suffix}"
    plot_ranking_curves(
        curves,
        title=title,
        highlight_k=highlight_k,
    )
