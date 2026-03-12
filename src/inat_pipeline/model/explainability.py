"""
explainability.py
─────────────────
Generates and logs feature importance visualizations to MLflow.
Called at the end of experiment.py after the final model is trained.

Three levels of explainability:
  1. Native importance  — fast, built into tree models (not model-agnostic)
  2. PCA loadings       — which input features drive each component
  3. SHAP values        — model-agnostic, shows per-prediction contribution
                          (pip install shap)
"""

import logging

import matplotlib
import mlflow
import numpy as np
import pandas as pd

matplotlib.use("Agg")  # non-interactive backend — safe for logging, no window pops up
import matplotlib.pyplot as plt
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# from .core import get_transformer
from .config import PipelineConfig

logger = logging.getLogger(__name__)


def log_feature_importance(pipeline: Pipeline, config: PipelineConfig, top_n: int = 20):
    """
    Logs a bar chart of feature importances for tree-based classifiers.
    Works for: RandomForest, GradientBoosting, XGBoost, LightGBM.
    Skipped silently for models that don't expose feature_importances_.

    mlflow.log_figure() — no temp file, no cleanup needed.
    The figure goes straight into MLflow's artifact store.
    """
    classifier = pipeline.named_steps["classifier"]

    if not hasattr(classifier, "feature_importances_"):
        logger.info(
            f"{type(classifier).__name__} has no feature_importances_ — skipping"
        )
        return

    preprocessor = pipeline.named_steps["preprocessor"]
    preprocessor: ColumnTransformer
    feature_names = preprocessor.get_feature_names_out()

    # If a reducer (PCA) is present, feature names are lost —
    # we log the PCA loadings separately instead (see below)
    if "reducer" in pipeline.named_steps:
        logger.info(
            "Reducer present — native importance not meaningful, "
            "use SHAP or PCA loadings"
        )
        return

    importances = classifier.feature_importances_
    indices = np.argsort(importances)[::-1][:top_n]
    fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.35)))
    ax.barh(
        range(len(indices)), importances[indices], color="steelblue", edgecolor="white"
    )
    ax.set_yticks(range(len(indices)))
    ax.set_yticklabels([feature_names[i] for i in indices], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Importance (mean decrease in impurity)")
    ax.set_title(f"Top {top_n} Feature Importances — {config.classifier}")
    fig.tight_layout()

    # ← key call: no file written to disk, goes straight to MLflow
    mlflow.log_figure(fig, "feature_importance.png")
    plt.close(fig)
    logger.info("Logged feature_importance.png")


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────


def create_explainability_report(
    pipeline: Pipeline, X_train: pd.DataFrame, config: PipelineConfig
):
    """
    Call this inside an active MLflow run after training.
    Logs whichever plots are applicable given the pipeline config.
    """
    print("\nLogging explainability artifacts...")
    log_feature_importance(pipeline, config)
    # log_pca_loadings(pipeline, config)
    # log_shap_summary(pipeline, X_train, config)
