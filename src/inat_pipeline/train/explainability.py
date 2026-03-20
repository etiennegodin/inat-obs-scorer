import logging

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import optuna
import optuna.visualization
import pandas as pd
import seaborn as sns
import shap
from optuna.importance import get_param_importances

matplotlib.use("Agg")  # non-interactive backend — safe for logging, no window pops up
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# from .core import get_transformer
from .config import PipelineConfig

logger = logging.getLogger(__name__)


## Features correlation


def log_feature_corr(features: pd.DataFrame):
    corr = features.corr(numeric_only=True)
    upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(upper_tri, cmap="Blues", ax=ax)
    ax.set_title("Training feature correlation")
    fig.tight_layout()

    # ← key call: no file written to disk, goes straight to MLflow
    mlflow.log_figure(fig, "feature_correlation.png")
    plt.close(fig)
    logger.info("Logged feature_correlation.png")


## Features importance
def log_feature_importance(pipeline: Pipeline, config: PipelineConfig, top_n: int = 30):
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


# PCA interpretability
def log_pca_loadings(pipeline, config, top_n: int = 8, n_components: int = 8):
    """
    Heatmap of which input features drive each PCA component.
    Only runs when config.reducer != "none".

    Reading the heatmap:
      - Each column is a principal component (PC1, PC2, ...)
      - Each row is an original feature
      - Color intensity = how much that feature contributes to that component
      - Warm colors = positive loading, cool = negative
    """
    if config.reducer == "none" or "reducer" not in pipeline.named_steps:
        return

    preprocessor = pipeline.named_steps["preprocessor"]
    reducer = pipeline.named_steps["reducer"]
    feature_names = preprocessor.get_feature_names_out()
    components = reducer.components_[:n_components]  # (n_components, n_features)
    explained_var = reducer.explained_variance_ratio_[:n_components]

    # For each component, find the top_n features by absolute loading
    top_feature_idx = set()
    for comp in components:
        top_feature_idx.update(np.argsort(np.abs(comp))[::-1][:top_n])
    top_feature_idx = sorted(top_feature_idx)

    loadings_df = pd.DataFrame(
        components[:, top_feature_idx],
        index=[f"PC{i+1} ({v:.1%})" for i, v in enumerate(explained_var)],
        columns=[feature_names[i] for i in top_feature_idx],
    )
    fig, ax = plt.subplots(figsize=(max(12, top_n * 1.5), n_components * 0.9 + 2))
    sns.heatmap(
        loadings_df.values,
        ax=ax,
        cbar=True,
        cmap="RdBu_r",
        vmin=-1,
        vmax=1,
        linewidths=0.5,  # Add lines between cells
    )

    ax.set_xticks(range(len(loadings_df.columns)))
    ax.set_xticklabels(loadings_df.columns, rotation=40, ha="right", fontsize=8)
    ax.set_yticks(range(len(loadings_df.index)))
    ax.set_yticklabels(loadings_df.index, fontsize=9)
    ax.set_title(f"PCA Loadings — top {top_n} features per component")
    fig.tight_layout()

    mlflow.log_figure(fig, "pca_loadings.png")
    plt.close(fig)

    # Also log the raw numbers as a CSV for further analysis
    loadings_df.T.to_csv("/tmp/_pca_loadings.csv")
    mlflow.log_artifact("/tmp/_pca_loadings.csv", artifact_path="tables")
    logger.info("Logged pca_loadings.png + pca_loadings.csv")


# ── 3. SHAP VALUES (model-agnostic, best explainability) ──────────────────────


def log_shap_summary(
    pipeline, X_train: pd.DataFrame, config, max_rows: int = 200, top_n: int = 30
):
    """
    Logs a SHAP summary plot — the gold standard for model explainability.
    """
    logger.info("Computing SHAP values (this may take a minute)...")

    # Transform data through all steps except the classifier
    transformer = pipeline[:-1]
    classifier = pipeline.named_steps["classifier"]

    # SHAP on a sample — full dataset can be slow
    X_sample = X_train.sample(min(max_rows, len(X_train)), random_state=42)
    X_transformed = transformer.transform(X_sample)

    # Recover feature names (lost after PCA, available otherwise)
    if "reducer" not in pipeline.named_steps:
        feature_names = list(
            pipeline.named_steps["preprocessor"].get_feature_names_out()
        )
    else:
        n_components = X_transformed.shape[1]
        feature_names = [f"PC{i+1}" for i in range(n_components)]

    X_transformed_df = pd.DataFrame(X_transformed, columns=feature_names)

    # Choose the right SHAP explainer for the classifier type
    classifier_type = type(classifier).__name__
    tree_classifiers = {
        "RandomForestClassifier",
        "GradientBoostingClassifier",
        "XGBClassifier",
        "LGBMClassifier",
    }

    if classifier_type in tree_classifiers:
        explainer = shap.TreeExplainer(classifier)
    else:
        # Model-agnostic fallback — slower but universal
        explainer = shap.Explainer(classifier.predict_proba, X_transformed_df)

    shap_values = explainer(X_transformed_df)

    try:
        # Summary plot (beeswarm) — shows distribution of SHAP values per feature
        fig, ax = plt.subplots(figsize=(10, max(6, len(feature_names) * 0.4)))
        shap.plots.beeswarm(
            shap_values,
            max_display=top_n,
            group_remaining_features=True,
            ax=ax,
            plot_size=None,
        )
        plt.tight_layout()
        mlflow.log_figure(plt.gcf(), "shap_summary.png")
        plt.close("all")
    except Exception:
        pass

    # Also log mean absolute SHAP as a clean bar chart
    mean_shap = np.abs(
        shap_values.values[..., 1]
        if shap_values.values.ndim == 3
        else shap_values.values
    ).mean(axis=0)
    shap_df = (
        pd.DataFrame({"feature": feature_names, "mean_abs_shap": mean_shap})
        .sort_values("mean_abs_shap", ascending=True)
        .tail(top_n)
    )

    fig2, ax2 = plt.subplots(figsize=(9, max(5, len(shap_df) * 0.38)))
    ax2.barh(
        shap_df["feature"], shap_df["mean_abs_shap"], color="coral", edgecolor="white"
    )
    ax2.set_xlabel("Mean |SHAP value|")
    ax2.set_title(f"Feature Impact — {config.classifier}")
    fig2.tight_layout()
    mlflow.log_figure(fig2, "shap_bar.png")
    plt.close(fig2)

    logger.info("Logged shap_summary.png + shap_bar.png")


def log_feature_importance_report(
    pipeline: Pipeline, X_train: pd.DataFrame, config: PipelineConfig
) -> None:
    """
    Call this inside an active MLflow run after training.
    Logs whichever plots are applicable given the pipeline config.
    """
    try:
        print("\nLogging explainability artifacts...")
        log_feature_importance(pipeline, config)
        log_pca_loadings(pipeline, config)
        log_shap_summary(pipeline, X_train, config)
    except Exception as e:
        logger.error(f"Error creating explainability report: {e}")
        return


def log_hyperparam_importance(study: optuna.Study) -> None:
    importances = get_param_importances(study)
    logger.info("\nHyperparameter importances (fANOVA):")
    for param, score in importances.items():
        bar = "█" * int(score * 40)
        logger.info(f"  {param:<40} {bar} {score:.3f}")

    # Log to MLflow so it's saved with the run
    mlflow.log_dict(importances, "hyperparameter_importance.json")

    # Optional: matplotlib figure version
    ax = optuna.visualization.matplotlib.plot_param_importances(study)
    fig = ax.figure  # Access the figure object from the axes
    mlflow.log_figure(fig, "hyperparameter_importance.png")
    plt.close(fig)
