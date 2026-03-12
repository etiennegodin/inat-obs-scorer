import logging

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import seaborn as sns

matplotlib.use("Agg")  # non-interactive backend — safe for logging, no window pops up

logger = logging.getLogger(__name__)


def log_feature_corr(features: pd.DataFrame):
    features.shape[1]
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
