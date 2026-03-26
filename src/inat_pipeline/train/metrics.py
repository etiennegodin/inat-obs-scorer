import matplotlib.pyplot as plt
import mlflow
import pandas as pd
from sklearn.metrics import PrecisionRecallDisplay
from sklearn.pipeline import Pipeline


def log_pr_auc_fig(model: Pipeline, X_val: pd.DataFrame, y_val: pd.DataFrame):
    plt.figure()
    # Precision recall curve
    PrecisionRecallDisplay.from_estimator(
        model, X_val, y_val, plot_chance_level=True, response_method="predict_proba"
    )
    plt.title("Precision-Recall Curve")
    mlflow.log_figure(plt.gcf(), "pr_auc.png")
    plt.close()
