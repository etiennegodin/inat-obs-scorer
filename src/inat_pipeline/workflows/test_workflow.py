import logging

import mlflow
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline

from .. import model
from ..app.container import Dependencies

logger = logging.getLogger(__name__)


def execute(deps: Dependencies) -> dict:
    # ── 1. Data ───────────────────────────────────────────────────────────────

    X_train, y_train, X_val, y_val, X_test, y_test = model.load_and_split(
        deps._DATA_FOLDER / "features.parquet"
    )

    # ── 2. Load model ─────────────────────────────────────────────────────────
    db_path = deps.project_root / "mlruns.db"
    mlflow.set_tracking_uri(f"sqlite:///{db_path}")

    model_uri = "models:/inat_scorer_lightgbm@latest"
    pipeline = mlflow.sklearn.load_model(model_uri)
    pipeline: Pipeline

    # ── 5. Evaluate on held-out test set ──────────────────────────────────
    y_pred = pipeline.predict(X_val)
    y_pred_proba = pipeline.predict_proba(X_val)[:, 1]

    test_metrics = {
        "test/test_roc_auc": roc_auc_score(y_val, y_pred_proba),
        "test/test_avg_precision": average_precision_score(y_val, y_pred_proba),
        "test/test_f1": f1_score(y_val, y_pred),
        "test/test_accuracy": accuracy_score(y_val, y_pred),
    }

    logger.info("\nTest set results:")
    for k, v in test_metrics.items():
        logger.info(f"  {k}: {v:.4f}")

    return {
        "test_metrics": test_metrics,
    }
