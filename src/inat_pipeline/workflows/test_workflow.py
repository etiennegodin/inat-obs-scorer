import json
import logging
from pathlib import Path

import mlflow
from mlflow.models.signature import infer_signature
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
)

from .. import train
from ..app.container import Dependencies
from ..train import explainability, metrics, ranking

logger = logging.getLogger(__name__)

mlflow.set_tracking_uri("sqlite:///mlruns.db")  # single file, easy to inspect


def execute(
    deps: Dependencies,
) -> dict:

    with open(deps.package_root / "test" / "pipeline_config.json", "r") as f:
        config_dict = json.load(f)

    with open(deps.package_root / "test" / "final_model_params.json", "r") as f:
        final_model_params = json.load(f)

    config = train.PipelineConfig(**config_dict)

    config.run_name = config.run_name + "_final"

    # ── 1. Data & Config setup ─────────────────────────────────────────────────

    X_train, y_train, X_val, y_val, X_test, y_test = train.load_and_split(
        deps._DATA_FOLDER / "features_test.parquet"
    )

    # ── 2. MLflow setup ───────────────────────────────────────────────────────

    mlflow.end_run()  # force-close any dangling run

    mlflow.set_experiment(config.experiment_name)

    with mlflow.start_run(run_name=config.run_name) as parent_run:
        parent_run_id = parent_run.info.run_id

        # Log pipeline structure as a JSON artifact
        final_pipeline = train.build_pipeline(
            config, classifier_params=final_model_params
        )
        pipeline_desc = train.helpers.describe_pipeline(final_pipeline)
        with open("pipeline_description.json", "w") as f:
            json.dump(pipeline_desc, f, indent=2, default=str)
        mlflow.log_artifact("pipeline_description.json")
        Path("pipeline_description.json").unlink()

        final_pipeline.fit(X_train, y_train)

        # ── 5. Evaluate on held-out test set ──────────────────────────────────
        y_pred_proba = final_pipeline.predict_proba(X_val)[:, 1]

        # Log plots
        metrics.log_pr_auc_fig(final_pipeline, X_val, y_val)

        test_metrics = {
            "test/test_roc_auc": roc_auc_score(y_val, y_pred_proba),
            "test/test_avg_precision": average_precision_score(y_val, y_pred_proba),
        }

        mlflow.log_metrics(test_metrics)

        logger.info("\nTest set results:")
        for k, v in test_metrics.items():
            logger.info(f"  {k}: {v:.4f}")

        # Ranking metrics
        try:
            rank_curv = ranking.compute_ranking_curves(y_val, y_pred_proba)
            rank_sum = ranking.ranking_summary(
                y_val,
                y_pred_proba,
                k_values=[0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5],
            )
            # to-do log all ranking to compare
            mlflow.log_metric("lift_at_k5_per", rank_sum["lift_at_k"].to_list()[-1])
            ranking.plots.log_ranking_plot(
                rank_curv, highlight_k=[0.01, 0.05, 0.1, 0.2, 0.5]
            )
            ranking.plots.log_score_distribution_plot(y_pred_proba, y_val)
            mlflow.log_table(data=rank_sum, artifact_file="ranking.json")

        except Exception as e:
            logger.error(e)

        # ── 6. Features explainability ─────────────────────────────────────────────
        # Saves features explainability artifacts to mlflow
        explainability.log_feature_importance_report(final_pipeline, X_val, config)

        # ── 7. Log the final model ─────────────────────────────────────────────
        # This saves the *entire pipeline* (preprocessor + reducer + classifier)
        # as a single artifact. Load it anywhere with:
        #   pipeline = mlflow.sklearn.load_model("runs:/<run_id>/model")
        #   predictions = pipeline.predict(new_dataframe)

        # Cast to concrete dtypes so MLflow can read them
        X_sample = X_train.head(5).copy()
        for col in config.numeric_features:
            X_sample[col] = X_sample[col].astype(float)
        for col in config.categorical_features:
            X_sample[col] = X_sample[col].astype(str)

        signature = infer_signature(X_sample, final_pipeline.predict(X_sample))
        mlflow.sklearn.log_model(
            sk_model=final_pipeline,
            name="model",
            signature=signature,
            input_example=X_train.head(3),
            registered_model_name=f"inat_scorer_{config.classifier}",
        )

        logger.info(f"\n✓ Model logged. Run ID: {parent_run_id}")
        logger.info(
            f"  To load: mlflow.sklearn.load_model('runs:/{parent_run_id}/model')"
        )
        logger.info("  View UI: mlflow ui  (then open http://localhost:5000)\n")

        mlflow.log_artifact(deps.log_path)
