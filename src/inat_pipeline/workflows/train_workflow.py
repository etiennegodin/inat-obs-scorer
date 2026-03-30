import json
import logging
import warnings
from pathlib import Path

import mlflow
import mlflow.models
import optuna
from mlflow.models.signature import infer_signature
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    classification_report,
    f1_score,
    roc_auc_score,
)

from .. import train
from ..app.container import Dependencies
from ..train import explainability, metrics, ranking

logger = logging.getLogger(__name__)

# Suppress noisy warnings during hyperparameter search
warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)

mlflow.set_tracking_uri("sqlite:///mlruns.db")  # single file, easy to inspect


def execute(
    deps: Dependencies,
    classifier: str,
    reducer: str,
    scaler: str,
    encoder: str,
    imputer: str,
    n_trials: int,
    cv_folds: int,
    random_seed: int,
    use_gpu: bool,
    n_jobs: int,
    stopping_rounds: int,
) -> dict:
    # Initialise pipeline configs
    config = train.PipelineConfig(
        classifier=classifier,
        numeric_imputer=imputer,
        reducer=reducer,
        scaler=scaler,
        encoder=encoder,
        n_trials=n_trials,
        cv_folds=cv_folds,
        random_seed=random_seed,
        use_gpu=use_gpu,
        version=deps.version,
        n_jobs=n_jobs,
        stopping_rounds=stopping_rounds,
        experiment_name="inat_obs_scorer_v0_2.1",
    )

    # ── 1. Data & Config setup ─────────────────────────────────────────────────

    X_train, y_train, X_val, y_val, X_test, y_test = train.load_and_split(
        deps._DATA_FOLDER / "features.parquet"
    )

    # Store features from dataframe
    config.set_features(X_train)
    # Override features type
    config.change_feature_type("oauth_application_id")

    # Get features set stats
    features_stats = train.utils.get_features_stats(X_train, y_train, config)

    # Get features diff from previous run
    features_diff = train.utils.get_feature_diff(
        config,
    )

    # ── 2. MLflow setup ───────────────────────────────────────────────────────

    mlflow.end_run()  # force-close any dangling run

    mlflow.set_experiment(config.experiment_name)

    with mlflow.start_run(run_name=config.run_name) as parent_run:
        parent_run_id = parent_run.info.run_id
        logger.info(f"\n{'=' * 60}")
        logger.info(f"MLflow Run ID: {parent_run_id}")
        logger.info(f"Classifier:    {config.classifier}")
        logger.info(f"Trials:        {config.n_trials}")
        logger.info(f"CV Folds:      {config.cv_folds}")
        logger.info(f"{'=' * 60}\n")

        # Log full config so this run is 100% reproducible
        mlflow.log_params(config.to_dict())

        # Log features diff
        if features_diff is not None:
            mlflow.log_dict(features_diff, "features_diff.json")

        # Log data statistics
        mlflow.log_metrics(features_stats)

        # Log plot of feature correlation
        explainability.log_feature_corr(X_train)

        # Log pipeline structure as a JSON artifact
        sample_pipeline = train.build_pipeline(
            config, classifier_params={"n_jobs": config.n_jobs}
        )
        pipeline_desc = train.helpers.describe_pipeline(sample_pipeline)
        with open("pipeline_description.json", "w") as f:
            json.dump(pipeline_desc, f, indent=2, default=str)
        mlflow.log_artifact("pipeline_description.json")
        Path("pipeline_description.json").unlink()

        # ── 3. Optuna hyperparameter search ───────────────────────────────────
        logger.info(f"Starting Optuna search ({config.n_trials} trials)...\n")
        study = optuna.create_study(
            direction="maximize",  # we want the highest ROC-AUC
            sampler=optuna.samplers.TPESampler(seed=config.random_seed),
            pruner=optuna.pruners.MedianPruner(n_warmup_steps=10),
            # TPE = Tree-structured Parzen Estimator — smarter than random search
            # MedianPruner cuts off trials that are clearly underperforming early
        )

        objective = train.objective.make_objective(
            config, X_train, y_train, parent_run_id
        )

        study.optimize(
            objective,
            n_trials=config.n_trials,
            show_progress_bar=False,
        )

        explainability.log_hyperparam_importance(study, config)

        best_params = study.best_params
        best_pr_score = study.best_value

        logger.info(
            f"\n✓ Optuna finished. Best PR {config.scoring_metric}: {best_pr_score:.4f}"
        )
        logger.info(f"  Best params: {best_params}\n")

        # Log best params and best CV score to the parent run
        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})
        mlflow.log_dict(best_params, "best_params.json")
        mlflow.log_metric(f"cv/best_pr_{config.scoring_metric}", best_pr_score)

        # ── 4. Final model training ────────────────────────────────────────────
        logger.info("Training final model on full training set...")
        final_model = train.train_final_model(config, best_params, X_train, y_train)

        # ── 5. Evaluate on held-out test set ──────────────────────────────────
        y_pred = final_model.predict(X_val)
        y_pred_proba = final_model.predict_proba(X_val)[:, 1]

        # Log plots
        metrics.log_pr_auc_fig(final_model, X_val, y_val)

        test_metrics = {
            "test/test_roc_auc": roc_auc_score(y_val, y_pred_proba),
            "test/test_avg_precision": average_precision_score(y_val, y_pred_proba),
            "test/test_f1": f1_score(y_val, y_pred),
            "test/test_accuracy": accuracy_score(y_val, y_pred),
        }

        mlflow.log_metrics(test_metrics)

        logger.info("\nTest set results:")
        for k, v in test_metrics.items():
            logger.info(f"  {k}: {v:.4f}")

            # Save classification report as artifact
        report = classification_report(
            y_val, y_pred, target_names=["not_rg", "research_grade"]
        )
        with open("classification_report.txt", "w") as f:
            f.write(report)
        mlflow.log_artifact("classification_report.txt")
        Path("classification_report.txt").unlink()

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
        explainability.log_feature_importance_report(final_model, X_val, config)

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

        signature = infer_signature(X_sample, final_model.predict(X_sample))
        mlflow.sklearn.log_model(
            sk_model=final_model,
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

        return {
            "run_id": parent_run_id,
            "best_params": best_params,
            "best_pr_score": best_pr_score,
            "test_metrics": test_metrics,
            "model": final_model,
        }
