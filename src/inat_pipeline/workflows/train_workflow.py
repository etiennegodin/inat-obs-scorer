import json
import logging
import warnings
from pathlib import Path

import mlflow
import optuna

from ..pipeline import model

logger = logging.getLogger(__name__)

# Suppress noisy warnings during hyperparameter search
warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)


def execute(
    db_path: Path,
    classifier: str,
    reducer: str,
    scaler: str,
    encoder: str,
    imputer: str,
    n_trials: int,
    cv_folds: int,
):
    # Initialise pipeline configs
    config = model.PipelineConfig(
        classifier=classifier,
        numeric_imputer=imputer,
        reducer=reducer,
        scaler=scaler,
        encoder=encoder,
        n_trials=n_trials,
        cv_folds=cv_folds,
    )
    # ── 1. Data ───────────────────────────────────────────────────────────────

    X_train, y_train, X_val, y_val, X_test, y_test, data_stats = model.load_and_split(
        db_path, config
    )

    # ── 2. MLflow setup ───────────────────────────────────────────────────────

    mlflow.set_experiment(config.experiment_name)

    with mlflow.start_run(run_name=f"{config.classifier}_optuna") as parent_run:
        parent_run_id = parent_run.info.run_id
        logger.info(f"\n{'=' * 60}")
        logger.info(f"MLflow Run ID: {parent_run_id}")
        logger.info(f"Classifier:    {config.classifier}")
        logger.info(f"Trials:        {config.n_trials}")
        logger.info(f"CV Folds:      {config.cv_folds}")
        logger.info(f"{'=' * 60}\n")

        # Log full config so this run is 100% reproducible
        mlflow.log_params(config.to_dict())

        # Log data statistics
        mlflow.log_metrics(data_stats)

        # Log pipeline structure as a JSON artifact
        sample_pipeline = model.build_pipeline(config)
        pipeline_desc = model.utils.describe_pipeline(sample_pipeline)
        with open("pipeline_description.json", "w") as f:
            json.dump(pipeline_desc, f, indent=2, default=str)
        mlflow.log_artifact("pipeline_description.json")

        # ── 3. Optuna hyperparameter search ───────────────────────────────────
        logger.info(f"Starting Optuna search ({config.n_trials} trials)...\n")
        study = optuna.create_study(
            direction="maximize",  # we want the highest ROC-AUC
            sampler=optuna.samplers.TPESampler(seed=config.random_seed),
            pruner=optuna.pruners.MedianPruner(n_warmup_steps=10),
            # TPE = Tree-structured Parzen Estimator — smarter than random search
            # MedianPruner cuts off trials that are clearly underperforming early
        )

        objective = model.objective.make_objective(
            config, X_train, y_train, parent_run_id
        )

        study.optimize(
            objective,
            n_trials=config.n_trials,
            show_progress_bar=False,
        )

        best_params = study.best_params
        best_cv_score = study.best_value

        logger.info(
            f"\n✓ Optuna finished. Best CV {config.scoring_metric}: {best_cv_score:.4f}"
        )
        logger.info(f"  Best params: {best_params}\n")

        # Log best params and best CV score to the parent run
        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})
        mlflow.log_metric(f"best_cv_{config.scoring_metric}", best_cv_score)

        # ── 4. Final model training ────────────────────────────────────────────
        logger.info("Training final model on full training set...")
        final_model = model.train_final_model(config, best_params, X_train, y_train)

        # ── 5. Evaluate on held-out test set ──────────────────────────────────
        from sklearn.metrics import (
            accuracy_score,
            average_precision_score,
            f1_score,
            roc_auc_score,
        )

        y_pred = final_model.predict(X_test)
        y_pred_proba = final_model.predict_proba(X_test)[:, 1]

        test_metrics = {
            "test_roc_auc": roc_auc_score(y_test, y_pred_proba),
            "test_avg_precision": average_precision_score(y_test, y_pred_proba),
            "test_f1": f1_score(y_test, y_pred),
            "test_accuracy": accuracy_score(y_test, y_pred),
        }

        mlflow.log_metrics(test_metrics)

        print("\nTest set results:")
        for k, v in test_metrics.items():
            print(f"  {k}: {v:.4f}")
