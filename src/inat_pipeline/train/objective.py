import logging
import time
import warnings

import mlflow
import numpy as np
import optuna
import pandas as pd
from sklearn.metrics import auc, precision_recall_curve, roc_auc_score

from .config import PipelineConfig
from .core import ExpandingWindowCvSplit, build_pipeline
from .registery import LIGHTGBM_GPU_PARAMS, SEARCH_SPACES

# Suppress noisy warnings during hyperparameter search
warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)

logger = logging.getLogger(__name__)

# ── OPTUNA OBJECTIVE FUNCTION ──────────────────────────────────────────────────


def make_objective(
    config: PipelineConfig,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    parent_run_id: str,
):
    """
    Factory that returns the function Optuna will call for each trial.

    WHY A FACTORY (function that returns a function)?
      Optuna's objective must accept only one argument: `trial`.
      We need X_train, y_train, config, etc. inside it.
      The factory "closes over" those variables — they're baked in.

    WHAT HAPPENS EACH TRIAL:
      1. Optuna's `trial` object suggests hyperparameter values
      2. We build a fresh pipeline with those values
      3. Cross-validation scores the pipeline on training data
         (no test data touched here — that would be data leakage)
      4. Score is logged to MLflow as a child run
      5. Score is returned to Optuna to inform the next trial

    CROSS-VALIDATION EXPLAINED:
      StratifiedKFold splits train into K folds, preserving class balance.
      We train on K-1 folds and validate on the held-out fold, K times.
      The mean score across folds is a more reliable estimate than a single split.

         Fold 1: [VAL] [TRN] [TRN] [TRN] [TRN]  → score_1
         Fold 2: [TRN] [VAL] [TRN] [TRN] [TRN]  → score_2
         ...
         Final:  mean(score_1, ..., score_5)
    """
    search_space = SEARCH_SPACES.get(config.classifier, {})

    custom_cv = ExpandingWindowCvSplit(n_folds=config.cv_folds)

    # Set base params
    base_params = {"n_jobs": config.n_jobs}

    # Inject gpu params if flagged
    if config.use_gpu:
        base_params.update(LIGHTGBM_GPU_PARAMS)

    def objective(trial: optuna.Trial) -> float:
        # ── Step 1: Ask Optuna for hyperparameter suggestions ─────────────────
        # trial.suggest_* methods implement Bayesian optimization:
        # early trials explore randomly; later trials focus on promising regions.

        try:
            logger.debug("Optuna objective")
            # Copy base params
            trial_params = base_params
            trial_params["random_state"] = config.random_seed

            for param_name, spec in search_space.items():
                suggest_type = spec["type"]
                kwargs = {k: v for k, v in spec.items() if k != "type"}

                # Strip "classifier__" prefix — we pass raw params to the classifier
                short_name = param_name.replace("classifier__", "")

                if suggest_type == "int":
                    trial_params[short_name] = trial.suggest_int(param_name, **kwargs)
                elif suggest_type == "float":
                    trial_params[short_name] = trial.suggest_float(param_name, **kwargs)
                elif suggest_type == "categorical":
                    trial_params[short_name] = trial.suggest_categorical(
                        param_name, **kwargs
                    )

            # ── Step 2: Build pipeline with suggested params ──────────────────────
            pipeline = build_pipeline(config, classifier_params=trial_params)

            logger.debug(pipeline.named_steps["classifier"])

            # ── Step 3: Cross-validate ────────────────────────────────────────────

            start = time.time()
            logger.debug("Start cv")

            roc_aucs = []
            pr_aucs = []

            for fold_idx, (train_idx, val_idx) in enumerate(custom_cv.split(X_train)):
                X_train_fold = X_train.iloc[train_idx]
                y_train_fold = y_train.iloc[train_idx]

                X_val_fold = X_train.iloc[val_idx]
                y_val_fold = y_train.iloc[val_idx]

                pipeline.fit(X_train_fold, y_train_fold)

                y_pred = pipeline.predict_proba(X_val_fold)[:, 1]

                # --- Metrics ---
                roc_auc = roc_auc_score(y_val_fold, y_pred)
                precision, recall, thresholds = precision_recall_curve(
                    y_val_fold, y_pred
                )
                pr_auc = auc(recall, precision)

                roc_aucs.append(roc_auc)
                pr_aucs.append(pr_auc)

                logger.debug(f"Fold {fold_idx +1} / {config.cv_folds} done ")

            elapsed = time.time() - start
            logger.debug(f"End cv {elapsed}")

            roc_auc_mean = float(np.mean(roc_aucs))
            roc_auc_std = float(np.std(roc_aucs))

            pr_auc_mean = float(np.mean(pr_aucs))
            pr_auc_std = float(np.std(pr_aucs))

            logger.debug(f"ROC-AUC: {roc_auc_mean}")
            logger.debug(f"PR-AUC: {pr_auc_mean}")

            # ── Step 4: Log this trial as a child MLflow run ──────────────────────
            with mlflow.start_run(
                run_name=f"trial_{trial.number:04d}",
                nested=True,  # child of the parent run
            ):
                # Log every suggested hyperparameter
                mlflow.log_params({f"trial_{k}": v for k, v in trial_params.items()})
                mlflow.log_metrics(
                    {
                        "cv_roc_auc_mean": roc_auc_mean,
                        "cv_roc_auc_std": roc_auc_std,
                        "cv_pr_auc_mean": pr_auc_mean,
                        "cv_pr_auc_std": pr_auc_std,
                        "cv_duration_seconds": elapsed,
                    }
                )
            print(
                f"  Trial {trial.number:3d}: roc_auc={roc_auc_mean:.4f}"
                f" ±{roc_auc_std:.4f} | pr_auc = {pr_auc_mean:.4f}"
                f" ±{pr_auc_std:.4f} [{elapsed:.1f}s]"
            )

            return roc_auc_mean  # Optuna maximizes this
        except Exception as e:
            logger.error(f"Error on trial #{trial.number}: {e}")
            raise

    return objective
