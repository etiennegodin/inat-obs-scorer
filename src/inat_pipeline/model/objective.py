import logging
import time
import warnings

import mlflow
import numpy as np
import optuna
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_val_score

from .config import PipelineConfig
from .core import build_pipeline
from .registery import SEARCH_SPACES

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
    cv = StratifiedKFold(
        n_splits=config.cv_folds,
        shuffle=True,
        random_state=config.random_seed,
    )

    def objective(trial: optuna.Trial) -> float:
        # ── Step 1: Ask Optuna for hyperparameter suggestions ─────────────────
        # trial.suggest_* methods implement Bayesian optimization:
        # early trials explore randomly; later trials focus on promising regions.

        logger.debug("Optuna objective")
        trial_params = {}
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

        logger.debug(pipeline.steps["classifier"])

        # ── Step 3: Cross-validate ────────────────────────────────────────────

        start = time.time()
        logger.debug(f"Start cv {start}")

        scores = cross_val_score(
            pipeline,
            X_train,
            y_train,
            cv=cv,
            scoring=config.scoring_metric,
            n_jobs=-1,  # use all CPU cores
            error_score="raise",
        )
        elapsed = time.time() - start
        logger.debug(f"End cv {elapsed}")

        mean_score = float(np.mean(scores))
        std_score = float(np.std(scores))

        logger.debug(f"Mean score {mean_score}")

        # ── Step 4: Log this trial as a child MLflow run ──────────────────────
        with mlflow.start_run(
            run_name=f"trial_{trial.number:04d}",
            nested=True,  # child of the parent run
        ):
            # Log every suggested hyperparameter
            mlflow.log_params({f"trial_{k}": v for k, v in trial_params.items()})
            mlflow.log_metrics(
                {
                    f"cv_{config.scoring_metric}_mean": mean_score,
                    f"cv_{config.scoring_metric}_std": std_score,
                    "cv_duration_seconds": elapsed,
                }
            )

        print(
            f"  Trial {trial.number:3d}: {config.scoring_metric}={mean_score:.4f} "
            f"±{std_score:.4f}  [{elapsed:.1f}s]"
        )

        return mean_score  # Optuna maximizes this

    return objective
