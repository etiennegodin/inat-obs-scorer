import logging
from dataclasses import asdict, dataclass, field

import pandas as pd

from ..exceptions import IncompatiblePipelineModules

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    # ── Data ──────────────────────────────────────────────────────────────────
    # data_path:          str = "data/observations.csv"
    target_column: str = "label"  # what we're predicting
    random_seed: int = 42  # controls ALL randomness
    version: str = "na"

    # ── Column groups ─────────────────────────────────────────────────────────
    # List columns explicitly so the pipeline is transparent about what it uses.
    features: list = field(default_factory=list)
    numeric_features: list = field(default_factory=list)
    categorical_features: list = field(default_factory=list)
    features_types: list = field(default_factory=lambda: ["numeric", "categorical"])

    # ── Module selection (keys into the registries above) ─────────────────────
    numeric_imputer: str = "median"
    categorical_imputer: str = "most_frequent"
    encoder: str = "onehot"
    scaler: str = "standard"
    reducer: str = "none"
    classifier: str = "logistic"

    # ── Experiment tracking ───────────────────────────────────────────────────
    experiment_name: str = "inat_obs_scorer_v0_1"
    n_trials: int = 10  # Optuna: number of hyperparameter combos to try
    cv_folds: int = 5  # cross-validation folds per trial
    scoring_metric: str = "roc_auc"  # what Optuna optimizes for
    run_name: str = field(init=False)  # Run name

    # Flags
    ct_verbose_feature_names_out: bool = False
    use_gpu: bool = False  # for lightgbm
    gap_days: int = 90
    n_jobs: int = -1

    def __post_init__(self):
        if self.use_gpu and self.classifier != "lightgbm":
            raise IncompatiblePipelineModules(
                "'use_gpu' is only supported with lightgbm classifier"
            )

        self.run_name = f"{self.classifier}_optuna"

    def set_features(self, df: pd.DataFrame) -> None:
        try:
            df.pop(self.target_column)
            logger.debug(f"Removed {self.target_column}")
        except Exception as e:
            logger.debug(e)

        self.features = df.columns.to_list()
        self.categorical_features = df.select_dtypes(include="object").columns.to_list()
        self.numeric_features = df.select_dtypes(include="number").columns.to_list()

    def set_git_hash(self, git_hash: str):
        self.git_hash = git_hash

    def change_feature_type(self, *features) -> None:
        for feature_name in features:
            if feature_name in self.features:
                try:
                    self.categorical_features.append(feature_name)
                    self.numeric_features.remove(feature_name)
                    logger.info(f"Changed '{feature_name}' to categorical type")
                except ValueError:
                    self.numeric_features.append(feature_name)
                    self.categorical_features.remove(feature_name)
                    logger.info(f"Changed '{feature_name}' to numerical type")
            else:
                logger.warning(f"{feature_name} not found in features list")

    def to_dict(self) -> dict:
        """Serialize config for logging to MLflow."""
        return asdict(self)
