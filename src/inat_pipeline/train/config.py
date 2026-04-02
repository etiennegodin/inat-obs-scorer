import logging
from dataclasses import asdict, dataclass, field

import pandas as pd

from ..exceptions import IncompatiblePipelineModules, TrainPipelineConfigError

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
    passthrough_features: list = field(default_factory=list)
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
    stopping_rounds: int = 50
    scoring_metric: str = "average_precision"  # what Optuna optimizes for
    run_name: str = "inat_obs_scorer"
    # Flags
    ct_verbose_feature_names_out: bool = False
    use_gpu: bool = False  # for lightgbm
    n_jobs: int = -1

    def __post_init__(self):
        if self.use_gpu and self.classifier != "lightgbm":
            raise IncompatiblePipelineModules(
                "'use_gpu' is only supported with lightgbm classifier"
            )

    def set_features(self, df: pd.DataFrame) -> None:
        try:
            df.pop(self.target_column)
            logger.debug(f"Removed {self.target_column}")
        except Exception as e:
            logger.debug(e)

        self.features = df.columns.to_list()
        self.categorical_features = df.select_dtypes(include="object").columns.to_list()
        self.numeric_features = df.select_dtypes(include="number").columns.to_list()

        try:
            self._passthrough_features()
        except Exception as e:
            logger.error(e)
            raise

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

    def _passthrough_features(self):
        """Force passthroughts features out of"
        numerical and categorical features list"""
        num_set = set(self.numeric_features)
        cat_set = set(self.categorical_features)
        pass_set = set(self.passthrough_features)
        all_set = set(self.features)

        # Assert passthrough are part of all features set
        if not pass_set.issubset(all_set):
            raise TrainPipelineConfigError(
                "Provided passthrough features are not part of dataset\n",
                details={"passthrough_feature": self.passthrough_features},
            )

        self.numeric_features = list(num_set - pass_set)
        self.categorical_features = list(cat_set - pass_set)
        self.passthrough_features = list(pass_set)

    def __str__(self):
        return asdict(self)
