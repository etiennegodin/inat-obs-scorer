import logging
import re

import optuna

logger = logging.getLogger(__name__)


def get_next_study_name(base_name: str, storage_url: str) -> str:
    """
    Increments the patch version automatically.
    If 'LGBM_v1.2' is passed, it looks for 'LGBM_v1.2.X'
    and returns 'LGBM_v1.2.0' or 'LGBM_v1.2.1' etc.
    """
    try:
        existing_studies = optuna.get_all_study_names(storage=storage_url)
    except Exception as e:
        logger.error(f"Error getting study names: {e}")
        existing_studies = []

    # Filter studies that match the "base_name.patch" pattern
    pattern = re.compile(rf"^{re.escape(base_name)}\.(\d+)$")
    patches = []

    for name in existing_studies:
        match = pattern.match(name)
        if match:
            patches.append(int(match.group(1)))

    # Next patch is max + 1, or 0 if none exist
    next_patch = max(patches) + 1 if patches else 0
    next_name = f"{base_name}.{next_patch}"
    logger.info(f"Starting study {next_name}")
    return next_name


class EarlyStoppingCallback:
    def __init__(self, early_stopping_rounds: int, direction: str = "maximize"):
        self.early_stopping_rounds = early_stopping_rounds
        self.direction = direction

    def __call__(
        self, study: optuna.study.Study, trial: optuna.trial.FrozenTrial
    ) -> None:
        # Get the number of the best trial found so far
        best_trial_number = study.best_trial.number
        # Get the current trial number
        current_trial_number = trial.number

        # If the gap between them is greater than our limit, stop
        if current_trial_number - best_trial_number >= self.early_stopping_rounds:
            print(
                "Stopping study: No improvement in "
                f"{self.early_stopping_rounds} trials."
            )
            study.stop()
