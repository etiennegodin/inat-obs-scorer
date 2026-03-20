import ast
import json
import logging

import mlflow
from mlflow.tracking import MlflowClient

from ..config import PipelineConfig

logger = logging.getLogger(__name__)


def get_feature_diff(
    config: PipelineConfig,
    n_runs: int = 1,
    tracking_uri: str | None = None,
) -> dict[str, list[str]] | None:
    """
    Print a colour-coded feature diff report and return the diff dict.

    Parameters
    ----------
    current_features : list of feature name strings for the upcoming run.
    experiment_name  : MLflow experiment name to search within.
    param_name       : The param key under which you log the feature list.
    n_runs           : Compare against the Nth most-recent run (default 1 = last run).
    tracking_uri     : Optional override for mlflow.set_tracking_uri().

    Returns
    -------
    dict with keys "added", "removed", "common" — or None if no prior run found.
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    current = set(config.features)

    prev_features, run_id = _fetch_previous_features(
        experiment_name=config.experiment_name,
        run_name=config.run_name,
        param_name="features",
        n_runs=n_runs,
    )

    if prev_features is None:
        return None
    prev = set(prev_features)

    added = sorted(current - prev)
    removed = sorted(prev - current)
    # common = sorted(current & prev)
    return {"added": added, "removed": removed}


def _parse_feature_param(raw: str) -> list[str]:
    """
    Parse a features param that may have been logged as:
        - a JSON array string:  '["a", "b", "c"]'
        - a Python repr string: "['a', 'b', 'c']"
        - a comma-separated string: 'a, b, c'
    """
    raw = raw.strip()
    # JSON
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return [str(f) for f in result]
    except json.JSONDecodeError:
        pass
    # Python literal
    try:
        result = ast.literal_eval(raw)
        if isinstance(result, (list, tuple)):
            return [str(f) for f in result]
    except (ValueError, SyntaxError):
        pass
    # Comma-separated fallback
    return [f.strip() for f in raw.split(",") if f.strip()]


def _fetch_previous_features(
    experiment_name: str,
    run_name: str,
    param_name: str,
    n_runs: int = 1,
) -> tuple[list[str], str] | tuple[None, None]:
    """
    Return (feature_list, run_id) for the Nth most recent *finished* run
    in the given experiment that has `param_name` logged.
    Returns (None, None) if no matching run is found.
    """
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return None, None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"params.{param_name} != ''",
        order_by=["start_time DESC"],
        max_results=n_runs + 10,  # small buffer in case some runs lack the param
    )

    this_runs = [r for r in runs if r.info.run_name == run_name]

    finished = [r for r in this_runs if r.info.status == "FINISHED"]
    if len(finished) < n_runs:
        return None, None

    target = finished[n_runs - 1]
    raw = target.data.params.get(param_name)
    if raw is None:
        return None, None
    return _parse_feature_param(raw), target.info.run_id
