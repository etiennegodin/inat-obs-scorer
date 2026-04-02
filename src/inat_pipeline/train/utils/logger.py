import json
import logging
from dataclasses import asdict

import mlflow

logger = logging.getLogger(__name__)


def export_config(config, output_path) -> None:
    try:
        with open(output_path, "w") as json_file:
            json.dump(asdict(config), json_file, indent=4)
    except Exception as e:
        logger.error(f"Failed to write final model params: {e}")


def export_final_model(pipeline, output_path) -> None:
    model = pipeline.named_steps["classifier"]
    # Log final model
    params = model.get_params()
    params["n_estimators"] = model.booster_.num_trees()
    mlflow.log_dict(params, "final_model_params.json")
    try:
        with open(output_path, "w") as json_file:
            json.dump(params, json_file, indent=4)
    except Exception as e:
        logger.error(f"Failed to write final model params: {e}")
