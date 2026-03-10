from pathlib import Path
from typing import Any

import yaml


def load(file: Path) -> dict[Any, Any]:
    try:
        with open(file, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        raise e
