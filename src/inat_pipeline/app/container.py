import logging
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import PipelineConfig


@dataclass
class Dependencies:
    """
    Container for application dependencies.

    All dependencies are created here and injected into workflows.
    Makes testing easy (just inject mocks) and ensures consistent setup.
    """

    # Core components
    logger: logging.Logger
    project_root: Path
    package_root: Path
    log_path: Path
    version: str
    git_branch: str

    def __post_init__(self):
        self.config = PipelineConfig()
        self._DATA_FOLDER = self.project_root / "data"
        self._RAW_DATA_FOLDER = self._DATA_FOLDER / "raw"
        self._PROCESSED_DATA_FOLDER = self._DATA_FOLDER / "processed"

        self.RAW_DB_PATH = self._DATA_FOLDER / "inat_raw.duckdb"
        self.FEATURES_FOLDER = self._DATA_FOLDER / "features"
        self.FEATURES_FOLDER.mkdir(parents=True, exist_ok=True)

        self.S3_METADATA_URL = "s3://inaturalist-open-data/"

        self.QUERY_FOLDER = self.package_root / "queries"
        self.SQL_API_PATH = self.QUERY_FOLDER / "api"
        self.SQL_STAGE_PATH = self.QUERY_FOLDER / "stage"
        self.SQL_FEATURES_PATH = self.QUERY_FOLDER / "features"
        self.SQL_SCHEMA_RAW_PATH = self.QUERY_FOLDER / "schema" / "raw"
        self.SQL_SCHEMA_PATH = self.QUERY_FOLDER / "schema" / "main"

        self.SQL_MACROS_PATH = self.QUERY_FOLDER / "macros"

        self.optunaDB = "sqlite:///optuna.sqlite3"
        self.mlflowDB = self.project_root / "mlruns.db"

    def to_dict(self) -> dict:
        """Serialize config for logging to MLflow."""
        return asdict(self)
