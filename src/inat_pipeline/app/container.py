import logging
from dataclasses import dataclass
from pathlib import Path


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

    def __post_init__(self):
        self._DATA_FOLDER = self.project_root / "data"
        self._RAW_DATA_FOLDER = self._DATA_FOLDER / "raw"
        self._PROCESSED_DATA_FOLDER = self._DATA_FOLDER / "processed"

        self.DOWNLOADS_FOLDER = self._RAW_DATA_FOLDER / "downloads"
        self.DB_PATH = self._DATA_FOLDER / "inat.duckdb"

        self._QUERY_FOLDER = self.package_root / "queries"
        self.SQL_INGEST_PATH = self._QUERY_FOLDER / "ingest"
        self.SQL_FEATURES_PATH = self._QUERY_FOLDER / "features"

        self.API_FIELDS_PATH = self.project_root / "api_fields.yaml"
