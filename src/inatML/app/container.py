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
    root: Path

    def __post_init__(self):
        self._DATA_FOLDER = self.root / "data"
        self._RAW_DATA_FOLDER = self._DATA_FOLDER / "raw"
        self._PROCESSED_DATA_FOLDER = self._DATA_FOLDER / "processed"

        self.DOWNLOADS_FOLDER = self._RAW_DATA_FOLDER / "downloads"
        self.DB_PATH = self._DATA_FOLDER / "inat.duckdb"

        self._QUERY_FOLDER = self.root / "queries"
        self.RAW_QUERY_FOLDER = self._QUERY_FOLDER / "raw"
        self.FEATURES_QUERY_FOLDER = self._QUERY_FOLDER / "features"
