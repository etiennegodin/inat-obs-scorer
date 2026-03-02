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
        self.RAW_DATA_FOLDER = self.root / "data" / "raw"
        self.DOWNLOADS_FOLDER = self.RAW_DATA_FOLDER / "downloads"
        self.RAW_DB_PATH = self.RAW_DATA_FOLDER / "raw.duckdb"
