import logging
import pprint
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

logger = logging.getLogger(__name__)


@dataclass
class SqlParams:
    """General purpose sql config"""

    pass


@dataclass
class IngestCSVParams:
    source_dir: str
    ignore: bool = True

    def __post_init__(self):
        # Format csv files path in source dir
        self.source_dir = f"{self.source_dir}/*.csv"


@dataclass
class IngestS3Params:
    s3_path: str


@dataclass
class TrainingSplitParams:
    cutoff_date: date
    scraped_at: date
    score_window_days: int = 7
    label_window_days: int = 365
    gap_days: int = 30
    val_window_days: int = 270  # Custom to dataset
    max_val_size: int = 50000
    max_test_size: int = 80000
    max_created_date: date = field(init=False)

    # Declared in post_init
    val_start: int = field(init=False)
    val_end: int = field(init=False)
    test_start: int = field(init=False)
    prediction_horizon: int = field(init=False)

    def __post_init__(self):
        # Assert score window is higher than label_window_days
        assert self.gap_days >= self.score_window_days
        assert self.label_window_days > self.score_window_days + self.gap_days

        # Dynamic date cutoffs based on gap days and val_window_days
        self.val_start = self.cutoff_date + timedelta(days=self.gap_days)
        self.val_end = self.val_start + timedelta(days=self.val_window_days)
        self.test_start = self.val_end + timedelta(days=self.gap_days)
        self.prediction_horizon = self.label_window_days - self.score_window_days

        # Max created date filter
        self.max_created_date = self.scraped_at - timedelta(days=self.label_window_days)

        # Log
        s = pprint.pformat(asdict(self), indent=4)
        logger.debug(s)
