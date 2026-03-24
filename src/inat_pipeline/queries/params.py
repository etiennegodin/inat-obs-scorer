from dataclasses import dataclass, field
from datetime import date, timedelta


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
class TrainingSplitParams:
    cutoff_date: date
    gap_days: int = 90
    val_window_days: int = 270
    max_val_size: int = 50000
    max_test_size: int = 80000
    val_start: int = field(init=False)
    val_end: int = field(init=False)
    test_start: int = field(init=False)
    score_window: int = 14

    def __post_init__(self):
        # Dynamic date cutoffs based on gap days and val_window_days
        self.val_start = self.cutoff_date + timedelta(days=self.gap_days)
        self.val_end = self.val_start + timedelta(days=self.val_window_days)
        self.test_start = self.val_end + timedelta(days=self.gap_days)

        # Assert score window is higher than gap_days
        assert self.gap_days >= self.score_window
