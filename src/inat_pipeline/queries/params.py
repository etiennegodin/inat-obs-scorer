from dataclasses import dataclass
from datetime import date


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
    cutoff_date: int = date(2021, 1, 1)
    gap_days: int = 90
    train_val_boundary: date = date(2021, 1, 1)
    val_test_boundary: date = date(2022, 6, 1)

    train_frac: float = 0.70
    val_frac: float = 0.15

    def __post_init__(self):
        # Format values for sql
        self.train_val_boundary = self.train_val_boundary.isoformat()
        self.val_test_boundary = self.val_test_boundary.isoformat()
