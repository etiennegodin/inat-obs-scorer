from dataclasses import dataclass
from datetime import date


@dataclass
class SqlParams:
    """General purpose sql config"""

    pass


@dataclass
class TrainingSplitParams(SqlParams):
    cutoff_date: int = date(2021, 1, 1)
    gap_days: int = 90
    train_frac: float = 0.70
    val_frac: float = 0.15
    train_val_boundary: date = date(2021, 1, 1)
    val_test_boundary: date = date(2022, 6, 1)

    def build_params_cte(self) -> str:
        return f"""
WITH params AS (
    SELECT
        DATE '{self.cutoff_date}'                       AS cutoff_date,
        {self.gap_days}                               AS gap_days,
        DATE '{self.train_val_boundary.isoformat()}'  AS train_val_boundary,
        DATE '{self.val_test_boundary.isoformat()}'   AS val_test_boundary,
        {self.train_frac}                             AS train_frac,
        {self.val_frac}                               AS val_frac,
        {self.train_frac + self.val_frac}              AS train_val_frac

)
"""
