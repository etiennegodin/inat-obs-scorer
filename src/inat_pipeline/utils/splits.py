"""
Reports on split strategy
"""

from ..db.sql import SQLEngine
from ..queries.params import TrainingSplitParams


def splits_report(sql: SQLEngine, params: TrainingSplitParams):
    df_total_val = sql.fetch_df("total_val_avail", params=params)
    # Fetch df from interpretability
    df_dist = sql.fetch_df("dist_year")
    df_splits = sql.fetch_df("training_splits_eda")
    df_splits["perc"] = (
        df_splits["n_obs"].div(df_splits["n_obs"].sum(axis=0), axis=0) * 100
    )

    true_total = df_dist["n"].sum()
    split_total = df_splits["n_obs"].sum()
    total_val_avail = df_total_val["total_val"].iloc[0]
    used_val = df_splits["n_obs"].iloc[1]
    removed = true_total - split_total

    print("\n", "-" * 50, "\n")
    print(df_splits)
    print(f"Starting model population total: {true_total}")
    print(f"Observations kept in split: {split_total}")
    print(
        f"Lost {removed} obs of total dataset"
        f" ~{round((removed / true_total) * 100, 3)}%"
    )
    print(
        f"{used_val} used obs in val split"
        f" from possible {total_val_avail}. "
        f" [{round((used_val / total_val_avail), 2) * 100}%]"
    )
    print("\n", "-" * 50, "\n")
