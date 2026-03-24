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
    removed = true_total - split_total

    print("\n", "-" * 50, "\n")
    print(df_splits)
    print(split_total, "observations in splits")
    print(f"Lost {removed} observations ~{round((removed / true_total) * 100, 3)}%")
    print(f"Total observation available in val split {total_val_avail}")
    print("\n", "-" * 50, "\n")
