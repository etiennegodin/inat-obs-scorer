import logging
from datetime import date

from ..app.container import Dependencies
from ..db import DuckDBConnection, DuckDbSQL
from ..queries.params import TrainingSplitParams

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        # Transform data and create features
        sql_features = DuckDbSQL(con, deps.SQL_FEATURES_PATH)
        sql_split = DuckDbSQL(con, deps.QUERY_FOLDER / "split")
        sql_features

        sql_features.execute_many(
            "community_taxon_windowed",
            "research_grade_windowed",
            "identifications",
            "identifiers",
            "taxon",
            "label",
            "observations",
            "observers",
            "observers_entropy",
        )
        """
        sql_features.execute_many("taxa_assymetry",
            "taxa_distance",
            "taxa_confusion",)
        """

        params = TrainingSplitParams(
            cutoff_date=date(2024, 1, 1),
            max_val_size=50000,
            val_window_days=250,
            max_test_size=90000,
        )
        sql_split.execute("split", params=params)

        df_total_val = sql_split.fetch_df("total_val_avail", params=params)

        # Fetch df from interpretability
        df_dist = sql_split.fetch_df("dist_year")
        df_splits = sql_split.fetch_df("training_splits_eda")
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
        print(f"Lost {removed} observations ~{round((removed/true_total) * 100, 3)}%")
        print(f"Total observation available in val split {total_val_avail}")
        print("\n", "-" * 50, "\n")

        sql_features.execute("training")
