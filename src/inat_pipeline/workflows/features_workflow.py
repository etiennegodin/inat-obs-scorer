import logging
from datetime import date

from ..app.container import Dependencies
from ..db import DuckDBConnection, DuckDbSQL
from ..queries.params import TrainingSplitParams
from ..utils.splits import splits_report

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
            "identifications_timeline",
            "cumulative_id_stats",
            "identifications",
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

        # Train/Val/Test splits
        params = TrainingSplitParams(
            cutoff_date=date(2024, 1, 1),
            max_val_size=50000,
            val_window_days=250,
            max_test_size=90000,
        )
        sql_split.execute("split", params=params)
        splits_report(sql_split, params)

        # Final merge
        sql_features.execute("training")
