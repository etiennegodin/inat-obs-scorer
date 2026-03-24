import logging
from datetime import date

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..queries.params import TrainingSplitParams
from ..utils.splits import splits_report

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBAdapter(deps.DB_PATH) as con:
        # Transform data and create features

        sql_features = DuckDbSQL(con, deps.SQL_FEATURES_PATH)
        sql_split = DuckDbSQL(con, deps.QUERY_FOLDER / "split")
        sql_graph = DuckDbSQL(con, deps.QUERY_FOLDER / "graph", ignore_params=True)

        # Train/Val/Test splits
        params = TrainingSplitParams(
            cutoff_date=date(2024, 1, 1),
            max_val_size=50000,
            val_window_days=250,
            max_test_size=90000,
            gap_days=90,
        )
        # Macros
        sql_features.execute_many(
            "community_taxon_windowed",
            "research_grade_windowed",
        )

        # Label
        sql_features.execute("label", params=params)

        # Splits
        sql_split.execute("split", params=params)
        splits_report(sql_split, params)

        # Confusion graphs
        sql_graph.execute("confusion_graph")
        sql_graph.execute("confusion_graph_metrics")

        # features with gap days :
        sql_features.execute("network_events", params=params)

        sql_features.execute_many(
            "network_events_stats",
            "user_role_timeline",
            "base",
            "taxon",
            "observations",
            "identifications",
            "taxa_confusion",
            "observers_entropy",
        )

        # Final merge
        sql_features.execute("training")

        # Export to data version controlled file
        output_path = deps._DATA_FOLDER / "features.parquet"
        con.execute(
            f"""COPY features.training TO
            '{output_path}' (FORMAT PARQUET);"""
        )
        logger.info(f"Exported features matrix to {output_path}")
