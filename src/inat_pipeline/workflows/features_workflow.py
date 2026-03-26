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
            cutoff_date=date(2023, 6, 1),
            max_val_size=18000,
            val_window_days=365,
            max_test_size=100000,
            label_window_days=365,
            score_window_days=7,
            gap_days=21,
        )

        # Macros registering
        sql_features.execute_many(
            "community_taxon_windowed",
            "research_grade_windowed",
        )

        # Defined score window cut-off
        sql_features.execute("identifications_at_window", params=params)

        # Define base observations for all features and separate population split
        sql_features.execute_many("base", "model_population")

        # Label at label_window_days
        sql_features.execute("label", params=params)

        # Splits from model_population
        sql_split.execute("split", params=params)
        splits_report(sql_split, params)

        # Static features
        sql_graph.execute("confusion_graph")
        sql_graph.execute("confusion_graph_metrics")
        sql_features.execute("network_events_raw", params=params)

        # Bases and non paramterised queries
        sql_features.execute_many(
            "network_events", "user_role_timeline", "base", "temporal"
        )

        # Time-windowed features :
        sql_features.execute("taxon", params=params)
        sql_features.execute("observations", params=params)

        # With dependencies
        sql_features.execute_many(
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
