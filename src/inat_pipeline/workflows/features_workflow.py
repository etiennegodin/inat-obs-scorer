import logging
from datetime import date

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..queries.params import TrainingSplitParams
from ..utils.splits import splits_report

logger = logging.getLogger(__name__)


def execute(
    deps: Dependencies,
    params: TrainingSplitParams | None = None,
    output_name: str = "features",
):

    with DuckDBAdapter(deps.DB_PATH) as con:
        # Transform data and create features

        sql_features = DuckDbSQL(con, deps.SQL_FEATURES_PATH)
        sql_split = DuckDbSQL(con, deps.QUERY_FOLDER / "split")

        # Train/Val/Test splits
        params = TrainingSplitParams(
            label_window_days=365,
            scraped_at=date(2026, 3, 1),
            score_window_days=7,
            cutoff_date=date(2023, 1, 1),
            max_val_size=30000,
            val_window_days=410,
            max_test_size=100000,
            gap_days=30,
        )

        # Macros registering
        sql_features.execute("macro_blended_histogram")
        sql_features.execute("macro_community_taxon_windowed")
        sql_features.execute("macro_research_grade_windowed")

        # Temporal features -- issue with macro if lower to-do
        sql_features.execute("temporal")

        # Define base observations for all features
        sql_features.execute("base", params=params)

        # Defined model population
        sql_features.execute("model_population", params=params)

        sql_features.execute("taxon", params=params)

        # Label at label_window_days
        sql_features.execute("label", params=params)

        # Splits from model_population
        sql_split.execute("split", params=params)
        splits_report(sql_split, params)

        # Taxon features
        sql_features.execute("taxon_specialist", params=params)
        sql_features.execute("taxon_confusion", params=params)

        sql_features.execute("network_events_raw", params=params)

        # Bases and non parametrised queries
        sql_features.execute_many(
            "network_events",
            "user_role_timeline",
        )

        # Time-windowed features :
        sql_features.execute("observations", params=params)
        sql_features.execute("identifications_at_window", params=params)

        # With dependencies
        sql_features.execute_many(
            "identifications",
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
