import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..queries.params import TrainingSplitParams
from ..utils.splits import splits_report

logger = logging.getLogger(__name__)


def execute(
    deps: Dependencies,
    params: TrainingSplitParams,
    feature_set_name: str = "features",
):
    # Path to the specific feature set database
    db_path = deps.FEATURES_FOLDER / f"features_{feature_set_name}.duckdb"

    # Connect to RAW_DB as primary (read_only) and attach features DB as writable output
    with DuckDBAdapter(
        deps.RAW_DB_PATH,
        attach_path=db_path,
        attach_alias="features_out",
        read_only=True,
        schema_path=deps.SQL_SCHEMA_PATH,
        macro_path=deps.SQL_MACROS_PATH,
    ) as con:
        # Transform data and create features

        sql_features = DuckDbSQL(con, deps.SQL_FEATURES_PATH)
        sql_split = DuckDbSQL(con, deps.QUERY_FOLDER / "split")
        con.execute("CREATE SCHEMA IF NOT EXISTS features")

        # Temporal features -- issue with macro if lower to-do
        sql_features.execute("temporal")

        # Define base observations for all features
        sql_features.execute("base", params=params)

        sql_features.execute("identifications_at_window", params=params)

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

        # Time-windowed features :
        sql_features.execute("observations", params=params)

        # With dependencies
        sql_features.execute_many("identifications", "observers_entropy", "community")

        # Final merge
        sql_features.execute("training")

        # Export to data version controlled file
        output_path = deps.FEATURES_FOLDER / f"{feature_set_name}.parquet"
        con.execute(
            f"""COPY features.training TO
            '{output_path}' (FORMAT PARQUET);"""
        )
        logger.info(f"Exported features matrix to {output_path}")
