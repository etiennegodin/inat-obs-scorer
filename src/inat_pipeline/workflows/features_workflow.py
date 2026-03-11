import logging

from ..app.container import Dependencies
from ..db import DuckDBConnection, SQLEngine

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        # Transform data and create features
        sql_features = SQLEngine(con, deps.SQL_FEATURES_PATH)

        # Macros
        sql_features.execute("community_taxon_windowed")
        sql_features.execute("research_grade_windowed")

        # Features
        sql_features.execute("identifications")
        sql_features.execute("identifiers")
        sql_features.execute("taxon")
        sql_features.execute("label")
        sql_features.execute("observations")
        sql_features.execute("observers")

        sql_features.execute("taxa_assymetry")
        sql_features.execute("taxa_distance")
        sql_features.execute("taxa_confusion")

        # Build params for interactive sql
        # split_format = TrainingSplitParams().build_params_cte()
        # sql_features.execute_with_params("stratify", split_format)
        sql_features.execute("training")
