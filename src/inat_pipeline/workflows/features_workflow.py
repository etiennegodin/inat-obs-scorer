import logging

from ..app.container import Dependencies
from ..db import DuckDBConnection, SQLEngine

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        # Transform data and create features
        sql = SQLEngine(con, deps.SQL_FEATURES_PATH)
        sql.execute_many(
            [
                "community_taxon_windowed",
                "research_grade_windowed",
                "identifications",
                "identifiers",
                "taxon",
                "label",
                "observations",
                "observers",
                "taxa_assymetry",
                "taxa_distance",
                "taxa_confusion",
            ]
        )

        # Build params for interactive sql
        # split_format = TrainingSplitParams().build_params_cte()
        # sql_features.execute_with_params("stratify", split_format)

        sql.execute("training")
