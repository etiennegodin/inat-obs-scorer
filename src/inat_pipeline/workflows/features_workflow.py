import logging
from dataclasses import asdict

from ..app.container import Dependencies
from ..db import DuckDBConnection, DuckDbSQL
from ..queries.params import TrainingSplitParams

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        # Transform data and create features
        sql = DuckDbSQL(con, deps.SQL_FEATURES_PATH)

        """
        sql.execute_many(
            "community_taxon_windowed",
            "research_grade_windowed",
            "identifications",
            "identifiers",
            "taxon",
            "label",
            "observations",
            "observers",
            "observers_entropy",
            "taxa_assymetry",
            "taxa_distance",
            "taxa_confusion",
        )
        """
        params = TrainingSplitParams()

        sql.execute("split", params=asdict(params))

        # Build params for interactive sql
        # sql_features.execute_with_params("stratify", split_format)

        sql.execute("training")
