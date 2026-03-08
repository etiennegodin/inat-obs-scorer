import logging
from typing import Union

from ..app.container import Dependencies
from ..utils.db import SQL_Engine, _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[int, None]):
    con = _open_connection(deps.DB_PATH)

    # Unpack raw data
    sql_ingest = SQL_Engine(con, deps.SQL_INGEST_PATH)
    # sql_features.execute("clean_inat_api")
    sql_ingest.execute("unpack_observations")
    sql_ingest.execute("unpack_identifications")
    sql_ingest.execute("unpack_photos")
    sql_ingest.execute("unpack_users")
    sql_ingest.execute("unpack_taxa")

    # Transform data and create features
    sql_features = SQL_Engine(con, deps.SQL_FEATURES_PATH)

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
    sql_features.execute("training")
