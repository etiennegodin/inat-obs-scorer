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
