import logging

from ..app.container import Dependencies
from ..utils.db import SQL_Engine, _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    con = _open_connection(deps.DB_PATH)
    sql = SQL_Engine(con, deps.FEATURES_QUERY_FOLDER)
    sql.execute()
