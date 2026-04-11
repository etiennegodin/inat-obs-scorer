import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL

logger = logging.getLogger(__name__)


def execute(
    deps: Dependencies,
):
    with DuckDBAdapter(deps.RAW_DB_PATH, macro_path=deps.SQL_MACROS_PATH) as con:
        # Stage collected data in db
        sql_stage = DuckDbSQL(con, deps.SQL_STAGE_PATH)

        # Similar species
        sql_stage.execute("stage_similar_species")
        sql_stage.execute("taxon_conf_tempo")

        # Network events
        sql_stage.execute("network_events_raw")

        # Bases and non parametrised queries
        sql_stage.execute_many(
            "network_events",
            "user_role_timeline",
        )

        # Static confusion features
        sql_graph = DuckDbSQL(con, deps.QUERY_FOLDER / "graph", ignore_params=True)
        sql_graph.execute("confusion_graph")
        sql_graph.execute("confusion_topology")
        sql_graph.execute("double_hop")
        sql_graph.execute("double_hop_derived")
