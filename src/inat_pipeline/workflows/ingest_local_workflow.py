import logging

from ..app.container import Dependencies
from ..db import DuckDBConnection, DuckDbSQL
from ..queries.params import IngestCSVParams

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        data_dir = deps._RAW_DATA_FOLDER

        # con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        sql = DuckDbSQL(con, deps.SQL_STAGE_PATH)

        # Ingest observations csv files
        source = "downloads"
        downloads_params = IngestCSVParams(source_dir=data_dir / source)
        sql.execute(
            "ingest_csv",
            params=downloads_params,
            table_name=f"raw.{source}",
            columns="*",
        )

        # Ingest taxa
        source = "taxa"
        downloads_params = IngestCSVParams(source_dir=data_dir / source)
        sql.execute(
            "ingest_csv",
            params=downloads_params,
            table_name=f"raw.{source}",
            columns="*",
        )

        # Assert needed columns
        sql.execute("stage_taxa")

        # Ingest places
        source = "places"
        downloads_params = IngestCSVParams(
            source_dir=data_dir / source,
        )
        sql.execute(
            "ingest_csv",
            params=downloads_params,
            table_name=f"raw.{source}",
            columns="id, slug, admin_level, latitude,"
            "longitude,swlat,swlng,nelat,nelng,place_type,bbox_area, uuid",
        )
