import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..queries.params import IngestS3Params

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, conditions: dict[str, str] | None = None):
    with DuckDBAdapter(deps.RAW_DB_PATH) as con:
        # Create schema
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        sql = DuckDbSQL(con, deps.SQL_STAGE_PATH)

        # 1. Ingest from S3
        # Note: 'downloads' table in the pipeline corresponds to observations.csv.gz
        sources = {
            "downloads": "observations.csv.gz",
            "taxa": "taxa.csv.gz",
            "observers": "observers.csv.gz",
            "photos": "photos.csv.gz",
        }

        conditions = conditions or {}

        for table, file in sources.items():
            s3_path = deps.S3_METADATA_URL + file
            condition = conditions.get(table, "1=1")
            logger.info(f"Ingesting {table} from {s3_path} (condition: {condition})")
            params = IngestS3Params(s3_path=s3_path, condition=condition)
            sql.execute(
                "ingest_s3", params=params, table_name=f"raw.{table}", columns="*"
            )

        logger.info("S3 Ingestion complete.")
