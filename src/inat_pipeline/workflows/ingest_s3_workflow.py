import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..queries.params import IngestS3Params

logger = logging.getLogger(__name__)


def execute(
    deps: Dependencies,
):
    with DuckDBAdapter(deps.RAW_DB_PATH) as con:
        # Create schema
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        sql = DuckDbSQL(con, deps.SQL_STAGE_PATH)

        sources = {
            "downloads": "observations.csv.gz",
            "taxa": "taxa.csv.gz",
            "observers": "observers.csv.gz",
            "photos": "photos.csv.gz",
        }

        for table, file in sources.items():
            s3_path = deps.S3_METADATA_URL + file
            source_function = "read_csv_auto"
            source_options = ", header=true, sep='\t', ignore_errors=true"

            logger.info(f"Ingesting {table} from {s3_path}")

            params = IngestS3Params(
                s3_path=s3_path,
                source_function=source_function,
                source_options=source_options,
            )

            sql.execute(
                "ingest_s3",
                params=params,
                table_name=f"raw.s3_{table}",
                columns="*",
                source_function=source_function,
                source_options=source_options,
            )

            sql.execute("stage_s3_photos")

        logger.info("S3 Ingestion complete.")
