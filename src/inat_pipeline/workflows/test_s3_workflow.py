import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBAdapter(deps.DB_PATH) as con:
        # Check observations
        s3_url = deps.S3_METADATA_URL + "observations.csv.gz"
        logger.info(f"Checking S3 file: {s3_url}")
        try:
            res = con.execute(
                f"""DESCRIBE SELECT *
                FROM read_csv_auto('{s3_url}', header=true, sep='\\t', n_rows=1);"""
            )
            logger.info("Observations schema:")
            for row in res.fetchall():
                logger.info(f"  {row[0]}: {row[1]}")

            # Check taxa
            s3_url = deps.S3_METADATA_URL + "taxa.csv.gz"
            logger.info(f"Checking S3 file: {s3_url}")
            res = con.execute(
                f"""DESCRIBE SELECT *
                FROM read_csv_auto('{s3_url}', header=true, sep='\\t', n_rows=1);"""
            )
            logger.info("Taxa schema:")
            for row in res.fetchall():
                logger.info(f"  {row[0]}: {row[1]}")

        except Exception as e:
            logger.error(f"Error checking S3: {e}")
            raise e
