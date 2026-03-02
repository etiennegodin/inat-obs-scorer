import logging
from pathlib import Path

from ...utils.db import _open_connection

ROOT_FOLDER = Path(__file__).parents[4]
RAW_DATA_FOLDER = ROOT_FOLDER / "data" / "raw"
DOWNLOADS_FOLDER = RAW_DATA_FOLDER / "downloads"
RAW_DB_PATH = RAW_DATA_FOLDER / "raw.duckdb"

logger = logging.getLogger(__name__)


def ingest_downloads():
    con = _open_connection()
    try:
        con.execute(
            f"""CREATE IF NO EXISTS TABLE AS 
            SELECT * FROM read_csv_auto('{DOWNLOADS_FOLDER}*.csv')"""
        )
    except Exception as e:
        logger.exception(e)


if __name__ == "main":
    ingest_downloads()
