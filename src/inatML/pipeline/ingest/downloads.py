from pathlib import Path

from duckdb import CatalogException

from ...utils.db import _open_connection


def ingest_downloads(db_path: Path, downloads_path: Path) -> list[Path]:
    con = _open_connection(db_path)
    create_query = f"""CREATE TABLE downloads AS 
            SELECT *
            FROM read_csv_auto('{downloads_path}/*.csv')"""
    files = [file for file in downloads_path.rglob("*.csv")]
    try:
        con.execute(create_query)
        return files
    except CatalogException:
        con.execute("DROP TABLE IF EXISTS downloads")
        try:
            con.execute(create_query)
            return files
        except Exception as e:
            raise e
