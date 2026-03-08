import logging
from pathlib import Path

import duckdb
from duckdb import IOException

from ..pipeline.exceptions import InatPipelineError, SqlError

logger = logging.getLogger(__name__)


def _open_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    # always create a fresh connection; use context manager where possible
    try:
        con = duckdb.connect(database=db_path)
        _load_spatial_extension(con)
        return con
    except IOException as e:
        logger.error(e)
        raise e
    except OSError as e:
        logger.error(e)
        raise e

    except Exception as e:
        logger.exception(f"Error connection to duckdb {db_path} : \n ", e)
        raise IOError(f"Error connecting : {e}")


def _load_spatial_extension(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
    except Exception as e:
        logger.error(f"Error loading spatial extension : {e}")
        raise e


class SQL_Engine:
    def __init__(self, con: duckdb.DuckDBPyConnection, path: Path):
        self.con = con
        self.path = path

    def execute(self, name: str):
        file = self._add_suffix(self.path / name)
        logger.info(f"Running {file} ")

        try:
            open(file, "r")
        except FileNotFoundError as e:
            logger.error(e)
            raise FileNotFoundError(e)
        except Exception:
            logger.error(f"Unexpected error reading file {file}")
            raise InatPipelineError(f"Unexpected error reading file {file}")

        else:
            with open(file, "r") as f:
                try:
                    self.con.execute(f.read())
                except Exception:
                    raise SqlError(f"Error executing sql query: \n{file}")

    def _add_suffix(self, file):
        if not str(file).endswith(".sql"):
            file = Path(f"{file}.sql")
        return file
