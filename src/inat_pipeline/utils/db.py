import logging
import time
from dataclasses import dataclass
from datetime import date
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


@dataclass
class SqlConfig:
    """General purpose sql config"""

    pass


@dataclass
class SplitConfig(SqlConfig):
    cutoff_date: int = date(2021, 1, 1)
    gap_days: int = 90
    train_frac: float = 0.70
    val_frac: float = 0.15
    train_val_boundary: date = date(2021, 1, 1)
    val_test_boundary: date = date(2022, 6, 1)

    def build_params_cte(self) -> str:
        return f"""
WITH params AS (
    SELECT
        DATE '{self.cutoff_date}'                       AS cutoff_date,
        {self.gap_days}                               AS gap_days,
        DATE '{self.train_val_boundary.isoformat()}'  AS train_val_boundary,
        DATE '{self.val_test_boundary.isoformat()}'   AS val_test_boundary,
        {self.train_frac}                             AS train_frac,
        {self.val_frac}                               AS val_frac,
        {self.train_frac + self.val_frac}              AS train_val_frac
    
)
"""


class SQL_Engine:
    def __init__(self, con: duckdb.DuckDBPyConnection, path: Path):
        self.con = con
        self.path = path
        logger.info(f"Initialized sql engine for queries in {path}")

    @staticmethod
    def _check_file(file: Path) -> bool:
        exists = False
        try:
            open(file, "r")
            exists = True
        except FileNotFoundError as e:
            logger.error(e)
            raise FileNotFoundError(e)
        except Exception:
            logger.error(f"Unexpected error reading file {file}")
            raise InatPipelineError(f"Unexpected error reading file {file}")

        return exists

    def execute(self, name: str) -> None:
        file = self._add_suffix(self.path / name)
        if self._check_file(file):
            with open(file, "r") as f:
                self._execute(f.read(), file)

    def execute_with_params(self, name: str, format: str) -> None:
        file = self._add_suffix(self.path / name)
        if self._check_file(file):
            with open(file, "r") as f:
                sql = f.read().format(params_cte=format)
                # logger.debug(sql)
                self._execute(sql, file)

    def _execute(self, sql: str, file_path: str) -> None:
        try:
            start = time.monotonic()
            self.con.execute(sql)
            logger.info(
                f"Executed {file_path.stem},"
                f"took {round((time.monotonic() - start), 3)}s"
            )
        except Exception as e:
            raise SqlError(f"Error executing sql query: \n{file_path} \n{e}")

    def _add_suffix(self, file):
        if not str(file).endswith(".sql"):
            file = Path(f"{file}.sql")
        return file
