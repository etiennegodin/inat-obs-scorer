import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from .protocols import DBConnection

logger = logging.getLogger(__name__)


class SQLEngine:
    def __init__(self, con: DBConnection, sql_dir: Path):
        self.con = con
        self.sql_dir = Path(sql_dir)

    def _load(self, script_name: str, **identifiers) -> str:
        """Shared file loading + identifier injection."""
        path = self.sql_dir / f"{script_name}.sql"
        if not path.exists():
            raise FileNotFoundError(f"SQL script not found: {path}")
        query = path.read_text()
        return (
            query.format(**identifiers) if identifiers else query
        )  # replace {table_name} etc.

    def execute(self, script_name: str, params: tuple = (), **identifiers) -> None:
        """Run a mutation — CREATE, INSERT, UPDATE. Returns nothing."""
        query = self._load(script_name, **identifiers)
        logger.debug("Executing SQL: %s", script_name)
        start = time.monotonic()
        self.con.execute(query, params)
        logger.info(
            f"Executed {script_name}.sql, "
            f"took {round((time.monotonic() - start), 3)}s"
        )

    def fetch(
        self, script_name: str, params: tuple = (), **identifiers
    ) -> list[dict[Any, Any]]:
        """Run a SELECT — returns rows as dicts."""
        query = self._load(script_name, **identifiers)
        result = self.con.execute(query, params)
        columns = [col[0] for col in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def fetch_df(
        self, script_name: str, params: tuple = (), **identifiers
    ) -> pd.DataFrame:
        """Fetch rows and convert to DataFrame — works with any PEP 249 driver."""
        query = self._load(script_name, **identifiers)
        result = self.con.execute(query, params)
        columns = [col[0] for col in result.description]
        return pd.DataFrame(result.fetchall(), columns=columns)

    def execute_many(self, *script_names: str) -> None:
        """Run multiple scripts in order — useful for staged pipelines."""
        logger.debug(script_names)
        for name in script_names:
            logger.debug(name)
            self.execute(name)
