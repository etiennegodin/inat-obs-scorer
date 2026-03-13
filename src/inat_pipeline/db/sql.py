import logging
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import sqlparams

from .protocols import DBConnection

ALLOWED_TABLES = ["raw.downloads", "raw.taxa", "raw.places"]

logger = logging.getLogger(__name__)


class SQLEngine(ABC):
    def __init__(self, con: DBConnection, sql_dir: Path):
        self.con = con
        self.sql_dir = Path(sql_dir)

    @abstractmethod
    def _parametrise_query(self, query: str, params: dict) -> tuple[str, list]:
        """Change for sql flavor"""
        pass

    def _prep_params(self, params: Any) -> dict:
        # Convert params to dict if passed as dataclass
        if is_dataclass(params):
            return asdict(params)
        else:
            return {}

    def _identifiers(self, query: str, **identifiers) -> str:
        # Insert identifiers
        if any(value not in ALLOWED_TABLES for value in identifiers.values()):
            logger.debug(
                [value for value in identifiers.values() if value not in ALLOWED_TABLES]
            )
            raise ValueError("Invalid table access!")
        return query.format(**identifiers) if identifiers else query

    def _load(self, script_name: str, params: Any, **identifiers) -> tuple[str, Any]:
        """Shared file loading + params & identifier injection."""
        path = self.sql_dir / f"{script_name}.sql"
        if not path.exists():
            raise FileNotFoundError(f"SQL script not found: {path}")
        query = path.read_text()

        # Format params to dict
        params = self._prep_params(params)

        # Parametrisation by subclass
        query, values = self._parametrise_query(query, params)
        # Inject identitifers
        identified = self._identifiers(query, **identifiers)
        return identified, values

    def execute(self, script_name: str, params: Any = None, **identifiers) -> None:
        """Run a mutation — CREATE, INSERT, UPDATE. Returns nothing."""
        query, values = self._load(script_name, params, **identifiers)
        logger.debug("Executing SQL: %s", script_name)
        start = time.monotonic()
        self.con.execute(query, values)
        logger.info(
            f"Executed {script_name}.sql, "
            f"took {round((time.monotonic() - start), 3)}s"
        )

    def fetch(
        self, script_name: str, params: Any, **identifiers
    ) -> list[dict[Any, Any]]:
        """Run a SELECT — returns rows as dicts."""
        query, params = self._load(script_name, params, **identifiers)
        result = self.con.execute(query, params)
        columns = [col[0] for col in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def fetch_df(self, script_name: str, params: Any, **identifiers) -> pd.DataFrame:
        """Fetch rows and convert to DataFrame — works with any PEP 249 driver."""
        query, params = self._load(script_name, params, **identifiers)
        result = self.con.execute(query, params)
        columns = [col[0] for col in result.description]
        return pd.DataFrame(result.fetchall(), columns=columns)

    def execute_many(self, *script_names: str) -> None:
        """Run multiple scripts in order — useful for staged pipelines."""
        logger.debug(script_names)
        for name in script_names:
            logger.debug(name)
            self.execute(name)


class DuckDbSQL(SQLEngine):
    def __init__(self, con: DBConnection, sql_dir: Path):
        self.con = con
        self.sql_dir = Path(sql_dir)

    def _parametrise_query(self, query: str, params: dict) -> tuple[str, list]:
        """Change for sql flavor"""
        query_tool = sqlparams.SQLParams("named", "qmark")
        sql, values = query_tool.format(query, params)
        return sql, values
