import logging
import time
from pathlib import Path

from .protocols import DBConnection

logger = logging.getLogger(__name__)


class SQLEngine:
    def __init__(self, con: DBConnection, sql_dir: Path):
        self.con = con
        self.sql_dir = Path(sql_dir)

    def execute(self, script_name: str, params: tuple = (), **identifiers) -> None:
        path = self.sql_dir / f"{script_name}.sql"

        if not path.exists():
            raise FileNotFoundError(f"SQL script not found: {path}")

        query = path.read_text()

        if identifiers:
            query = query.format(**identifiers)  # replace {table_name} etc.

        logger.debug("Executing SQL: %s", path.name)
        start = time.monotonic()

        self.con.execute(query, params)
        logger.info(
            f"Executed {path.stem}.sql ,"
            f"took {round((time.monotonic() - start), 3)}s"
        )

    def execute_many(self, *script_names: str) -> None:
        """Run multiple scripts in order — useful for staged pipelines."""
        for name in script_names:
            self.execute(name)
