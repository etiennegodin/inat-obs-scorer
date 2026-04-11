import json
import logging
import uuid
from typing import Optional

from .adapters.duckdb_adapter import DuckDBAdapter

logger = logging.getLogger(__name__)


class LineageTracker:
    def __init__(self, con: DuckDBAdapter, run_id: Optional[uuid.UUID] = None):
        self.con = con
        self.run_id = run_id or uuid.uuid4()

    def start_run(self, command: str, config: dict, git_hash: str, git_branch: str):
        """Register the start of a new pipeline run."""
        logger.info(f"Starting run {self.run_id} for command: {command}")
        query = """
            INSERT INTO meta.runs
            (run_id, command, status, config, git_hash, git_branch)
            VALUES (?, ?, 'RUNNING', ?, ?, ?)
        """
        self.con.execute(
            query,
            (
                self.run_id,
                command,
                json.dumps(config, default=str),
                git_hash,
                git_branch,
            ),
        )

    def end_run(self, status: str = "COMPLETED", error: Optional[str] = None):
        """Register the end of a pipeline run."""
        query = """
            UPDATE meta.runs
            SET status = ?, ended_at = CURRENT_TIMESTAMP, error_message = ?
            WHERE run_id = ?
        """
        self.con.execute(query, (status, error, self.run_id))

    def start_task(
        self,
        task_name: str,
        inputs_hash: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> uuid.UUID:
        """Register the start of a task within the current run."""
        lineage_id = uuid.uuid4()
        query = """
            INSERT INTO meta.lineage
            (lineage_id, run_id, task_name, status, inputs_hash, metadata)
            VALUES (?, ?, ?, 'RUNNING', ?, ?)
        """
        self.con.execute(
            query,
            (
                lineage_id,
                self.run_id,
                task_name,
                inputs_hash,
                json.dumps(metadata or {}, default=str),
            ),
        )
        return lineage_id

    def end_task(
        self,
        lineage_id: uuid.UUID,
        status: str = "COMPLETED",
        error: Optional[str] = None,
        outputs_path: Optional[str] = None,
    ):
        """Register the end of a task."""
        query = """
            UPDATE meta.lineage
            SET status = ?,
            ended_at = CURRENT_TIMESTAMP, error_message = ?, outputs_path = ?
            WHERE lineage_id = ?
        """
        self.con.execute(query, (status, error, outputs_path, lineage_id))

    def is_task_completed(
        self, task_name: str, inputs_hash: Optional[str] = None
    ) -> bool:
        """Check if a task with the same
        name and inputs has already completed successfully."""
        if inputs_hash:
            query = """
                SELECT COUNT(*) FROM meta.lineage
                WHERE task_name = ? AND inputs_hash = ? AND status = 'COMPLETED'
            """
            result = self.con.execute(query, (task_name, inputs_hash)).fetchone()
        else:
            # Check if this task has EVER been completed successfully across any run
            query = """
                SELECT COUNT(*) FROM meta.lineage
                WHERE task_name = ? AND status = 'COMPLETED'
            """
            result = self.con.execute(query, (task_name,)).fetchone()

        return result[0] > 0 if result else False
