-- Lineage and Run metadata schema
CREATE SCHEMA IF NOT EXISTS meta;

-- Track high-level pipeline runs
CREATE TABLE IF NOT EXISTS meta.runs (
    run_id UUID PRIMARY KEY,
    command TEXT NOT NULL,           -- e.g., 'run', 'ingest', 'train'
    status TEXT NOT NULL,            -- PENDING, RUNNING, COMPLETED, FAILED
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    config JSON,                     -- Full CLI args/config
    git_hash TEXT,
    git_branch TEXT,
    error_message TEXT
);

-- Track individual tasks within a run
CREATE TABLE IF NOT EXISTS meta.lineage (
    lineage_id UUID PRIMARY KEY,
    run_id UUID REFERENCES meta.runs (run_id),
    task_name TEXT NOT NULL,         -- e.g., 'ingest_api', 'stage', 'features/train_val'
    status TEXT NOT NULL,            -- PENDING, RUNNING, COMPLETED, FAILED
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    inputs_hash TEXT,                -- To check if we need to re-run
    outputs_path TEXT,
    error_message TEXT,
    metadata JSON                    -- Any extra task-specific info
);
