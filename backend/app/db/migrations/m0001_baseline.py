"""Baseline schema, as it stood when the migration chain was introduced.

``IF NOT EXISTS`` throughout, so an install that predates the chain (version 0 with
the tables already built) takes this as a no-op and continues at the next version.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS download_tasks (
    id                  TEXT PRIMARY KEY,
    source_url          TEXT NOT NULL DEFAULT '',
    source_key          TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','running','completed','failed')),
    progress_pct        REAL NOT NULL DEFAULT 0,
    engine              TEXT NOT NULL DEFAULT '',
    creator             TEXT NOT NULL DEFAULT '',
    title               TEXT NOT NULL DEFAULT '',
    media_id            TEXT NOT NULL DEFAULT '',
    resolved_full_path  TEXT NOT NULL DEFAULT '',
    resolved_folder     TEXT NOT NULL DEFAULT '',
    resolved_filename   TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    output_dir          TEXT NOT NULL DEFAULT '',
    output_template     TEXT NOT NULL DEFAULT '',
    folder_template     TEXT NOT NULL DEFAULT '',
    filename_template   TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    encoding            TEXT NOT NULL DEFAULT '{}',
    last_log_lines      TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created
    ON download_tasks(status, created_at, id);

CREATE TABLE IF NOT EXISTS download_history (
    id                  TEXT PRIMARY KEY,
    source_url          TEXT    NOT NULL DEFAULT '',
    source_key          TEXT    NOT NULL DEFAULT '',
    engine              TEXT    NOT NULL DEFAULT '',
    creator             TEXT    NOT NULL DEFAULT '',
    title               TEXT    NOT NULL DEFAULT '',
    media_id            TEXT    NOT NULL DEFAULT '',
    resolved_full_path  TEXT    NOT NULL DEFAULT '',
    resolved_path_key   TEXT    NOT NULL DEFAULT '',
    resolved_folder     TEXT    NOT NULL DEFAULT '',
    resolved_filename   TEXT    NOT NULL DEFAULT '',
    file_size           INTEGER NOT NULL DEFAULT 0,
    scan_mtime_ns       INTEGER NOT NULL DEFAULT 0,
    scan_revision       TEXT    NOT NULL DEFAULT '',
    folder_template     TEXT    NOT NULL DEFAULT '',
    filename_template   TEXT    NOT NULL DEFAULT '',
    needs_resolve       INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL,
    encoding            TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_history_order
    ON download_history(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_history_source_order
    ON download_history(source_key, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_history_path
    ON download_history(resolved_path_key);
CREATE INDEX IF NOT EXISTS idx_history_media_id
    ON download_history(media_id);
CREATE INDEX IF NOT EXISTS idx_history_needs_resolve
    ON download_history(created_at DESC, id DESC) WHERE needs_resolve = 1;

CREATE TABLE IF NOT EXISTS download_enrichment_jobs (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL DEFAULT 'completion',
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','running','failed')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT NOT NULL DEFAULT '',
    payload     TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_enrichment_status_created
    ON download_enrichment_jobs(status, created_at, id);

CREATE TABLE IF NOT EXISTS learned_formats (
    source_key TEXT PRIMARY KEY,
    host TEXT NOT NULL DEFAULT '',
    templates TEXT NOT NULL DEFAULT '',
    id_min INTEGER NOT NULL DEFAULT 0,
    id_max INTEGER NOT NULL DEFAULT 0,
    id_classes TEXT NOT NULL DEFAULT '',
    samples INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_learned_formats_host ON learned_formats(host);

-- Which past downloads have already been folded into learned_formats. Learning is
-- cumulative and already persisted, so re-teaching the same download on every scan
-- is pure duplicated work; this records what is done so a scan only reads what is new.
CREATE TABLE IF NOT EXISTS seeded_downloads (
    task_id TEXT PRIMARY KEY,
    seeded_at TEXT NOT NULL
);

-- Renames in flight. A rename touches the disk and the history row separately, so a
-- crash between the two would leave a row pointing at a path that no longer exists
-- (which the next scan would treat as a missing file and delete). A row is written
-- here before the disk is touched and cleared once the history row agrees with it,
-- so anything still present on the next scan is a rename to finish or undo.
CREATE TABLE IF NOT EXISTS rename_journal (
    task_id    TEXT PRIMARY KEY,
    old_path   TEXT NOT NULL,
    new_path   TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cookies (
    id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL DEFAULT '',
    filename TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL DEFAULT 0,
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_cookies_source_key ON source_cookies(source_key);
"""


def upgrade(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
