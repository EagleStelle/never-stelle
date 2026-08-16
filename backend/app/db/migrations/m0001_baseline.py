"""Baseline schema, as it stood when the migration chain was introduced.

An install that predates the chain sits at version 0 with tables already built, but
not necessarily on this shape: the pre-chain code only ever ran ``CREATE TABLE IF NOT
EXISTS``, so every column added after that database was first created is missing from
it. This migration therefore declares the schema and then reconciles what it finds
against it, rebuilding any table whose columns disagree.
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

# Columns that were renamed rather than introduced, so a rebuild carries the data over
# instead of resetting it to a default. download_history once stamped a single
# completed_at; both of the timestamps that replaced it start from that value.
_RENAMED_FROM = {
    "download_history": {"created_at": "completed_at", "updated_at": "completed_at"},
}


class _Column:
    __slots__ = ("name", "type", "required")

    def __init__(self, name: str, declared_type: str, notnull: int, default: str | None) -> None:
        self.name = name
        self.type = declared_type
        self.required = bool(notnull) and default is None


def _declared() -> tuple[dict[str, tuple[str, list[_Column]]], dict[str, str]]:
    """The schema above, read back from SQLite rather than parsed by hand.

    Returns tables as ``name -> (create statement, columns)`` and indexes as
    ``name -> create statement``.
    """
    reference = sqlite3.connect(":memory:")
    try:
        reference.executescript(SCHEMA)
        tables: dict[str, tuple[str, list[_Column]]] = {}
        indexes: dict[str, str] = {}
        objects = reference.execute(
            "SELECT name, type, sql FROM sqlite_master WHERE sql IS NOT NULL"
        ).fetchall()
        for name, kind, sql in objects:
            if kind == "table":
                tables[name] = (sql, _columns(reference, name))
            elif kind == "index":
                indexes[name] = sql
        return tables, indexes
    finally:
        reference.close()


def _columns(connection: sqlite3.Connection, table: str) -> list[_Column]:
    rows = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [_Column(row[1], row[2], row[3], row[4]) for row in rows]


def _placeholder(declared_type: str) -> str:
    """A literal for a NOT NULL column the old table has nothing to fill from."""
    kind = declared_type.upper()
    if "BLOB" in kind:
        return "x''"
    if any(marker in kind for marker in ("INT", "REAL", "FLOA", "DOUB", "NUM")):
        return "0"
    return "''"


def _rebuild(connection: sqlite3.Connection, table: str, create_sql: str, want: list[_Column]) -> None:
    """Move the table onto the declared shape, keeping every column both shapes have."""
    have = {column.name for column in _columns(connection, table)}
    renames = _RENAMED_FROM.get(table, {})
    targets: list[str] = []
    sources: list[str] = []
    for column in want:
        source = column.name if column.name in have else renames.get(column.name)
        if source not in have:
            # Nothing to copy: a NOT NULL column without a default still needs a value.
            if not column.required:
                continue
            source = _placeholder(column.type)
        else:
            source = f'"{source}"'
        targets.append(f'"{column.name}"')
        sources.append(source)

    connection.execute(f'ALTER TABLE "{table}" RENAME TO "{table}__old"')
    connection.execute(create_sql)
    if targets:
        connection.execute(
            f'INSERT INTO "{table}" ({", ".join(targets)})'
            f' SELECT {", ".join(sources)} FROM "{table}__old"'
        )
    connection.execute(f'DROP TABLE "{table}__old"')


def _reconcile_indexes(connection: sqlite3.Connection, declared: dict[str, str]) -> None:
    """Drop indexes the schema no longer declares, create the ones it does."""
    live = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL"
        ).fetchall()
    }
    for name, sql in live.items():
        if declared.get(name) != sql:
            connection.execute(f'DROP INDEX "{name}"')
    for name, sql in declared.items():
        if live.get(name) != sql:
            connection.execute(sql)


def upgrade(connection: sqlite3.Connection) -> None:
    tables, indexes = _declared()
    for table, (create_sql, want) in tables.items():
        have = _columns(connection, table)
        if not have:
            connection.execute(create_sql)
        elif [column.name for column in have] != [column.name for column in want]:
            _rebuild(connection, table, create_sql, want)
    _reconcile_indexes(connection, indexes)
