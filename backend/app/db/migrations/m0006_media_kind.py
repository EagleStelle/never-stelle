"""Download rows carry their media kind.

The image/video tallies behind the source chips derived the kind in SQL, which meant a
LOWER/LIKE pass over every history row on every poll of the downloads page, and a
second copy of the rule to keep in step with the Python one. The kind only changes
when a row is written, so it becomes a stored column filled by ``media_kind_for`` and
the tallies read an index alone.
"""

from __future__ import annotations

import sqlite3

from backend.app.domains.downloads.constants import media_kind_for

from . import table_exists

_TABLES = ("download_tasks", "download_history")


def upgrade(connection: sqlite3.Connection) -> None:
    for table in _TABLES:
        if not table_exists(connection, table):
            continue
        connection.execute(f"ALTER TABLE {table} ADD COLUMN media_kind TEXT NOT NULL DEFAULT ''")
        rows = connection.execute(f"SELECT id, resolved_filename, engine FROM {table}").fetchall()
        connection.executemany(
            f"UPDATE {table} SET media_kind = ? WHERE id = ?",
            [(media_kind_for(filename, engine), row_id) for row_id, filename, engine in rows],
        )
    if table_exists(connection, "download_history"):
        connection.execute("CREATE INDEX idx_history_source_media ON download_history(source_key, media_kind)")
