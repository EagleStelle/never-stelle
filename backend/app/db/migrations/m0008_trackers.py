"""Trackers: saved collection links checked on a schedule.

``trackers`` holds one row per followed link. ``tracker_entries`` remembers every entry a
tracker has listed, so an entry is queued once even after its download is removed or its
file deleted. ``download_id`` is the id ``queue_task`` returned: the task row while active,
the history row with the same id once complete. It is a plain column rather than a foreign
key because that row moves between the two tables on completion.
"""

from __future__ import annotations

import sqlite3


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE trackers (
            id               TEXT PRIMARY KEY,
            source_url       TEXT    NOT NULL DEFAULT '',
            name             TEXT    NOT NULL DEFAULT '',
            enabled          INTEGER NOT NULL DEFAULT 0,
            interval_seconds INTEGER NOT NULL DEFAULT 21600,
            quality          TEXT    NOT NULL DEFAULT '{}',
            post_processing  TEXT    NOT NULL DEFAULT '{}',
            next_check_at    TEXT    NOT NULL DEFAULT '',
            last_checked_at  TEXT    NOT NULL DEFAULT '',
            last_success_at  TEXT    NOT NULL DEFAULT '',
            last_error       TEXT    NOT NULL DEFAULT '',
            checking_at      TEXT    NOT NULL DEFAULT '',
            created_at       TEXT    NOT NULL,
            updated_at       TEXT    NOT NULL
        )
        """
    )
    connection.execute("CREATE INDEX idx_trackers_due ON trackers(enabled, next_check_at)")
    connection.execute(
        """
        CREATE TABLE tracker_entries (
            tracker_id  TEXT NOT NULL,
            entry_key   TEXT NOT NULL,
            entry_url   TEXT NOT NULL DEFAULT '',
            download_id TEXT NOT NULL DEFAULT '',
            seen_at     TEXT NOT NULL,
            PRIMARY KEY (tracker_id, entry_key)
        ) WITHOUT ROWID
        """
    )
    connection.execute("CREATE INDEX idx_tracker_entries_download ON tracker_entries(download_id)")
