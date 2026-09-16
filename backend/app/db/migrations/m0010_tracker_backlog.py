"""Trackers keep the item links a scroll found until a check lists them.

A page that only grows as it is scrolled used to be scrolled again from its top by every check,
past everything earlier checks had handled, to reach one more batch. ``tracker_backlog`` holds
what a scroll found and no check has handled yet, so a later check lists those without scrolling
and only scrolls far enough to find what was posted since.

``tracker_entries`` cannot hold them: its rows are handled entries the tracker counts and links
downloads to, keyed without an order. A backlog row is removed once its entry is recorded.
``found_at`` is the walk that found the link and ``position`` its place in that walk, so the
newest walk's links come first in page order; ``attempts`` counts checks that could not read it.
"""

from __future__ import annotations

import sqlite3


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE tracker_backlog (
            tracker_id TEXT    NOT NULL,
            entry_key  TEXT    NOT NULL,
            entry_url  TEXT    NOT NULL,
            found_at   TEXT    NOT NULL,
            position   INTEGER NOT NULL,
            attempts   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tracker_id, entry_key)
        ) WITHOUT ROWID
        """
    )
