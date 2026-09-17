"""Trackers remember where their pages are on their link.

A source's page rows are saved by name, and each tracked link names those pages its own way.
``feeds`` holds ``{"pages": {...}, "ended": [...]}``: the page each row turned out to be on this
link, so later checks go straight to it, and the pages scrolled to their end.
"""

from __future__ import annotations

import sqlite3

from . import table_exists


def upgrade(connection: sqlite3.Connection) -> None:
    if table_exists(connection, "trackers"):
        connection.execute("ALTER TABLE trackers ADD COLUMN feeds TEXT NOT NULL DEFAULT '{}'")
