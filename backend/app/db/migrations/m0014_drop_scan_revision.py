"""History rows stop recording the rules a scan resolved them by.

A scan now derives a disk row again only when its file changes or the row still lacks
a source or link, so the marker of the settings and learned formats has no reader.
"""

from __future__ import annotations

import sqlite3

from . import table_exists


def upgrade(connection: sqlite3.Connection) -> None:
    if table_exists(connection, "download_history"):
        connection.execute("ALTER TABLE download_history DROP COLUMN scan_revision")
