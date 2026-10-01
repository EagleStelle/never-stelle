"""One table for everything learned per link route.

Whether a route redirects used to live in ``learned_redirects``. A download also learns
which engine gets media on a route and whether a second read after it adds anything, and
all three are counts per route shape, so they share this table: each row is one fact of
one route, with its hits, misses and a learned value. Redirect answers move over as the
``redirect`` fact, a redirect counting as a hit and a direct answer as a miss.
"""

from __future__ import annotations

import sqlite3

from . import table_exists


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE learned_routes (
            shape      TEXT    NOT NULL,
            fact       TEXT    NOT NULL,
            hits       INTEGER NOT NULL DEFAULT 0,
            misses     INTEGER NOT NULL DEFAULT 0,
            value      TEXT    NOT NULL DEFAULT '',
            updated_at TEXT    NOT NULL,
            PRIMARY KEY (shape, fact)
        ) WITHOUT ROWID
        """
    )
    if table_exists(connection, "learned_redirects"):
        connection.execute(
            """
            INSERT INTO learned_routes (shape, fact, hits, misses, updated_at)
            SELECT shape, 'redirect', expands, direct, updated_at FROM learned_redirects
            """
        )
        connection.execute("DROP TABLE learned_redirects")
