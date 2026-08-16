"""Learn which URL routes redirect, replacing the hardcoded platform domain lists.

Whether a link has to be followed before it can be queued used to be a hand-maintained
set of platform domains. It is an observation the resolver already makes and threw away:
following a link reports whether the URL changed. This table remembers that answer per
route shape, so a route costs a round trip a couple of times and every later paste of the
same shape skips it.
"""

from __future__ import annotations

import sqlite3


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE learned_redirects (
            shape      TEXT PRIMARY KEY,
            expands    INTEGER NOT NULL DEFAULT 0,
            direct     INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
