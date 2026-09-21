"""Formats are learned once, when a download succeeds, so nothing re-learns them from history.

``seeded_downloads`` recorded which past downloads a library scan had already folded into
``learned_formats``. The scan no longer learns: a download teaches its format as it completes,
and a format the user deletes stays deleted instead of coming back from history on the next
scan. The ledger has no reader left.
"""

from __future__ import annotations

import sqlite3


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute("DROP TABLE IF EXISTS seeded_downloads")
