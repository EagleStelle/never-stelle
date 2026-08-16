"""Drop learning a test run wrote into a live database.

Until the suite opened its own database per test, a scan test wrote its fixture URLs
into the running install, and the full-replace save it used deleted every real source
on the way in. What is left behind is a source on the reserved ``.test`` domain
(RFC 2606, so never a real host) and a seeded-download id with no download behind it.
Both are removed; the next library scan relearns the real sources from history.
"""

from __future__ import annotations

import sqlite3


def upgrade(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        DELETE FROM learned_formats
        WHERE host = 'test'
           OR host LIKE '%.test'
           OR templates LIKE '%://%.test/%'
        """
    )
    # A seeded id only means "already folded into learning". Pointing at no download,
    # it just withholds a source the next scan would otherwise recover.
    connection.execute(
        """
        DELETE FROM seeded_downloads
        WHERE task_id NOT IN (SELECT id FROM download_history)
          AND task_id NOT IN (SELECT id FROM download_tasks)
        """
    )
