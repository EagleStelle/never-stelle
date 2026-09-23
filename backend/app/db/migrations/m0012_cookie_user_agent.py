"""Cookie jars remember the browser they came from.

A jar is one logged-in session, and a site ties that session to the browser that signed
in. Every request the app sends with a jar presents that browser, so each jar keeps the
user agent of the browser that uploaded it. Jars stored before this have none and present
the default browser.
"""

from __future__ import annotations

import sqlite3

from . import table_exists


def upgrade(connection: sqlite3.Connection) -> None:
    if table_exists(connection, "source_cookies"):
        connection.execute("ALTER TABLE source_cookies ADD COLUMN user_agent TEXT NOT NULL DEFAULT ''")
