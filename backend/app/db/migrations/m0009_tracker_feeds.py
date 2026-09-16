"""Trackers remember which of their link's pages list items.

A check used to load the tracked link and every tab it links, scrolling each, although most
tabs (people, about pages) never list an item and each one cost a page load. ``feeds`` holds
``{"urls": [...], "explored_at": "..."}``: the pages that listed items, most productive first,
and when every page was last visited to find them. Later checks start with those pages.
"""

from __future__ import annotations

import sqlite3

from . import table_exists


def upgrade(connection: sqlite3.Connection) -> None:
    if table_exists(connection, "trackers"):
        connection.execute("ALTER TABLE trackers ADD COLUMN feeds TEXT NOT NULL DEFAULT '{}'")
