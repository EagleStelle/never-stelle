from __future__ import annotations

from typing import Any

from backend.app.core.time import utc_now
from backend.app.db.database import transaction


def load_learned_redirects_payload() -> dict[str, dict[str, Any]]:
    with transaction() as connection:
        rows = connection.execute(
            "SELECT shape, expands, direct, updated_at FROM learned_redirects"
        ).fetchall()
    loaded: dict[str, dict[str, Any]] = {}
    for row in rows:
        shape = str(row["shape"] or "").strip()
        if shape:
            loaded[shape] = {
                "expands": int(row["expands"] or 0),
                "direct": int(row["direct"] or 0),
                "updated_at": str(row["updated_at"] or ""),
            }
    return loaded


def record_redirect_observation(shape: str, *, expands: bool) -> None:
    """Count one conclusive probe of ``shape``.

    Counted, not overwritten: a route that answers differently once (a region redirect,
    a consent interstitial that happened to resolve) should move the balance rather than
    replace what every previous paste of the same shape reported.
    """
    shape = str(shape or "").strip()
    if not shape:
        return
    # One of two literals, never caller input.
    column = "expands" if expands else "direct"
    now = utc_now()
    with transaction() as connection:
        connection.execute(
            f"""
            INSERT INTO learned_redirects (shape, {column}, created_at, updated_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(shape) DO UPDATE SET
                {column} = {column} + 1,
                updated_at = excluded.updated_at
            """,
            (shape, now, now),
        )
