from __future__ import annotations

from typing import Any

from backend.app.core.time import utc_now
from backend.app.db.database import transaction


def load_route_facts_payload(shape: str) -> dict[str, dict[str, Any]]:
    """Every counted fact of one route shape, by fact."""
    with transaction() as connection:
        rows = connection.execute(
            "SELECT fact, hits, misses, value, updated_at FROM learned_routes WHERE shape = ?",
            (shape,),
        ).fetchall()
    return {
        str(row["fact"]): {
            "hits": int(row["hits"] or 0),
            "misses": int(row["misses"] or 0),
            "value": str(row["value"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in rows
    }


def record_route_observation(shape: str, fact: str, *, hit: bool, value: str = "", window: int = 0) -> None:
    """Count one observation of ``fact`` on ``shape``.

    With ``window``, both counts halve once together they reach it, so a route that
    changes how it behaves outweighs its past within a few observations. A non-empty
    ``value`` replaces the stored one.
    """
    shape = str(shape or "").strip()
    fact = str(fact or "").strip()
    if not shape or not fact:
        return
    with transaction() as connection:
        connection.execute(
            """
            INSERT INTO learned_routes (shape, fact, hits, misses, value, updated_at)
            VALUES (:shape, :fact, :hit, :miss, :value, :now)
            ON CONFLICT(shape, fact) DO UPDATE SET
                hits = (CASE WHEN :window > 0 AND hits + misses >= :window THEN hits / 2 ELSE hits END)
                    + excluded.hits,
                misses = (CASE WHEN :window > 0 AND hits + misses >= :window THEN misses / 2 ELSE misses END)
                    + excluded.misses,
                value = CASE WHEN excluded.value != '' THEN excluded.value ELSE value END,
                updated_at = excluded.updated_at
            """,
            {
                "shape": shape,
                "fact": fact,
                "hit": int(hit),
                "miss": int(not hit),
                "value": str(value or ""),
                "now": utc_now(),
                "window": int(window),
            },
        )
