from __future__ import annotations

from typing import Any

from backend.app.db.database import transaction, utc_now
from backend.app.db.repositories.utils import _decode, _encode


def settings_revision() -> str:
    """Cheap marker of the saved-settings state; a primary-key lookup, no decode."""
    with transaction() as connection:
        row = connection.execute("SELECT updated_at FROM app_settings WHERE key = ?", ("app",)).fetchone()
    return str(row["updated_at"]) if row else ""


def load_settings_payload() -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute("SELECT value FROM app_settings WHERE key = ?", ("app",)).fetchone()
    payload = _decode(row["value"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def save_settings_payload(payload: dict[str, Any]) -> None:
    now = utc_now()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("app", _encode(payload if isinstance(payload, dict) else {}), now),
        )
