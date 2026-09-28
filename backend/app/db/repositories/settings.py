from __future__ import annotations

from typing import Any

from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import _decode, _encode


def _load_app_value(key: str) -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    payload = _decode(row["value"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def _save_app_value(key: str, payload: dict[str, Any]) -> None:
    now = utc_now()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            (key, _encode(payload if isinstance(payload, dict) else {}), now),
        )


def load_settings_payload() -> dict[str, Any]:
    return _load_app_value("app")


def save_settings_payload(payload: dict[str, Any]) -> None:
    _save_app_value("app", payload)


def load_naming_snapshots_payload() -> dict[str, Any]:
    """Per source, the naming its files were written with before an unapplied change."""
    return _load_app_value("naming")


def save_naming_snapshots_payload(payload: dict[str, Any]) -> None:
    _save_app_value("naming", payload)
