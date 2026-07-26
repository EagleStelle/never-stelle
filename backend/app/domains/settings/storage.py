from __future__ import annotations

from typing import Any

from backend.app.db.repositories import load_settings_payload, save_settings_payload


def load_saved_settings_file() -> dict[str, Any]:
    payload = load_settings_payload()
    return payload if isinstance(payload, dict) else {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_settings_payload(payload if isinstance(payload, dict) else {})
