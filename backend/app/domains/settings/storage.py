from __future__ import annotations

from typing import Any

from backend.app.core.resolution import invalidate, resolved
from backend.app.db.repositories import load_settings_payload, save_settings_payload

# Every derivation below (profiles, fields, templates, cleaning) starts by reading
# this row, so a single operation used to decode it dozens of times.
SAVED_SETTINGS_KEY = "settings.saved"


def _read_saved_settings_file() -> dict[str, Any]:
    payload = load_settings_payload()
    return payload if isinstance(payload, dict) else {}


def load_saved_settings_file() -> dict[str, Any]:
    return resolved(SAVED_SETTINGS_KEY, _read_saved_settings_file)


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    save_settings_payload(payload if isinstance(payload, dict) else {})
    # A write inside a scope (PUT /settings, then rebuilding the response) must not
    # keep serving the pre-write snapshot.
    invalidate(SAVED_SETTINGS_KEY, "settings.", "core.")
    # Sole settings write path, so cached derivations can trust their entries
    # until this fires instead of re-reading the row per use.
    from .cookie_policy import invalidate_cookie_policies

    invalidate_cookie_policies()
