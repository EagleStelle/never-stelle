from __future__ import annotations

from typing import Any

from backend.app.domains.settings import TEMPLATE_KEYS


def template_row_fields(template_settings: dict[str, str] | None) -> dict[str, str]:
    """The templates a task or history row records, trimmed."""
    settings = template_settings if isinstance(template_settings, dict) else {}
    return {key: str(settings.get(key) or "").strip() for key in TEMPLATE_KEYS}


def template_settings_from_row(payload: dict[str, Any]) -> dict[str, str] | None:
    """The templates a row was written with, or None when it carries none."""
    fields = template_row_fields(payload)
    return fields if any(fields.values()) else None
