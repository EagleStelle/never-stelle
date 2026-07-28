from __future__ import annotations

from typing import Any


def template_columns(template_settings: dict[str, str] | None) -> dict[str, str]:
    settings = template_settings if isinstance(template_settings, dict) else {}
    return {
        "folder_template": str(settings.get("folder_template") or "").strip(),
        "filename_template": str(settings.get("filename_template") or "").strip(),
    }


def template_settings_from_columns(payload: dict[str, Any]) -> dict[str, str] | None:
    folder_template = str(payload.get("folder_template") or "").strip()
    filename_template = str(payload.get("filename_template") or "").strip()
    if not (folder_template or filename_template):
        return None
    return {"folder_template": folder_template, "filename_template": filename_template}
