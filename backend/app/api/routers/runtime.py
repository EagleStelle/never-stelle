from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from backend.app.api.deps import require_authenticated_session
from backend.app.core.config import get_default_general_location, load_app_config
from backend.app.domains.settings import build_settings_response, get_effective_saved_settings

router = APIRouter(tags=["runtime"], dependencies=[Depends(require_authenticated_session)])


@router.get("/runtime-settings")
def runtime_settings() -> dict[str, Any]:
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    payload = build_settings_response(cfg, saved)
    payload.update(
        {
            "default_filename_template": saved.get("template_settings", {}).get("filename_template", ""),
            "default_folder_template": saved.get("template_settings", {}).get("folder_template", ""),
            "default_general_location": get_default_general_location(cfg),
        }
    )
    return payload

