from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from backend.app.core.config import load_app_config
from backend.app.services.settings import (
    build_settings_response,
    clear_ytdlp_cookies_upload,
    get_effective_saved_settings,
    save_ytdlp_cookies_upload,
)
from backend.app.services.tasks import (
    clear_pending_tasks,
    count_tasks,
    counts_by_menu,
    fetch_tasks,
    queue_task,
    remove_pending_task,
    resolve_task_file,
)

router = APIRouter()


class SettingsPayload(BaseModel):
    site_locations: dict[str, str] = Field(default_factory=dict)
    template_settings: dict[str, str] = Field(default_factory=dict)
    source_profiles: list[dict[str, Any]] | dict[str, Any] = Field(default_factory=list)
    source_templates: dict[str, dict[str, str]] = Field(default_factory=dict)


class AddTaskPayload(BaseModel):
    url: str = ""
    site_locations: dict[str, str] = Field(default_factory=dict)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ui-config")
def ui_config() -> dict[str, Any]:
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    payload = build_settings_response(cfg, saved)
    payload.update(
        {
            "default_filename_template": saved.get("template_settings", {}).get("filename_template", ""),
            "default_folder_template": saved.get("template_settings", {}).get("folder_template", ""),
            "default_general_location": next(iter(saved.get("site_locations", {}).values()), ""),
        }
    )
    return payload


@router.get("/settings")
def get_settings() -> dict[str, Any]:
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.post("/settings")
def update_settings(payload: SettingsPayload) -> dict[str, Any]:
    from backend.app.services.settings import persist_settings

    cfg = load_app_config()
    saved = persist_settings(
        cfg,
        payload.site_locations,
        payload.template_settings,
        payload.source_profiles,
        payload.source_templates,
    )
    return build_settings_response(cfg, saved)


@router.post("/settings/ytdlp-cookies/{platform}")
async def upload_ytdlp_cookies(platform: str, file: UploadFile = File(...)) -> dict[str, Any]:
    try:
        await save_ytdlp_cookies_upload(file, platform)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.delete("/settings/ytdlp-cookies/{platform}")
def delete_ytdlp_cookies(platform: str) -> dict[str, Any]:
    clear_ytdlp_cookies_upload(platform)
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.get("/tasks")
def list_tasks() -> dict[str, Any]:
    tasks = fetch_tasks()
    return {
        "tasks": tasks,
        "counts": count_tasks(tasks),
        "counts_by_menu": counts_by_menu(tasks),
    }


@router.post("/tasks")
def add_task(payload: AddTaskPayload) -> dict[str, Any]:
    try:
        created, reused = queue_task(
            payload.url,
            payload.site_locations,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"created": created, "reused": reused}


@router.delete("/tasks/{task_id}", status_code=204, response_class=Response)
def delete_task(task_id: str) -> Response:
    try:
        remove_pending_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return Response(status_code=204)


@router.get("/tasks/{task_id}/file")
def download_task_file(task_id: str) -> FileResponse:
    try:
        path, filename = resolve_task_file(task_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(path, filename=filename, media_type="application/octet-stream")


@router.post("/tasks/clear-pending")
def clear_pending() -> dict[str, Any]:
    return clear_pending_tasks()
