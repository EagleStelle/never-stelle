from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from backend.app.core.config import discover_volume_roots, load_app_config
from backend.app.services.settings import (
    build_settings_response,
    clear_ytdlp_cookies_upload,
    get_effective_saved_settings,
    save_ytdlp_cookies_upload,
)
from backend.app.services.tasks import (
    clear_completed_tasks,
    clear_pending_tasks,
    count_tasks,
    counts_by_menu,
    fetch_tasks,
    hide_done_task,
    mark_task_delivered,
    queue_task,
    remove_pending_task,
    resolve_task_file,
)


router = APIRouter()


class SettingsPayload(BaseModel):
    site_locations: dict[str, str] = Field(default_factory=dict)
    save_mode: str = "nas"
    template_settings: dict[str, str] = Field(default_factory=dict)


class AddTaskPayload(BaseModel):
    url: str = ""
    site_locations: dict[str, str] = Field(default_factory=dict)
    save_mode: str = "nas"
    client_tab_id: str = ""


class DeliveredPayload(BaseModel):
    client_tab_id: str = ""


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
        payload.save_mode,
        payload.template_settings,
    )
    return build_settings_response(cfg, saved)


@router.post("/settings/instagram-ytdlp-cookies")
async def upload_instagram_ytdlp_cookies(file: UploadFile = File(...)) -> dict[str, Any]:
    try:
        await save_ytdlp_cookies_upload(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.delete("/settings/instagram-ytdlp-cookies")
def delete_instagram_ytdlp_cookies() -> dict[str, Any]:
    clear_ytdlp_cookies_upload()
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
            payload.save_mode,
            payload.client_tab_id,
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


@router.post("/tasks/{task_id}/hide", status_code=204, response_class=Response)
def hide_task(task_id: str) -> Response:
    try:
        hide_done_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/tasks/{task_id}/delivered")
def mark_delivered(task_id: str, payload: DeliveredPayload) -> dict[str, Any]:
    try:
        return mark_task_delivered(task_id, payload.client_tab_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@router.post("/tasks/clear-completed")
def clear_completed() -> dict[str, Any]:
    return clear_completed_tasks()


@router.post("/cleanup-nfo")
def cleanup_nfo() -> dict[str, Any]:
    roots = discover_volume_roots()
    if not roots:
        raise HTTPException(status_code=500, detail="No accessible volume roots are configured.")
    deleted = 0
    errors: list[dict[str, str]] = []
    for root_value in roots:
        root = Path(root_value)
        if not root.exists():
            continue
        for path in root.rglob("*.nfo"):
            try:
                path.unlink()
                deleted += 1
            except Exception as exc:
                errors.append({"file": str(path), "error": str(exc)})
    return {"deleted": deleted, "errors": errors}
