from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from backend.app.core.config import load_app_config
from backend.app.services.auth import (
    AuthError,
    InvalidCredentials,
    authenticate_user,
    clear_session_cookie,
    create_session_token,
    current_auth_session,
    set_session_cookie,
    update_auth_credentials,
)
from backend.app.services.settings import (
    build_settings_response,
    clear_ytdlp_cookies_upload,
    ensure_source_profile_for_url,
    get_effective_saved_settings,
    save_ytdlp_cookies_upload,
)
from backend.app.services.tasks import (
    build_counts,
    cancel_task,
    clear_pending_tasks,
    fetch_active_tasks,
    fetch_history_page,
    probe_url,
    queue_task,
    remove_pending_task,
    resolve_task_file,
    retry_task,
    scan_media_library,
    set_task_source,
)

router = APIRouter()


class LoginPayload(BaseModel):
    username: str = ""
    password: str = ""


class CredentialsPayload(BaseModel):
    username: str = ""
    current_password: str = ""
    new_password: str = ""


class SettingsPayload(BaseModel):
    site_locations: dict[str, str] = Field(default_factory=dict)
    template_settings: dict[str, str] = Field(default_factory=dict)
    source_profiles: list[dict[str, Any]] | dict[str, Any] = Field(default_factory=list)
    source_templates: dict[str, dict[str, str]] = Field(default_factory=dict)
    default_quality: dict[str, str] = Field(default_factory=dict)


class AddTaskPayload(BaseModel):
    url: str = ""
    urls: list[str] = Field(default_factory=list)
    site_locations: dict[str, str] | None = None
    template_settings: dict[str, str] | None = None
    source_profiles: list[dict[str, Any]] | dict[str, Any] | None = None
    source_templates: dict[str, dict[str, str]] | None = None
    quality: dict[str, str] | None = None


class ProbePayload(BaseModel):
    url: str = ""


class SetSourcePayload(BaseModel):
    source_key: str = ""


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/auth/session")
def auth_session(request: Request) -> dict[str, Any]:
    session = current_auth_session(request)
    if not session:
        return {"authenticated": False, "username": ""}
    return {"authenticated": True, "username": session["username"]}


@router.post("/auth/login")
def auth_login(payload: LoginPayload, response: Response) -> dict[str, Any]:
    try:
        auth = authenticate_user(payload.username, payload.password)
    except InvalidCredentials as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    set_session_cookie(response, create_session_token(auth))
    return {"authenticated": True, "username": auth["username"]}


@router.post("/auth/logout")
def auth_logout(response: Response) -> dict[str, Any]:
    clear_session_cookie(response)
    return {"authenticated": False, "username": ""}


@router.post("/auth/credentials")
def auth_credentials(payload: CredentialsPayload, response: Response) -> dict[str, Any]:
    try:
        auth = update_auth_credentials(
            payload.current_password,
            payload.username,
            payload.new_password,
        )
    except InvalidCredentials as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    set_session_cookie(response, create_session_token(auth))
    return {"authenticated": True, "username": auth["username"]}


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
        payload.default_quality,
    )
    return build_settings_response(cfg, saved)


@router.post("/settings/ytdlp-cookies/{platform}")
async def upload_ytdlp_cookies(
    platform: str,
    file: UploadFile = File(...),
    source: str = Form(""),
) -> dict[str, Any]:
    # `source` (a pasted link/host) lets the user attach cookies to any site
    # with no predefined profile; it wins over the path param when present.
    target = ensure_source_profile_for_url(source) if source.strip() else platform
    try:
        await save_ytdlp_cookies_upload(file, target)
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
    # Active rows (queued/running/failed) plus SQL-cheap counts. Completed downloads
    # come from /history so this poll stays light regardless of history size.
    return {"tasks": fetch_active_tasks(), **build_counts()}


@router.get("/history")
def list_history(offset: int = 0, limit: int = 30, source_key: str = "", search: str = "") -> dict[str, Any]:
    limit = max(1, min(200, limit))
    offset = max(0, offset)
    return fetch_history_page(offset, limit, source_key, search)


@router.post("/scan")
def scan_media() -> dict[str, int]:
    try:
        return scan_media_library()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/tasks/probe")
def probe_task(payload: ProbePayload) -> dict[str, Any]:
    try:
        return probe_url(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/tasks")
def add_task(payload: AddTaskPayload) -> dict[str, Any]:
    targets = [url for url in (payload.urls or [payload.url]) if url and url.strip()]
    if not targets:
        raise HTTPException(status_code=400, detail="Paste a URL first.")

    created: list[dict[str, Any]] = []
    reused = False
    errors: list[str] = []
    for target in targets:
        try:
            task_created, task_reused = queue_task(
                target,
                payload.site_locations,
                payload.template_settings,
                payload.source_profiles,
                payload.source_templates,
                payload.quality,
            )
        except Exception as exc:
            errors.append(str(exc))
            continue
        created.extend(task_created)
        reused = reused or task_reused
    if not created and errors:
        raise HTTPException(status_code=400, detail=errors[0])
    return {"created": created, "reused": reused}


@router.delete("/tasks/{task_id}", status_code=204, response_class=Response)
def delete_task(task_id: str) -> Response:
    try:
        remove_pending_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/tasks/{task_id}/cancel", status_code=204, response_class=Response)
def cancel_task_route(task_id: str) -> Response:
    try:
        cancel_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/tasks/{task_id}/retry", status_code=204, response_class=Response)
def retry_task_route(task_id: str) -> Response:
    try:
        retry_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204)


@router.patch("/tasks/{task_id}/source")
def update_task_source(task_id: str, payload: SetSourcePayload) -> dict[str, str]:
    if not payload.source_key.strip():
        raise HTTPException(status_code=400, detail="Choose or type a source.")
    try:
        source_key = set_task_source(task_id, payload.source_key)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"source_key": source_key}


@router.get("/tasks/{task_id}/file")
def download_task_file(task_id: str) -> FileResponse:
    try:
        path, filename, cleanup_path = resolve_task_file(task_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    background = BackgroundTask(cleanup_path.unlink, missing_ok=True) if cleanup_path else None
    media_type = "application/zip" if path.suffix.lower() == ".zip" else "application/octet-stream"
    return FileResponse(path, filename=filename, media_type=media_type, background=background)


@router.post("/tasks/clear-pending")
def clear_pending() -> dict[str, Any]:
    return clear_pending_tasks()
