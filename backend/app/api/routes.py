from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from backend.app.core.config import load_app_config
from backend.app.services import swaratelle
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
    source_scrape_rules: dict[str, Any] = Field(default_factory=dict)
    source_token_roles: dict[str, Any] = Field(default_factory=dict)


class ScrapeTestPayload(BaseModel):
    url: str = ""
    source_key: str = ""
    rules: list[dict[str, Any]] = Field(default_factory=list)


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
        payload.source_scrape_rules,
        payload.source_token_roles,
    )
    return build_settings_response(cfg, saved)


@router.post("/settings/scrape-test")
def scrape_test(payload: ScrapeTestPayload) -> dict[str, Any]:
    # Dry-run the user's rules against a live page so the UI can show what each
    # selector grabs before the rules are saved. Never persists anything.
    from backend.app.services.settings import detect_cookie_source
    from backend.app.services.tasks.enrich import fetch_html, normalize_scrape_rule, scrape_tokens

    url = payload.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="Paste a sample URL first.")
    rules = [rule for rule in (normalize_scrape_rule(item) for item in payload.rules) if rule]
    cookie_key = payload.source_key.strip() or detect_cookie_source(url)
    html = fetch_html(url, cookie_key)
    if not html:
        return {"fetched": False, "results": [], "detail": "Could not fetch that page."}
    tokens = scrape_tokens(html, rules)
    results = [
        {"token": rule["token"], "value": tokens.get(rule["token"], ""), "matched": rule["token"] in tokens}
        for rule in rules
    ]
    return {"fetched": True, "results": results}


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
    try:
        clear_ytdlp_cookies_upload(platform)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.get("/tasks")
def list_tasks() -> dict[str, Any]:
    from backend.app.services.tasks.serializers import build_counts, fetch_active_tasks

    # Active rows (queued/running/failed) plus SQL-cheap counts. Completed downloads
    # come from /history so this poll stays light regardless of history size.
    return {"tasks": fetch_active_tasks(), **build_counts()}


@router.get("/history")
def list_history(cursor: str = "", limit: int = 50, source_key: str = "", q: str = "") -> dict[str, Any]:
    from backend.app.services.tasks.serializers import fetch_history_page

    limit = max(1, min(100, limit))
    try:
        return fetch_history_page(cursor, limit, source_key, q)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/scan")
def scan_media() -> dict[str, int]:
    from backend.app.services.tasks.scan import scan_media_library

    try:
        local = scan_media_library()
        external = swaratelle.scan_media_library()
        return {
            "checked": int(local.get("checked", 0)) + int(external.get("checked", 0)),
            "missing": int(local.get("missing", 0)) + int(external.get("missing", 0)),
            "added": int(local.get("added", 0)) + int(external.get("added", 0)),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/tasks/probe")
def probe_task(payload: ProbePayload) -> dict[str, Any]:
    from backend.app.services.tasks.probe import probe_url

    try:
        return probe_url(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/tasks")
def add_task(payload: AddTaskPayload) -> dict[str, Any]:
    from backend.app.services.tasks.operations import queue_task

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
    from backend.app.services.tasks.operations import remove_pending_task

    try:
        remove_pending_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except swaratelle.SwaratelleError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/tasks/{task_id}/cancel", status_code=204, response_class=Response)
def cancel_task_route(task_id: str) -> Response:
    from backend.app.services.tasks.operations import cancel_task

    try:
        cancel_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except swaratelle.SwaratelleError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/tasks/{task_id}/retry", status_code=204, response_class=Response)
def retry_task_route(task_id: str) -> Response:
    from backend.app.services.tasks.operations import retry_task

    try:
        retry_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204)


@router.patch("/tasks/{task_id}/source")
def update_task_source(task_id: str, payload: SetSourcePayload) -> dict[str, str]:
    from backend.app.services.tasks.operations import set_task_source

    if not payload.source_key.strip():
        raise HTTPException(status_code=400, detail="Choose or type a source.")
    try:
        source_key = set_task_source(task_id, payload.source_key)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"source_key": source_key}


@router.get("/tasks/{task_id}/file")
def download_task_file(task_id: str) -> FileResponse:
    from backend.app.services.tasks.operations import resolve_task_file

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
    from backend.app.services.tasks.operations import clear_pending_tasks

    return clear_pending_tasks()
