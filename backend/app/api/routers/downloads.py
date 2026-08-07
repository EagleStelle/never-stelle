from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from backend.app.api.deps import require_authenticated_session
from backend.app.api.responses import local_download_response
from backend.app.api.schemas.downloads import AddDownloadPayload, ProbePayload, SetSourcePayload
from backend.app.domains.downloads import operations, probe, serializers
from backend.app.integrations.swaratelle import client as swaratelle

router = APIRouter(
    prefix="/downloads",
    tags=["downloads"],
    dependencies=[Depends(require_authenticated_session)],
)


@router.get("")
def list_downloads() -> dict[str, Any]:
    return {
        "tasks": serializers.fetch_active_tasks(),
        **serializers.build_counts(),
        # Rides the poll the client already runs, so no second endpoint or timer.
        **serializers.library_activity(),
    }


@router.post("")
def add_download(payload: AddDownloadPayload) -> dict[str, Any]:
    targets = [url for url in (payload.urls or [payload.url]) if url and url.strip()]
    if not targets:
        raise HTTPException(status_code=400, detail="Paste a URL first.")

    created: list[dict[str, Any]] = []
    reused = False
    errors: list[str] = []
    for target in targets:
        try:
            task_created, task_reused = operations.queue_task(
                target,
                payload.source_locations,
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


@router.post("/probe")
def probe_download(payload: ProbePayload) -> dict[str, Any]:
    try:
        return probe.probe_url(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/history")
def list_history(cursor: str = "", limit: int = 50, source_key: str = "", q: str = "") -> dict[str, Any]:
    limit = max(1, min(100, limit))
    try:
        return serializers.fetch_history_page(cursor, limit, source_key, q)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/clear-pending")
def clear_pending() -> dict[str, Any]:
    return operations.clear_pending_tasks()


@router.delete("/{task_id}", status_code=204, response_class=Response)
def delete_download(task_id: str) -> Response:
    try:
        operations.remove_pending_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except swaratelle.SwaratelleError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/{task_id}/cancel", status_code=204, response_class=Response)
def cancel_download(task_id: str) -> Response:
    try:
        operations.cancel_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except swaratelle.SwaratelleError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/{task_id}/retry", status_code=204, response_class=Response)
def retry_download(task_id: str) -> Response:
    try:
        operations.retry_task(task_id)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204)


@router.patch("/{task_id}/source")
def update_download_source(task_id: str, payload: SetSourcePayload) -> dict[str, str]:
    if not payload.source_key.strip():
        raise HTTPException(status_code=400, detail="Choose or type a source.")
    try:
        source_key = operations.set_task_source(task_id, payload.source_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"source_key": source_key}


@router.get("/{task_id}")
def get_download(task_id: str) -> dict[str, Any]:
    try:
        return operations.get_task(task_id)
    except swaratelle.SwaratelleError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{task_id}/file")
def download_file(request: Request, task_id: str) -> Response:
    if swaratelle.is_swaratelle_task_id(task_id):
        try:
            download = swaratelle.open_download_file(task_id)
        except swaratelle.SwaratelleError as exc:
            raise HTTPException(status_code=exc.status_code or 502, detail=str(exc)) from exc
        return StreamingResponse(
            download.iter_bytes(),
            media_type=download.media_type,
            headers=download.headers,
            background=BackgroundTask(download.close),
        )

    try:
        path, filename, cleanup_path = operations.resolve_task_file(task_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return local_download_response(request, path, filename, cleanup_path)
