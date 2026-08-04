from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import require_api_access
from backend.app.db.repositories.integration import (
    PUBLIC_TABLES,
    integration_schema,
    list_download_records,
    list_integration_rows,
)
from backend.app.db.repositories.settings import load_settings_payload

router = APIRouter(
    prefix="/integration",
    tags=["integration"],
    dependencies=[Depends(require_api_access)],
)


@router.get("/manifest")
def integration_manifest() -> dict[str, Any]:
    return {
        **integration_schema(),
        "auth": {
            "session_cookie": True,
            "api_token_env": "NEVER_STELLE_API_TOKEN",
            "bearer_token_header": "Authorization: Bearer <token>",
            "api_key_header": "X-API-Key: <token>",
        },
        "endpoints": {
            "downloads": "/api/integration/downloads",
            "tables": "/api/integration/tables/{table_name}",
            "settings": "/api/integration/settings",
        },
    }


@router.get("/downloads")
def integration_downloads(
    state: str = "history",
    limit: int = 100,
    offset: int = 0,
    source_key: str = "",
    q: str = "",
) -> dict[str, Any]:
    try:
        return list_download_records(state, limit, offset, source_key, q)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tables")
def integration_tables() -> dict[str, Any]:
    return {"tables": list(PUBLIC_TABLES)}


@router.get("/tables/{table_name}")
def integration_table_rows(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
    decode_json: bool = True,
) -> dict[str, Any]:
    try:
        return list_integration_rows(table_name, limit, offset, decode_json=decode_json)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/settings")
def integration_settings() -> dict[str, Any]:
    payload = dict(load_settings_payload())
    payload.pop("auth", None)
    return {"settings": payload}
