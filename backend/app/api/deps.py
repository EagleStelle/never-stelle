from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request

from backend.app.domains.auth import api_key_matches, current_auth_session


def require_auth(request: Request) -> dict[str, Any]:
    session = current_auth_session(request)
    if session:
        return session
    supplied = (
        request.query_params.get("apikey")
        or request.headers.get("x-api-key")
        or (request.headers.get("authorization") or "").removeprefix("Bearer ")
    )
    if api_key_matches(supplied):
        return {"authenticated": True, "api_key": True}
    raise HTTPException(status_code=401, detail="Authentication required.")
