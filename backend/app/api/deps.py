from __future__ import annotations

import hmac
import os
from typing import Any

from fastapi import HTTPException, Request

from backend.app.domains.auth import current_auth_session


def require_authenticated_session(request: Request) -> dict[str, Any]:
    session = current_auth_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return session


def _api_token() -> str:
    return str(os.environ.get("NEVER_STELLE_API_TOKEN") or "").strip()


def _request_bearer_token(request: Request) -> str:
    authorization = str(request.headers.get("authorization") or "").strip()
    scheme, separator, token = authorization.partition(" ")
    if separator and scheme.lower() == "bearer":
        return token.strip()
    return ""


def require_api_access(request: Request) -> dict[str, Any]:
    token = _api_token()
    supplied = _request_bearer_token(request) or str(request.headers.get("x-api-key") or "").strip()
    if token and supplied and hmac.compare_digest(supplied.encode("utf-8"), token.encode("utf-8")):
        return {"authenticated": True, "api_token": True}

    session = current_auth_session(request)
    if session:
        return session

    raise HTTPException(
        status_code=401,
        detail="Authentication required. Use an app session or API token.",
        headers={"WWW-Authenticate": "Bearer"},
    )
