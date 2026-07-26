from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request

from backend.app.domains.auth import current_auth_session


def require_authenticated_session(request: Request) -> dict[str, Any]:
    session = current_auth_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return session

