from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from backend.app.api.deps import require_auth
from backend.app.api.schemas.auth import ApiKeyPayload, AuthSessionResponse, CredentialsPayload, LoginPayload
from backend.app.domains.auth import (
    AuthError,
    InvalidCredentials,
    authenticate_user,
    clear_session_cookie,
    create_session_token,
    current_auth_session,
    set_api_key,
    set_session_cookie,
    update_auth_credentials,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/session", response_model=AuthSessionResponse)
def auth_session(request: Request) -> dict[str, Any]:
    session = current_auth_session(request)
    if not session:
        return {"authenticated": False, "username": ""}
    return {"authenticated": True, "username": session["username"]}


@router.post("/login", response_model=AuthSessionResponse)
def auth_login(payload: LoginPayload, response: Response) -> dict[str, Any]:
    try:
        auth = authenticate_user(payload.username, payload.password)
    except InvalidCredentials as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    set_session_cookie(response, create_session_token(auth))
    return {"authenticated": True, "username": auth["username"]}


@router.post("/logout", response_model=AuthSessionResponse)
def auth_logout(response: Response) -> dict[str, Any]:
    clear_session_cookie(response)
    return {"authenticated": False, "username": ""}


@router.patch(
    "/credentials",
    response_model=AuthSessionResponse,
    dependencies=[Depends(require_auth)],
)
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


@router.put("/api-key", dependencies=[Depends(require_auth)])
def auth_api_key(payload: ApiKeyPayload) -> dict[str, str]:
    try:
        return {"api_key": set_api_key(payload.api_key)}
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

