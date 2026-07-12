from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any

from fastapi import Response
from starlette.requests import Request

from backend.app.db.repositories import load_settings_payload, save_settings_payload

AUTH_COOKIE_NAME = "never_stelle_session"
AUTH_SETTINGS_KEY = "auth"
DEFAULT_USERNAME = "root"
DEFAULT_PASSWORD = "never-stelle"
PASSWORD_ITERATIONS = 390_000
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30


class AuthError(ValueError):
    pass


class InvalidCredentials(AuthError):
    pass


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii"))


def _first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return ""


def _seed_username() -> str:
    return _normalize_username(
        _first_env("NEVER_STELLE_USERNAME", "NEVER_STELLE_AUTH_USERNAME") or DEFAULT_USERNAME
    )


def _seed_password() -> str:
    return _first_env("NEVER_STELLE_PASSWORD", "NEVER_STELLE_AUTH_PASSWORD") or DEFAULT_PASSWORD


def _normalize_username(value: Any) -> str:
    return str(value or "").strip()


def _coerce_session_version(value: Any) -> int:
    try:
        return max(1, int(value))
    except Exception:
        return 1


def _hash_password(password: str, salt: bytes | None = None, iterations: int = PASSWORD_ITERATIONS) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64encode(salt)}${_b64encode(digest)}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, raw_iterations, raw_salt, raw_digest = encoded.split("$", 3)
        iterations = int(raw_iterations)
        salt = _b64decode(raw_salt)
        digest = _b64decode(raw_digest)
    except Exception:
        return False
    if algorithm != "pbkdf2_sha256" or iterations <= 0:
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(candidate, digest)


def _password_hash_looks_valid(encoded: str) -> bool:
    try:
        algorithm, raw_iterations, raw_salt, raw_digest = encoded.split("$", 3)
        iterations = int(raw_iterations)
        _b64decode(raw_salt)
        _b64decode(raw_digest)
    except Exception:
        return False
    return algorithm == "pbkdf2_sha256" and iterations > 0


def _load_auth_payload() -> tuple[dict[str, Any], dict[str, Any]]:
    payload = load_settings_payload()
    auth = payload.get(AUTH_SETTINGS_KEY)
    return payload, dict(auth) if isinstance(auth, dict) else {}


def ensure_auth_settings() -> dict[str, Any]:
    payload, auth = _load_auth_payload()
    now = int(time.time())

    username = _normalize_username(auth.get("username")) or _seed_username()
    password_hash = str(auth.get("password_hash") or "")
    if not password_hash or not _password_hash_looks_valid(password_hash):
        password_hash = _hash_password(_seed_password())

    session_secret = str(auth.get("session_secret") or "")
    if len(session_secret) < 32:
        session_secret = _b64encode(secrets.token_bytes(32))

    normalized = {
        **auth,
        "username": username,
        "password_hash": password_hash,
        "session_secret": session_secret,
        "session_version": _coerce_session_version(auth.get("session_version")),
        "created_at": int(auth.get("created_at") or now),
        "updated_at": int(auth.get("updated_at") or now),
    }

    if payload.get(AUTH_SETTINGS_KEY) != normalized:
        payload[AUTH_SETTINGS_KEY] = normalized
        save_settings_payload(payload)

    return normalized


def auth_public_payload() -> dict[str, Any]:
    auth = ensure_auth_settings()
    return {
        "username": auth.get("username", ""),
        "password_configured": bool(auth.get("password_hash")),
    }


def authenticate_user(username: str, password: str) -> dict[str, Any]:
    auth = ensure_auth_settings()
    expected_username = str(auth.get("username") or "")
    supplied_username = _normalize_username(username)
    username_ok = hmac.compare_digest(
        supplied_username.encode("utf-8"),
        expected_username.encode("utf-8"),
    )
    password_ok = _verify_password(str(password or ""), str(auth.get("password_hash") or ""))
    if not username_ok or not password_ok:
        raise InvalidCredentials("Invalid username or password.")
    return auth


def _session_signature(body: str, secret: str) -> str:
    raw = hmac.new(secret.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return _b64encode(raw)


def create_session_token(auth: dict[str, Any] | None = None) -> str:
    auth = auth or ensure_auth_settings()
    now = int(time.time())
    body_payload = {
        "sub": str(auth.get("username") or ""),
        "iat": now,
        "exp": now + SESSION_MAX_AGE_SECONDS,
        "ver": _coerce_session_version(auth.get("session_version")),
    }
    body = _b64encode(json.dumps(body_payload, separators=(",", ":")).encode("utf-8"))
    return f"{body}.{_session_signature(body, str(auth.get('session_secret') or ''))}"


def read_session_token(token: str) -> dict[str, Any] | None:
    try:
        body, signature = str(token or "").split(".", 1)
    except ValueError:
        return None

    auth = ensure_auth_settings()
    expected_signature = _session_signature(body, str(auth.get("session_secret") or ""))
    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        payload = json.loads(_b64decode(body).decode("utf-8"))
    except Exception:
        return None

    if int(payload.get("exp") or 0) < int(time.time()):
        return None
    if str(payload.get("sub") or "") != str(auth.get("username") or ""):
        return None
    if _coerce_session_version(payload.get("ver")) != _coerce_session_version(auth.get("session_version")):
        return None

    return {
        "authenticated": True,
        "username": str(auth.get("username") or ""),
        "expires_at": int(payload.get("exp") or 0),
    }


def current_auth_session(request: Request) -> dict[str, Any] | None:
    return read_session_token(request.cookies.get(AUTH_COOKIE_NAME, ""))


def is_authenticated_request(request: Request) -> bool:
    return current_auth_session(request) is not None


def _cookie_secure() -> bool:
    return os.environ.get("NEVER_STELLE_COOKIE_SECURE", "").strip().lower() in {"1", "true", "yes", "on"}


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(AUTH_COOKIE_NAME, path="/", samesite="lax")


def update_auth_credentials(
    current_password: str,
    username: str,
    new_password: str = "",
) -> dict[str, Any]:
    auth = ensure_auth_settings()
    if not _verify_password(str(current_password or ""), str(auth.get("password_hash") or "")):
        raise InvalidCredentials("Current password is incorrect.")

    next_username = _normalize_username(username) or str(auth.get("username") or "")
    if not next_username:
        raise AuthError("Username is required.")
    if len(next_username) > 120:
        raise AuthError("Username is too long.")

    next_password = str(new_password or "")
    if next_password and len(next_password) < 8:
        raise AuthError("Password must be at least 8 characters.")

    updated = dict(auth)
    changed = False
    if next_username != str(auth.get("username") or ""):
        updated["username"] = next_username
        changed = True
    if next_password:
        updated["password_hash"] = _hash_password(next_password)
        changed = True

    if changed:
        updated["session_version"] = _coerce_session_version(auth.get("session_version")) + 1
        updated["updated_at"] = int(time.time())
        payload, _ = _load_auth_payload()
        payload[AUTH_SETTINGS_KEY] = updated
        save_settings_payload(payload)

    return updated
