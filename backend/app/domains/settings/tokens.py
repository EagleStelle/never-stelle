from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import normalize_source_key

from .storage import load_saved_settings_file

TOKEN_ROLES = {"creator", "title", "ignore"}
_TOKEN_NAME_RE = re.compile(r"[^a-zA-Z0-9_]+")


def normalize_token_name(value: Any) -> str:
    token = _TOKEN_NAME_RE.sub("_", str(value or "").strip()).strip("_")
    if not token or not re.match(r"[a-zA-Z_]", token):
        return ""
    return token.lower()


def normalize_source_token_roles(raw: Any) -> dict[str, dict[str, str]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, str]] = {}
    for raw_key, raw_roles in source.items():
        key = normalize_source_key(raw_key)
        roles: dict[str, str] = {}
        if isinstance(raw_roles, dict):
            for raw_token, raw_role in raw_roles.items():
                token = normalize_token_name(raw_token)
                role = str(raw_role or "").strip().lower()
                if role in {"username", "nickname"}:
                    role = "creator"
                if not token or role not in TOKEN_ROLES:
                    continue
                roles[token] = role
        if key and roles:
            out[key] = roles
    return out


def get_effective_token_roles(payload: dict[str, Any] | None = None) -> dict[str, dict[str, str]]:
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_source_token_roles(payload.get("source_token_roles"))


def load_token_roles() -> dict[str, dict[str, str]]:
    return get_effective_token_roles()
