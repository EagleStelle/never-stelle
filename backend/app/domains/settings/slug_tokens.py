from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import normalize_source_key

from .storage import load_saved_settings_file
from .tokens import normalize_token_name

_SLUG_PART_RE = re.compile(r"^(path:\d+|query:\S+)$")


def normalize_slug_part(value: Any) -> str:
    part = str(value or "").strip()
    return part if _SLUG_PART_RE.fullmatch(part) else ""


def normalize_source_slug_tokens(raw: Any) -> dict[str, list[dict[str, str]]]:
    # Per source, an ordered list of {part, token}. Blank tokens are preserved as
    # an explicit "no URL-part token for this part" choice.
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, list[dict[str, str]]] = {}

    for raw_key, raw_list in source.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_list, list):
            continue

        tokens: list[dict[str, str]] = []
        seen_tokens: set[str] = set()
        seen_parts: set[str] = set()
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            part = normalize_slug_part(item.get("part"))
            if not part or part in seen_parts:
                continue
            raw_token = item.get("token")
            token = normalize_token_name(raw_token)
            if token:
                if token in seen_tokens:
                    continue
                seen_tokens.add(token)
            seen_parts.add(part)
            tokens.append({"part": part, "token": token})
        if tokens:
            out[key] = tokens
    return out


def get_effective_slug_tokens(payload: dict[str, Any] | None = None) -> dict[str, list[dict[str, str]]]:
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_source_slug_tokens(payload.get("source_slug_tokens"))


def load_slug_tokens() -> dict[str, list[dict[str, str]]]:
    return get_effective_slug_tokens()
