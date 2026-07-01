from __future__ import annotations

from backend.app.core.sources import source_key_from_url


def canonicalize_source_url(source_url: str) -> str:
    source_url = str(source_url or "").strip()
    if not source_url:
        return ""
    if "://" not in source_url:
        source_url = f"https://{source_url}"
    return source_url


def detect_source_key(source_url: str) -> str:
    return source_key_from_url(source_url)
