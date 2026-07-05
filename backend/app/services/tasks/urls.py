from __future__ import annotations

from backend.app.core.sources import source_key_from_url
from backend.app.services.tasks.formats import canonicalize_url


def canonicalize_source_url(source_url: str) -> str:
    return canonicalize_url(source_url)


def detect_source_key(source_url: str) -> str:
    return source_key_from_url(source_url)
