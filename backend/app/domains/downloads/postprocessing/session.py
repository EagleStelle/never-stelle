from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


def _header_map(source: Any) -> dict[str, str]:
    raw_headers = source.get("http_headers") if isinstance(source, dict) else None
    if not isinstance(raw_headers, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in raw_headers.items()
        if str(key).strip() and str(value).strip()
    }


class _YtdlpLogger:
    """Keep yt-dlp's own output at debug; callers log the outcome."""

    def debug(self, message: str) -> None:
        logger.debug(message)

    info = warning = error = debug


@contextmanager
def _ytdlp_session() -> Iterator[Any]:
    """One yt-dlp network session for the extra files a download's payload points at."""
    from yt_dlp import YoutubeDL

    params = {
        "quiet": True,
        "noprogress": True,
        "logger": _YtdlpLogger(),
        "socket_timeout": 30,
        "retries": 3,
        "nopart": True,
        "continuedl": False,
        "overwrites": True,
    }
    with YoutubeDL(params) as ydl:
        yield ydl
