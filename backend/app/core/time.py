from __future__ import annotations

from datetime import UTC, datetime


def utc_now_datetime() -> datetime:
    return datetime.now(UTC)


def utc_now() -> str:
    return utc_now_datetime().isoformat()
