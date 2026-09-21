from __future__ import annotations

import os
import subprocess
import threading
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from backend.app.domains.settings import (
    CookieLease,
    cookie_rotation,
    has_cookies_for_source,
    looks_antibot_walled,
    looks_rate_limited,
)

_TARGET_LIST_TIMEOUT_SECONDS = 30
# Directory holding the fingerprint backend (curl_cffi), kept off the default import path
# because yt-dlp loads every installed request handler at startup.
_IMPERSONATE_PATH_ENV = "NEVER_STELLE_IMPERSONATE_PATH"

_target_lock = threading.Lock()
_target_cache: str | None = None


@dataclass
class AccessIdentity:
    """How one attempt reaches a site. Each engine translates it into its own flags."""

    cookies_file: str = ""
    # Browser family whose TLS fingerprint the engine presents; "" sends its own.
    impersonate: str = ""
    lease: CookieLease | None = None
    walled: bool = False

    def report(self, output: Any) -> None:
        """Classify a failed attempt's output so the rotation can pick the next step."""
        self.walled = looks_antibot_walled(output)
        if self.lease is not None:
            self.lease.banned = looks_rate_limited(output) and not self.walled


def _impersonation_env() -> dict[str, str] | None:
    path = str(os.environ.get(_IMPERSONATE_PATH_ENV) or "").strip()
    if not path:
        return None
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(part for part in (path, env.get("PYTHONPATH", "")) if part)
    return env


def access_env(access: AccessIdentity) -> dict[str, str] | None:
    """Subprocess environment for an attempt; ``None`` keeps the inherited one."""
    return _impersonation_env() if access.impersonate else None


def parse_impersonate_targets(output: str) -> str:
    """Browser family of yt-dlp's preferred usable target; its listing prints that one last."""
    preferred = ""
    for line in str(output or "").splitlines():
        cells = line.split()
        if len(cells) < 3 or line.startswith("[") or "(unavailable)" in line:
            continue
        family = cells[0].partition("-")[0].lower()
        if family.isalpha() and family != "client":
            preferred = family
    return preferred


def impersonation_target() -> str:
    """Browser family this install can impersonate, or "" when no backend is present."""
    global _target_cache
    with _target_lock:
        if _target_cache is None:
            try:
                result = subprocess.run(
                    ["yt-dlp", "--list-impersonate-targets"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=_TARGET_LIST_TIMEOUT_SECONDS,
                    env=_impersonation_env(),
                )
            except (OSError, subprocess.SubprocessError):
                return ""
            _target_cache = parse_impersonate_targets(result.stdout)
        return _target_cache


def access_rotation(
    cookie_source_key: str | Callable[[], str],
    *,
    fingerprint: bool = True,
    first_cookie_wait: float | None = None,
) -> Iterator[AccessIdentity]:
    """Yield ways to reach a site, cheapest first, until the caller finds one that works.

    Anonymous first. After a wall, anonymous again behind a browser fingerprint. Then
    each of the source's cookie jars, fingerprinted once any attempt hit a wall. Callers
    that cannot present a fingerprint pass ``fingerprint=False``. A callable source key
    is only resolved once cookies are needed. ``first_cookie_wait`` caps the wait for the
    first jar, the source's policy by default. Call ``report`` with a failed attempt's
    output before continuing, and close the iterator (``contextlib.closing``) when
    breaking early.
    """
    anonymous = AccessIdentity()
    yield anonymous
    impersonate = impersonation_target() if fingerprint and anonymous.walled else ""
    if impersonate:
        yield AccessIdentity(impersonate=impersonate)
    if callable(cookie_source_key):
        cookie_source_key = cookie_source_key()
    if not has_cookies_for_source(cookie_source_key):
        return
    with closing(cookie_rotation(cookie_source_key, first_wait=first_cookie_wait)) as rotation:
        for lease in rotation:
            identity = AccessIdentity(cookies_file=lease.path, impersonate=impersonate, lease=lease)
            yield identity
            if fingerprint and identity.walled and not impersonate:
                impersonate = impersonation_target()
