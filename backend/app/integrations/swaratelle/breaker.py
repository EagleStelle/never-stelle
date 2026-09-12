"""Circuit breaker for the Swaratelle remote.

Refuses poll-path calls while the remote is known unreachable and reopens them once a
background probe succeeds.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

COOLDOWN_SECONDS = 30.0

_lock = threading.Lock()
_open_until: float | None = 0.0
_probing = False
_probe: Callable[[], None] | None = None


def register_probe(probe: Callable[[], None]) -> None:
    """Install the call the background thread makes to test the remote."""
    global _probe
    _probe = probe


def allow() -> bool:
    """True when a poll-path call may go out now.

    An expired cooldown starts the probe and still refuses the caller.
    """
    global _probing
    with _lock:
        if _open_until is None:
            return True
        if time.monotonic() < _open_until or _probing or _probe is None:
            return False
        _probing = True
    threading.Thread(target=_run_probe, daemon=True).start()
    return False


def record_success() -> None:
    global _open_until
    with _lock:
        _open_until = None


def record_failure() -> None:
    global _open_until
    with _lock:
        _open_until = time.monotonic() + COOLDOWN_SECONDS


def reset() -> None:
    """Back to the cold state, nothing known about the remote."""
    global _open_until, _probing
    with _lock:
        _open_until = 0.0
        _probing = False


def start_probe() -> None:
    """Probe the remote off the request path, ignoring any cooldown."""
    global _probing
    with _lock:
        if _probing or _probe is None:
            return
        _probing = True
    threading.Thread(target=_run_probe, daemon=True).start()


def _run_probe() -> None:
    global _probing
    try:
        _probe()
    except Exception:
        pass
    finally:
        with _lock:
            _probing = False
