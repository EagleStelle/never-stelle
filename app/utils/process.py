"""Subprocess watchdog utilities."""

import os
import subprocess
import threading
from typing import Any


ACTIVITY_TIMEOUT_SECONDS = int(os.environ.get("TASK_ACTIVITY_TIMEOUT", "300"))


class ActivityWatchdog:
    """Kills a subprocess if no stdout activity for ACTIVITY_TIMEOUT_SECONDS."""

    def __init__(self, process: subprocess.Popen, timeout: float = ACTIVITY_TIMEOUT_SECONDS):
        self._process = process
        self._timeout = timeout
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()
        self._triggered = False
        self._arm()

    def _arm(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(self._timeout, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self) -> None:
        with self._lock:
            self._triggered = True
        try:
            self._process.kill()
        except Exception:
            pass

    def ping(self) -> None:
        """Call on each line of subprocess output to reset the timer."""
        if not self._triggered:
            self._arm()

    def cancel(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

    @property
    def timed_out(self) -> bool:
        return self._triggered
