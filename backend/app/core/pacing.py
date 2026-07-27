from __future__ import annotations

import os
import threading
import time
from pathlib import Path

# A bulk loop (library scan, log streaming) must not own the box. Rather than
# guessing a sleep per item, the pacer measures the CPU the loop actually burns
# and sleeps just enough to hold it under its budget, so it self-tunes to the
# machine, the library size, and whatever else is running.
#
# The budget is derived, never configured: it comes from the CPU the container is
# actually allowed to use, and it is split live across however many bulk loops are
# running, so two concurrent scans each take half rather than doubling the load.

_MIN_SHARE = 0.2
_MAX_SHARE = 0.9
_RESERVED_CORES = 1.0

_registry_lock = threading.Lock()
_active_pacers = 0

_capacity_lock = threading.Lock()
_capacity_cache: float | None = None


def _cgroup_v2_quota() -> float | None:
    try:
        raw = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
    except OSError:
        return None
    if len(raw) != 2 or raw[0] == "max":
        return None
    try:
        quota, period = float(raw[0]), float(raw[1])
    except ValueError:
        return None
    return quota / period if period > 0 and quota > 0 else None


def _cgroup_v1_quota() -> float | None:
    base = Path("/sys/fs/cgroup/cpu")
    try:
        quota = float(Path(base / "cpu.cfs_quota_us").read_text(encoding="utf-8").strip())
        period = float(Path(base / "cpu.cfs_period_us").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    return quota / period if period > 0 and quota > 0 else None


def available_cores() -> float:
    """Cores this process may actually use, honoring a container CPU limit.

    ``os.cpu_count()`` reports the host on a NAS running Docker, which would let a
    scan claim far more than the container is allowed. The cgroup quota is the real
    ceiling when one is set.
    """
    global _capacity_cache
    with _capacity_lock:
        if _capacity_cache is not None:
            return _capacity_cache
    cores = float(os.cpu_count() or 1)
    affinity = getattr(os, "sched_getaffinity", None)
    if affinity is not None:
        try:
            cores = min(cores, float(len(affinity(0))))
        except OSError:
            pass
    for quota in (_cgroup_v2_quota(), _cgroup_v1_quota()):
        if quota:
            cores = min(cores, quota)
    cores = max(1.0, cores)
    with _capacity_lock:
        _capacity_cache = cores
    return cores


def background_cpu_budget() -> float:
    """Total core-equivalents all background bulk loops may share.

    Keeps roughly one core free for the API, the downloader processes, and the
    rest of the machine, then caps the remainder so a many-core box does not hand
    a scan the whole CPU either.
    """
    cores = available_cores()
    return max(_MIN_SHARE, min(cores - _RESERVED_CORES, cores * _MAX_SHARE))


def _thread_cpu() -> float:
    clock = getattr(time, "thread_time", None) or time.process_time
    try:
        return clock()
    except Exception:
        return time.process_time()


class CpuPacer:
    """Throttle a loop to its live share of the background CPU budget.

    ``tick()`` is called once per unit of work. Every ``check_every`` ticks it
    compares the thread CPU burned against the wall time spent and sleeps the
    shortfall needed to meet the target, so a cheap loop never sleeps and an
    expensive one backs off on its own. The target is recomputed each time from
    how many pacers are currently running, which makes concurrent scans divide the
    budget instead of multiplying the load.
    """

    def __init__(self, check_every: int = 64, max_sleep: float = 0.25) -> None:
        self.check_every = max(1, int(check_every))
        self.max_sleep = max(0.0, float(max_sleep))
        self.share = background_cpu_budget()
        self._ticks = 0
        self._entered = False
        self._cpu_mark = _thread_cpu()
        self._wall_mark = time.monotonic()

    def __enter__(self) -> CpuPacer:
        global _active_pacers
        with _registry_lock:
            _active_pacers += 1
        self._entered = True
        self._reset_marks()
        return self

    def __exit__(self, *_exc: object) -> None:
        global _active_pacers
        if self._entered:
            with _registry_lock:
                _active_pacers = max(0, _active_pacers - 1)
            self._entered = False

    def _current_share(self) -> float:
        with _registry_lock:
            sharers = max(1, _active_pacers)
        return background_cpu_budget() / sharers

    def _reset_marks(self) -> None:
        self._cpu_mark = _thread_cpu()
        self._wall_mark = time.monotonic()

    def tick(self) -> None:
        self._ticks += 1
        if self._ticks < self.check_every:
            return
        self._ticks = 0
        share = self._current_share()
        self.share = share
        if share >= 1.0:
            # Allowed a full core or more to itself; nothing to hold back.
            self._reset_marks()
            return
        busy = _thread_cpu() - self._cpu_mark
        elapsed = time.monotonic() - self._wall_mark
        # Wall time this batch should have taken at the target share, minus what it
        # already took. Sleeping the remainder pulls the running average onto target.
        deficit = (busy / share) - elapsed
        if deficit > 0.001:
            time.sleep(min(deficit, self.max_sleep))
        self._reset_marks()
