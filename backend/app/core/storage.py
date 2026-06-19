from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Callable


_locks: dict[Path, threading.RLock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.RLock:
    resolved = path.resolve(strict=False)
    with _locks_guard:
        lock = _locks.get(resolved)
        if lock is None:
            lock = threading.RLock()
            _locks[resolved] = lock
        return lock


def load_json(
    path: Path,
    default: Callable[[], Any],
    normalizer: Callable[[Any], Any] | None = None,
) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        if not path.exists():
            return default()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default()
        return normalizer(payload) if normalizer else payload


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)


def file_lock(path: Path) -> threading.RLock:
    path.parent.mkdir(parents=True, exist_ok=True)
    return _lock_for(path)
