from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import DATA_DIR, DATABASE_PATH, PROJECT_ROOT

_DB_LOCK = threading.RLock()
_INITIALIZED = False


SCHEMA = """
CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    site_category TEXT NOT NULL DEFAULT 'others',
    save_mode TEXT NOT NULL DEFAULT 'nas',
    progress_pct REAL NOT NULL DEFAULT 0,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_source_url ON tasks(source_url);

CREATE TABLE IF NOT EXISTS task_meta (
    task_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS download_history (
    task_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL DEFAULT '',
    site_category TEXT NOT NULL DEFAULT 'others',
    payload TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_download_history_source_url ON download_history(source_url);

CREATE TABLE IF NOT EXISTS file_blobs (
    key TEXT PRIMARY KEY,
    filename TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def database_path() -> Path:
    return DATABASE_PATH


def _connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(DATABASE_PATH), timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    initialize_database()
    with _DB_LOCK:
        connection = _connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def initialize_database() -> None:
    global _INITIALIZED
    with _DB_LOCK:
        if _INITIALIZED:
            return
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = _connect()
        try:
            connection.executescript(SCHEMA)
            _migrate_legacy_files(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        _INITIALIZED = True


def _load_json_file(path: Path) -> Any | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _legacy_dirs() -> list[Path]:
    candidates = [
        DATA_DIR,
        DATA_DIR / "data",
        PROJECT_ROOT / ".local" / "data",
        PROJECT_ROOT / "data",
    ]
    out: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        try:
            key = str(candidate.resolve(strict=False)).lower()
        except Exception:
            key = str(candidate).lower()
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    return out


def _table_count(connection: sqlite3.Connection, table: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
    return int(row["count"] if row else 0)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _first_legacy_payload(filename: str) -> Any | None:
    for directory in _legacy_dirs():
        payload = _load_json_file(directory / filename)
        if payload is not None:
            return payload
    return None


def _insert_setting_payload(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT OR REPLACE INTO settings (key, value, updated_at)
        VALUES (?, ?, ?)
        """,
        ("app", json.dumps(payload, ensure_ascii=False), now),
    )


def _insert_task_payload(connection: sqlite3.Connection, task_id: str, payload: dict[str, Any]) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT OR REPLACE INTO tasks (
            id, source_url, status, site_category, save_mode, progress_pct, payload, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            str(payload.get("source_url") or ""),
            str(payload.get("status") or "pending"),
            _site_category(str(payload.get("source_url") or "")),
            str(payload.get("save_mode") or "nas"),
            _safe_float(payload.get("progress_pct")),
            json.dumps(payload, ensure_ascii=False),
            str(payload.get("created_at") or now),
            now,
        ),
    )


def _insert_meta_payload(connection: sqlite3.Connection, task_id: str, payload: dict[str, Any]) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT OR REPLACE INTO task_meta (task_id, payload, updated_at)
        VALUES (?, ?, ?)
        """,
        (task_id, json.dumps(payload, ensure_ascii=False), now),
    )


def _insert_history_payload(connection: sqlite3.Connection, task_id: str, payload: dict[str, Any]) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT OR REPLACE INTO download_history (
            task_id, source_url, site_category, payload, completed_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            str(payload.get("source_url") or ""),
            str(payload.get("site_category") or _site_category(str(payload.get("source_url") or ""))),
            json.dumps(payload, ensure_ascii=False),
            str(payload.get("completed_at") or now),
            now,
        ),
    )


def _site_category(source_url: str) -> str:
    source_url = str(source_url or "").lower()
    if "youtube.com" in source_url or "youtu.be" in source_url:
        return "youtube"
    if "facebook.com" in source_url or "fb.com" in source_url or "fb.watch" in source_url:
        return "facebook"
    if "instagram.com" in source_url:
        return "instagram"
    if "tiktok.com" in source_url:
        return "tiktok"
    return "others"


def _migrate_legacy_files(connection: sqlite3.Connection) -> None:
    migrated = connection.execute("SELECT value FROM app_meta WHERE key = ?", ("legacy_migration_v1",)).fetchone()
    if migrated:
        return

    if _table_count(connection, "settings") == 0:
        settings_payload = _first_legacy_payload("settings.json")
        if isinstance(settings_payload, dict):
            _insert_setting_payload(connection, settings_payload)

    if _table_count(connection, "tasks") == 0:
        tasks_payload = _first_legacy_payload("tasks.json")
        if isinstance(tasks_payload, dict):
            for task_id, task in (tasks_payload.get("tasks") or {}).items():
                if isinstance(task, dict):
                    _insert_task_payload(connection, str(task_id), task)

    if _table_count(connection, "task_meta") == 0:
        meta_payload = _first_legacy_payload("task_meta.json")
        if isinstance(meta_payload, dict):
            source = meta_payload.get("tasks") if isinstance(meta_payload.get("tasks"), dict) else meta_payload
            for task_id, task_meta in (source or {}).items():
                if isinstance(task_meta, dict):
                    _insert_meta_payload(connection, str(task_id), task_meta)

    if _table_count(connection, "download_history") == 0:
        history_payload = _first_legacy_payload("download_history.json")
        if isinstance(history_payload, dict):
            for task_id, entry in (history_payload.get("entries") or {}).items():
                if isinstance(entry, dict):
                    _insert_history_payload(connection, str(task_id), entry)

    if _table_count(connection, "file_blobs") == 0:
        for directory in _legacy_dirs():
            cookie_path = directory / "instagram-ytdlp-cookies.txt"
            if cookie_path.exists() and cookie_path.is_file():
                settings_payload = _first_legacy_payload("settings.json")
                filename = "cookies.txt"
                uploaded_at = utc_now()
                if isinstance(settings_payload, dict):
                    filename = str(settings_payload.get("instagram_cookies_filename") or filename)
                    uploaded_at = str(settings_payload.get("instagram_cookies_uploaded_at") or uploaded_at)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO file_blobs (
                        key, filename, content_type, content, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "instagram_ytdlp_cookies",
                        filename,
                        "text/plain",
                        cookie_path.read_bytes(),
                        uploaded_at,
                        utc_now(),
                    ),
                )
                break

    connection.execute(
        "INSERT OR REPLACE INTO app_meta (key, value, updated_at) VALUES (?, ?, ?)",
        ("legacy_migration_v1", "complete", utc_now()),
    )
