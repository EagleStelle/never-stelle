from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from backend.app.core.resolution import resolved
from backend.app.core.sources import merge_source_profiles, normalize_source_key

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _running_in_container() -> bool:
    """Detect a container runtime via Docker markers; decides root only."""
    if Path("/.dockerenv").exists():
        return True
    # Image is built with WORKDIR /app and no checked-out repo.
    if sys.platform.startswith("linux") and Path("/app/backend").exists() and not (PROJECT_ROOT / ".git").exists():
        return True
    return False


def _runtime_root() -> Path:
    """Root of the data/media/scratch trio: ``/`` in container, ``<repo>/.local`` local."""
    if _running_in_container():
        return Path("/")
    return (PROJECT_ROOT / ".local").resolve()


RUNTIME_ROOT = _runtime_root()
# Fixed container-relative roots; the host side is remapped via volume mounts only.
DATA_DIR = (RUNTIME_ROOT / "data").resolve()  # sqlite db, config.yaml, built frontend
MEDIA_DIR = (RUNTIME_ROOT / "media").resolve()  # download library (also the default download root)
SCRATCH_DIR = (RUNTIME_ROOT / "scratch").resolve()  # temp files, partial downloads, caches

DATABASE_PATH = (DATA_DIR / "never-stelle.sqlite3").resolve()
DEFAULT_LIBRARY_DIR = MEDIA_DIR


def _first_existing(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _frontend_dir(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if (candidate / "index.html").exists():
            return candidate.resolve()
    return _first_existing(candidates)


FRONTEND_DIR = _frontend_dir(
    [
        DATA_DIR / "frontend-dist",
        PROJECT_ROOT / "frontend" / "dist",
        PROJECT_ROOT / "frontend",
    ],
)
APP_CONFIG_PATH = _first_existing(
    [
        DATA_DIR / "config.yaml",
        PROJECT_ROOT / "config.yaml",
        PROJECT_ROOT / "config" / "config.yaml",
        Path("/config/config.yaml"),
    ],
)


for _dir in (DATA_DIR, MEDIA_DIR, SCRATCH_DIR):
    _dir.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
DEFAULT_LIBRARY_DIR.mkdir(parents=True, exist_ok=True)

# Route all temp activity (yt-dlp, ffmpeg, tempfile) into scratch.
for _tmp_var in ("TMPDIR", "TEMP", "TMP"):
    os.environ[_tmp_var] = str(SCRATCH_DIR)
tempfile.tempdir = str(SCRATCH_DIR)

SITE_KEYS: tuple[str, ...] = ()
SITE_LABELS = {"all": "All"}


_MAX_WORKER_POOL_SIZE = 16


def max_concurrent_downloads(default: int = 3) -> int:
    # Worker-pool size: parallel downloads. Env-overridable, clamped 1..16.
    raw = str(os.environ.get("NEVER_STELLE_MAX_CONCURRENT") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(value, _MAX_WORKER_POOL_SIZE))


def discover_volume_roots() -> list[str]:
    # Media base is the single library root; per-platform folders live beneath it.
    root = MEDIA_DIR
    if not root.exists() or not root.is_dir():
        return []
    try:
        return [str(root.resolve())]
    except Exception:
        return [str(root)]


# Recursively walking the media tree is expensive on a NAS; the location list
# barely changes, so it is cached and refreshed off the caller's thread. A caller
# never pays for the walk: it gets the last result while a single background
# thread rebuilds it, so a request or a download never blocks on the filesystem.
_VOLUME_LOCATIONS_TTL_SECONDS = 30.0
# Location pickers only need folders a user would actually target; an unbounded
# descent into a deep library costs seconds and offers nothing extra.
_VOLUME_LOCATIONS_MAX_DEPTH = 4
_VOLUME_LOCATIONS_MAX_ENTRIES = 5000
_volume_locations_lock = threading.Lock()
_volume_locations_cache: tuple[float, list[str]] | None = None
_volume_locations_refreshing = False


def _walk_location_dirs(root: Path, out: list[str], budget: int) -> int:
    """Breadth-first directory walk bounded by depth and total entries."""
    frontier = [(root, 0)]
    while frontier and budget > 0:
        current, depth = frontier.pop(0)
        if depth >= _VOLUME_LOCATIONS_MAX_DEPTH:
            continue
        try:
            with os.scandir(current) as entries:
                children = sorted(
                    (entry.path for entry in entries if entry.is_dir(follow_symlinks=False)),
                )
        except OSError:
            continue
        for child in children:
            if budget <= 0:
                break
            out.append(child)
            budget -= 1
            frontier.append((Path(child), depth + 1))
    return budget


def _scan_volume_locations() -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    budget = _VOLUME_LOCATIONS_MAX_ENTRIES
    for root_value in discover_volume_roots():
        root = Path(root_value)
        found: list[str] = [str(root)]
        budget = _walk_location_dirs(root, found, budget)
        for value in found:
            if value not in seen:
                seen.add(value)
                out.append(value)
    return out


def _refresh_volume_locations() -> None:
    global _volume_locations_cache, _volume_locations_refreshing
    try:
        result = _scan_volume_locations()
    except Exception:
        result = []
    with _volume_locations_lock:
        if result or _volume_locations_cache is None:
            _volume_locations_cache = (time.monotonic(), result)
        else:
            _volume_locations_cache = (time.monotonic(), _volume_locations_cache[1])
        _volume_locations_refreshing = False


def discover_volume_locations() -> list[str]:
    global _volume_locations_cache, _volume_locations_refreshing
    now = time.monotonic()
    with _volume_locations_lock:
        cached = _volume_locations_cache
        if cached is not None:
            stale = now - cached[0] >= _VOLUME_LOCATIONS_TTL_SECONDS
            if stale and not _volume_locations_refreshing:
                _volume_locations_refreshing = True
                threading.Thread(
                    target=_refresh_volume_locations, name="never-stelle-locations", daemon=True
                ).start()
            # Serve the known list either way; a stale folder list is never worth
            # stalling a caller on a recursive filesystem walk.
            return list(cached[1])
    # First call in the process: nothing to serve yet, so walk inline once.
    result = _scan_volume_locations()
    with _volume_locations_lock:
        _volume_locations_cache = (now, result)
    return list(result)


def normalize_download_locations(cfg: dict[str, Any]) -> list[str]:
    raw = cfg.get("downloadLocations") or []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str):
            path = item.strip()
        elif isinstance(item, dict):
            path = str(item.get("path") or item.get("value") or "").strip()
        else:
            path = ""
        if path and path not in out:
            out.append(path)
    return out


def normalize_allowed_location(raw_path: str) -> str:
    candidate_raw = str(raw_path or "").strip()
    if not candidate_raw:
        return ""
    try:
        candidate_resolved = Path(candidate_raw).resolve(strict=False)
    except Exception:
        candidate_resolved = Path(candidate_raw)

    for root_value in discover_volume_roots():
        try:
            root_resolved = Path(root_value).resolve()
        except Exception:
            root_resolved = Path(root_value)
        if candidate_resolved == root_resolved or root_resolved in candidate_resolved.parents:
            return str(candidate_resolved)
    return ""


def is_allowed_location(path: str) -> bool:
    return bool(normalize_allowed_location(path))


def _read_app_config() -> dict[str, Any]:
    file_cfg: dict[str, Any] = {}
    if APP_CONFIG_PATH.exists():
        try:
            file_cfg = yaml.safe_load(APP_CONFIG_PATH.read_text(encoding="utf-8")) or {}
        except Exception:
            file_cfg = {}

    cfg = deepcopy(file_cfg) if isinstance(file_cfg, dict) else {}
    discovered_locations = discover_volume_locations()
    if discovered_locations:
        cfg["downloadLocations"] = discovered_locations
    return cfg


# Scope key for the resolved app config, so callers can recognize the object they
# were handed as the one this operation already resolved.
APP_CONFIG_KEY = "core.app_config"


def load_app_config() -> dict[str, Any]:
    # Parsing the YAML is cheap once but not fifty times per request; the config
    # cannot change mid-operation, so resolve it once per scope.
    return resolved(APP_CONFIG_KEY, _read_app_config)


def get_default_general_location(cfg: dict[str, Any]) -> str:
    value = normalize_allowed_location(str(cfg.get("defaultGeneralDownloadLocation") or "").strip())
    if value:
        return value
    locations = normalize_download_locations(cfg)
    return locations[0] if locations else str(MEDIA_DIR)


def get_config_source_profiles(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    return merge_source_profiles(cfg.get("sourceProfiles") or cfg.get("sources") or {})


def get_default_site_location(cfg: dict[str, Any], site: str) -> str:
    site = normalize_source_key(site)
    if not site:
        return ""
    profile = next((item for item in get_config_source_profiles(cfg) if item.get("key") == site), {})
    profile_default = str(profile.get("default_download_location") or "").strip()
    if profile_default:
        normalized_profile_default = normalize_allowed_location(profile_default)
        if normalized_profile_default:
            return normalized_profile_default
    base = get_default_general_location(cfg)
    if not base:
        return ""
    if "\\" in base or ":" in base:
        return str(Path(base) / site)
    return f"{base.rstrip('/')}/{site}"


def get_site_default_locations(cfg: dict[str, Any], source_keys: Iterable[str] | None = None) -> dict[str, str]:
    raw_keys = source_keys or [profile["key"] for profile in get_config_source_profiles(cfg)]
    keys = [key for key in (normalize_source_key(key) for key in raw_keys) if key]
    return {site: get_default_site_location(cfg, site) for site in dict.fromkeys(keys)}
