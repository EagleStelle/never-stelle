from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from backend.app.core.sources import FALLBACK_SOURCE_KEY, merge_source_profiles, normalize_source_key

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _running_in_container() -> bool:
    """Best-effort detection of a containerized runtime.

    Honors an explicit APP_IN_CONTAINER override, then falls back to the
    standard Docker markers. Detection only decides the default root.
    """
    flag = os.environ.get("APP_IN_CONTAINER", "")
    if flag:
        return _truthy(flag)
    if Path("/.dockerenv").exists():
        return True
    # Image is built with WORKDIR /app and no checked-out repo.
    if sys.platform.startswith("linux") and Path("/app/backend").exists() and not (PROJECT_ROOT / ".git").exists():
        return True
    return False


def _runtime_root() -> Path:
    """Root that holds the data/media/scratch trio.

    - Container  -> ``/`` so the trio resolves to ``/data /media /scratch``.
    - Local/dev  -> ``<repo>/.local`` (Windows constraint: everything under .local).
    """
    if _running_in_container():
        return Path("/")
    return (PROJECT_ROOT / ".local").resolve()


def _role_dir(env_name: str, root: Path, name: str) -> Path:
    override = os.environ.get(env_name, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (root / name).resolve()


RUNTIME_ROOT = _runtime_root()
# Persistent app state: sqlite db, config.yaml, built frontend.
DATA_DIR = _role_dir("APP_DATA_DIR", RUNTIME_ROOT, "data")
# Downloaded media library (also the default download root).
MEDIA_DIR = _role_dir("APP_MEDIA_DIR", RUNTIME_ROOT, "media")
# Ephemeral scratch: temp files, partial downloads, caches.
SCRATCH_DIR = _role_dir("APP_SCRATCH_DIR", RUNTIME_ROOT, "scratch")

# Back-compat alias: code/scripts still reference RUNTIME_DIR.
RUNTIME_DIR = DATA_DIR

DATABASE_PATH = Path(os.environ.get("APP_DATABASE_PATH") or DATA_DIR / "never-stelle.sqlite3").resolve()
DEFAULT_LIBRARY_DIR = MEDIA_DIR


def _resolve_existing_path(env_name: str, candidates: list[Path]) -> Path:
    override = os.environ.get(env_name, "").strip()
    if override:
        return Path(override).resolve()
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


FRONTEND_DIR = _resolve_existing_path(
    "FRONTEND_DIR",
    [
        DATA_DIR / "frontend-dist",
        PROJECT_ROOT / "frontend" / "dist",
        PROJECT_ROOT / "frontend",
    ],
)
APP_CONFIG_PATH = _resolve_existing_path(
    "APP_CONFIG_PATH",
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


def discover_volume_roots() -> list[str]:
    # Media base is the single library root; per-platform folders live beneath it.
    root = MEDIA_DIR
    if not root.exists() or not root.is_dir():
        return []
    try:
        return [str(root.resolve())]
    except Exception:
        return [str(root)]


def discover_volume_locations() -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for root_value in discover_volume_roots():
        root = Path(root_value)
        try:
            candidates = [root]
            candidates.extend(sorted((child for child in root.rglob("*") if child.is_dir()), key=str))
        except Exception:
            candidates = [root]
        for candidate in candidates:
            value = str(candidate)
            if value not in seen:
                seen.add(value)
                out.append(value)
    return out


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


def load_app_config() -> dict[str, Any]:
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


def get_default_general_location(cfg: dict[str, Any]) -> str:
    value = normalize_allowed_location(str(cfg.get("defaultGeneralDownloadLocation") or "").strip())
    if value:
        return value
    locations = normalize_download_locations(cfg)
    return locations[0] if locations else str(MEDIA_DIR)


def get_config_source_profiles(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    return merge_source_profiles(cfg.get("sourceProfiles") or cfg.get("sources") or {})


def get_default_site_location(cfg: dict[str, Any], site: str) -> str:
    site = normalize_source_key(site or FALLBACK_SOURCE_KEY)
    profile = next((item for item in get_config_source_profiles(cfg) if item.get("key") == site), {})
    profile_default = str(profile.get("default_download_location") or "").strip()
    if profile_default:
        normalized_profile_default = normalize_allowed_location(profile_default)
        if normalized_profile_default:
            return normalized_profile_default
    base = get_default_general_location(cfg)
    return f"{base.rstrip('/')}/{site}" if base else ""


def get_site_default_locations(cfg: dict[str, Any], source_keys: Iterable[str] | None = None) -> dict[str, str]:
    raw_keys = source_keys or [profile["key"] for profile in get_config_source_profiles(cfg)]
    keys = [normalize_source_key(key) for key in raw_keys]
    if FALLBACK_SOURCE_KEY not in keys:
        keys.append(FALLBACK_SOURCE_KEY)
    return {site: get_default_site_location(cfg, site) for site in dict.fromkeys(keys)}
