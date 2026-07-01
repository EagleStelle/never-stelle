from __future__ import annotations

import json
import os
import sys
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

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

SITE_KEYS = (
    "youtube",
    "tiktok",
    "instagram",
    "twitter",
    "facebook",
    "reddit",
    "twitch",
    "pinterest",
    "bluesky",
    "linkedin",
    "others",
)
SITE_LABELS = {
    "all": "All",
    "youtube": "YouTube",
    "tiktok": "TikTok",
    "instagram": "Instagram",
    "twitter": "X (Twitter)",
    "facebook": "Facebook",
    "reddit": "Reddit",
    "twitch": "Twitch",
    "pinterest": "Pinterest",
    "bluesky": "Bluesky",
    "linkedin": "LinkedIn",
    "others": "Others",
}


def parse_env_locations(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [item.strip() for item in raw.split("|") if item.strip()]


def discover_volume_roots() -> list[str]:
    configured = parse_env_locations(os.environ.get("ACCESSIBLE_VOLUMES_ROOTS", str(DEFAULT_LIBRARY_DIR)))

    out: list[str] = []
    seen: set[str] = set()
    for item in configured:
        path = Path(str(item).strip())
        if not path.exists() or not path.is_dir():
            continue
        try:
            normalized = str(path.resolve())
        except Exception:
            normalized = str(path)
        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


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

    default_others = os.environ.get("DEFAULT_OTHERS_DOWNLOAD_LOCATION", "").strip()
    env_map = {
        "defaultGeneralDownloadLocation": default_others,
        "defaultYoutubeDownloadLocation": os.environ.get("DEFAULT_YOUTUBE_DOWNLOAD_LOCATION", "").strip()
        or default_others,
        "defaultFacebookDownloadLocation": os.environ.get("DEFAULT_FACEBOOK_DOWNLOAD_LOCATION", "").strip()
        or default_others,
        "defaultInstagramDownloadLocation": os.environ.get("DEFAULT_INSTAGRAM_DOWNLOAD_LOCATION", "").strip()
        or default_others,
        "defaultTiktokDownloadLocation": os.environ.get("DEFAULT_TIKTOK_DOWNLOAD_LOCATION", "").strip()
        or default_others,
        "defaultOthersDownloadLocation": default_others,
    }
    for key, value in env_map.items():
        normalized = normalize_allowed_location(value) if value else ""
        if normalized:
            cfg[key] = normalized
    return cfg


def get_default_general_location(cfg: dict[str, Any]) -> str:
    value = normalize_allowed_location(str(cfg.get("defaultGeneralDownloadLocation") or "").strip())
    if value:
        return value
    locations = normalize_download_locations(cfg)
    return locations[0] if locations else ""


SITE_DEFAULT_LOCATION_KEYS = {
    "youtube": "defaultYoutubeDownloadLocation",
    "facebook": "defaultFacebookDownloadLocation",
    "instagram": "defaultInstagramDownloadLocation",
    "tiktok": "defaultTiktokDownloadLocation",
    "others": "defaultOthersDownloadLocation",
}


def get_default_site_location(cfg: dict[str, Any], site: str) -> str:
    site = (site or "others").lower()
    key = SITE_DEFAULT_LOCATION_KEYS.get(site, "")
    value = normalize_allowed_location(str(cfg.get(key) or "").strip()) if key else ""
    return value or get_default_general_location(cfg)


def get_site_default_locations(cfg: dict[str, Any]) -> dict[str, str]:
    return {site: get_default_site_location(cfg, site) for site in SITE_KEYS}
