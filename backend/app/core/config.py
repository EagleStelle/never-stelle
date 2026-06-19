from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = Path(os.environ.get("APP_DATA_DIR", PROJECT_ROOT / "data")).resolve()
FRONTEND_DIR = Path(os.environ.get("FRONTEND_DIR", PROJECT_ROOT / "frontend")).resolve()
APP_CONFIG_PATH = Path(os.environ.get("APP_CONFIG_PATH", "/config/config.yaml"))

DATA_DIR.mkdir(parents=True, exist_ok=True)

SITE_KEYS = ("youtube", "facebook", "instagram", "tiktok", "others")
SITE_LABELS = {
    "all": "All",
    "youtube": "YouTube",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "tiktok": "TikTok",
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
    configured = parse_env_locations(os.environ.get("DOWNLOAD_LOCATIONS", ""))
    if not configured:
        configured = parse_env_locations(
            os.environ.get("ACCESSIBLE_VOLUMES_ROOTS", str(PROJECT_ROOT / "library"))
        )

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

    default_others = (
        os.environ.get("DEFAULT_OTHERS_DOWNLOAD_LOCATION", "").strip()
        or os.environ.get("DEFAULT_GENERAL_DOWNLOAD_LOCATION", "").strip()
    )
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
