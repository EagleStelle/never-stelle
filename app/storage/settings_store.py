"""Settings persistence and Instagram authentication management."""

import json
import os
import pickle
import shutil
import threading
import time
from datetime import datetime, timezone
from http.cookiejar import Cookie, MozillaCookieJar
from pathlib import Path
from typing import Any
from copy import deepcopy

import instaloader
import requests
import yaml
from instaloader.exceptions import TwoFactorAuthRequiredException

from app.config import (
    APP_CONFIG_PATH,
    INSTAGRAM_PENDING_2FA_FILE,
    INSTAGRAM_RUNTIME_COOKIES_FILE,
    INSTAGRAM_SESSION_FILE,
    INSTAGRAM_STAGING_DIR,
    INSTAGRAM_UPLOADED_COOKIES_FILE,
    INSTAGRAM_YTDLP_COOKIES_FILE,
    SETTINGS_FILE,
    settings_lock,
)
from app.utils.templates import normalize_template_settings
from app.utils.url import detect_site_category


# ── Raw settings I/O ──────────────────────────────────────────────────────────

def load_saved_settings_file() -> dict[str, Any]:
    with settings_lock:
        if not SETTINGS_FILE.exists():
            return {}
        try:
            payload = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    with settings_lock:
        SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    invalidate_app_config_cache()


# ── Volume / location discovery ───────────────────────────────────────────────

def _parse_env_locations(raw: str) -> list[str]:
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
    configured = _parse_env_locations(os.environ.get("DOWNLOAD_LOCATIONS", ""))
    if not configured:
        configured = _parse_env_locations(os.environ.get("ACCESSIBLE_VOLUMES_ROOTS", "/library"))
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
    roots = discover_volume_roots()
    out: list[str] = []
    seen: set[str] = set()
    for root_value in roots:
        root = Path(root_value)
        try:
            candidates = [root]
            candidates.extend(sorted((c for c in root.rglob("*") if c.is_dir()), key=lambda p: str(p).lower()))
        except Exception:
            candidates = [root]
        for candidate in candidates:
            value = str(candidate)
            if value not in seen:
                seen.add(value)
                out.append(value)
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
    path = (path or "").strip()
    if not path:
        return False
    cfg = load_app_config()
    return path in normalize_download_locations(cfg)


# ── App config (YAML + env overrides) ────────────────────────────────────────

def build_runtime_config() -> dict[str, Any]:
    file_cfg: dict[str, Any] = {}
    config_path = Path(APP_CONFIG_PATH)
    if config_path.exists():
        try:
            file_cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
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
        "defaultYoutubeDownloadLocation": os.environ.get("DEFAULT_YOUTUBE_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultFacebookDownloadLocation": os.environ.get("DEFAULT_FACEBOOK_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultInstagramDownloadLocation": os.environ.get("DEFAULT_INSTAGRAM_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultTiktokDownloadLocation": os.environ.get("DEFAULT_TIKTOK_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultOthersDownloadLocation": default_others,
        "defaultIwaraDownloadLocation": os.environ.get("DEFAULT_IWARA_DOWNLOAD_LOCATION", "").strip(),
        "authorization": os.environ.get("IWARA_AUTH_TOKEN", "").strip(),
    }
    for key, value in env_map.items():
        if value:
            cfg[key] = value
    return cfg


_app_config_cache: dict[str, Any] = {}
_app_config_cache_ts: float = 0.0
_APP_CONFIG_TTL = 30.0
_app_config_lock = threading.Lock()


def invalidate_app_config_cache() -> None:
    global _app_config_cache_ts
    with _app_config_lock:
        _app_config_cache_ts = 0.0


def load_app_config() -> dict[str, Any]:
    global _app_config_cache, _app_config_cache_ts
    now = time.time()
    with _app_config_lock:
        if now - _app_config_cache_ts < _APP_CONFIG_TTL and _app_config_cache:
            return dict(_app_config_cache)
    cfg = build_runtime_config()
    with _app_config_lock:
        _app_config_cache = cfg
        _app_config_cache_ts = time.time()
    return dict(cfg)


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


# ── Per-site default locations ────────────────────────────────────────────────

from app.config import SITE_DEFAULT_LOCATION_KEYS  # noqa: E402


def get_default_general_location(cfg: dict[str, Any]) -> str:
    value = str(cfg.get("defaultGeneralDownloadLocation") or "").strip()
    locations = normalize_download_locations(cfg)
    return value if value else (locations[0] if locations else "")


def get_default_iwara_location(cfg: dict[str, Any]) -> str:
    value = str(cfg.get("defaultIwaraDownloadLocation") or "").strip()
    locations = normalize_download_locations(cfg)
    return value if value else (locations[0] if locations else "")


def get_default_site_location(cfg: dict[str, Any], site: str) -> str:
    site = (site or "others").lower()
    if site == "iwara":
        return get_default_iwara_location(cfg)
    key = SITE_DEFAULT_LOCATION_KEYS.get(site, "")
    value = normalize_allowed_location(str(cfg.get(key) or "").strip()) if key else ""
    return value if value else get_default_general_location(cfg)


def get_site_default_locations(cfg: dict[str, Any]) -> dict[str, str]:
    return {site: get_default_site_location(cfg, site) for site in ("youtube", "facebook", "instagram", "tiktok", "iwara", "others")}


def normalize_site_location_selection(raw: Any, cfg: dict[str, Any]) -> dict[str, str]:
    defaults = get_site_default_locations(cfg)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for site in ("youtube", "facebook", "instagram", "tiktok", "iwara", "others"):
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "")
    return out


# ── Effective settings ────────────────────────────────────────────────────────

def get_effective_template_settings() -> dict[str, str]:
    payload = load_saved_settings_file()
    return normalize_template_settings(payload.get("template_settings"))


def get_effective_saved_settings(cfg: dict[str, Any]) -> dict[str, Any]:
    payload = load_saved_settings_file()
    return {
        "site_locations": normalize_site_location_selection(payload.get("site_locations"), cfg),
        "save_mode": "device" if str(payload.get("save_mode") or "").strip().lower() == "device" else "nas",
        "template_settings": normalize_template_settings(payload.get("template_settings")),
        "instagram_auth": get_instagram_auth_status(),
        "instagram_ytdlp_cookies": get_instagram_ytdlp_cookies_status(),
    }


def persist_settings(
    cfg: dict[str, Any],
    raw_site_locations: Any,
    raw_save_mode: Any,
    raw_template_settings: Any = None,
) -> dict[str, Any]:
    existing = load_saved_settings_file()
    payload = dict(existing)
    payload.update({
        "site_locations": normalize_site_location_selection(raw_site_locations, cfg),
        "save_mode": "device" if str(raw_save_mode or "").strip().lower() == "device" else "nas",
        "template_settings": normalize_template_settings(raw_template_settings),
    })
    save_saved_settings_file(payload)
    return get_effective_saved_settings(cfg)


# ── Instagram auth settings ───────────────────────────────────────────────────

def normalize_instagram_identifier_type(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in {"username", "email", "phone"} else "username"


def normalize_instagram_auth_payload(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    identifier = str(source.get("identifier") or source.get("username") or "").strip()
    session_username = str(source.get("session_username") or "").strip()
    if not session_username and normalize_instagram_identifier_type(source.get("identifier_type")) == "username":
        session_username = identifier
    return {
        "identifier_type": normalize_instagram_identifier_type(
            source.get("identifier_type") or ("username" if source.get("username") else "")
        ),
        "identifier": identifier,
        "session_username": session_username,
        "password": str(source.get("password") or ""),
        "last_login_at": str(source.get("last_login_at") or "").strip(),
        "last_error": str(source.get("last_error") or "").strip(),
    }


def get_instagram_auth_settings() -> dict[str, str]:
    payload = load_saved_settings_file()
    auth = normalize_instagram_auth_payload(payload.get("instagram_auth"))
    if not auth["identifier"]:
        auth["identifier"] = str(os.environ.get("INSTAGRAM_USERNAME") or "").strip()
    if not auth["session_username"] and auth["identifier_type"] == "username":
        auth["session_username"] = auth["identifier"]
    if not auth["password"]:
        auth["password"] = str(os.environ.get("INSTAGRAM_PASSWORD") or "")
    return auth


def save_instagram_auth_settings(
    identifier_type: str,
    identifier: str,
    password: str,
    *,
    session_username: str = "",
    last_login_at: str = "",
    last_error: str = "",
) -> None:
    existing = load_saved_settings_file()
    existing["instagram_auth"] = {
        "identifier_type": normalize_instagram_identifier_type(identifier_type),
        "identifier": str(identifier or "").strip(),
        "session_username": str(session_username or "").strip(),
        "password": str(password or ""),
        "last_login_at": str(last_login_at or "").strip(),
        "last_error": str(last_error or "").strip(),
    }
    save_saved_settings_file(existing)


def update_instagram_auth_settings(
    *,
    identifier_type: str | None = None,
    identifier: str | None = None,
    session_username: str | None = None,
    password: str | None = None,
    last_login_at: str | None = None,
    last_error: str | None = None,
) -> dict[str, str]:
    current = get_instagram_auth_settings()
    if identifier_type is not None:
        current["identifier_type"] = normalize_instagram_identifier_type(identifier_type)
    if identifier is not None:
        current["identifier"] = str(identifier or "").strip()
    if session_username is not None:
        current["session_username"] = str(session_username or "").strip()
    if password is not None:
        current["password"] = str(password or "")
    if last_login_at is not None:
        current["last_login_at"] = str(last_login_at or "").strip()
    if last_error is not None:
        current["last_error"] = str(last_error or "").strip()
    if not current["session_username"] and current["identifier_type"] == "username":
        current["session_username"] = current["identifier"]
    save_instagram_auth_settings(
        current["identifier_type"],
        current["identifier"],
        current["password"],
        session_username=current["session_username"],
        last_login_at=current["last_login_at"],
        last_error=current["last_error"],
    )
    return current


def clear_instagram_pending_2fa() -> None:
    with settings_lock:
        try:
            if INSTAGRAM_PENDING_2FA_FILE.exists():
                INSTAGRAM_PENDING_2FA_FILE.unlink()
        except Exception:
            pass


def clear_instagram_auth_settings() -> None:
    existing = load_saved_settings_file()
    existing.pop("instagram_auth", None)
    save_saved_settings_file(existing)
    clear_instagram_pending_2fa()
    for path in (INSTAGRAM_SESSION_FILE, INSTAGRAM_RUNTIME_COOKIES_FILE):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def get_instagram_auth_status() -> dict[str, Any]:
    auth = get_instagram_auth_settings()
    session_saved = INSTAGRAM_SESSION_FILE.exists() and INSTAGRAM_SESSION_FILE.is_file()
    active_username = auth["session_username"] or auth["identifier"]
    return {
        "configured": bool(auth["identifier"] and (auth["password"] or session_saved)),
        "identifier_type": auth["identifier_type"],
        "identifier": auth["identifier"],
        "session_username": auth["session_username"],
        "username": active_username,
        "session_saved": session_saved,
        "pending_2fa": False,
        "last_login_at": auth["last_login_at"],
        "last_error": auth["last_error"],
    }


# ── Instagram cookies ─────────────────────────────────────────────────────────

def get_instagram_uploaded_cookie_metadata() -> dict[str, Any]:
    payload = load_saved_settings_file()
    filename = str(payload.get("instagram_cookies_filename") or "").strip()
    uploaded_at = str(payload.get("instagram_cookies_uploaded_at") or "").strip()
    has_uploaded_file = INSTAGRAM_UPLOADED_COOKIES_FILE.exists() and INSTAGRAM_UPLOADED_COOKIES_FILE.is_file()
    return {
        "configured": has_uploaded_file,
        "source": "uploaded" if has_uploaded_file else "none",
        "filename": filename if has_uploaded_file else "",
        "uploaded_at": uploaded_at if has_uploaded_file else "",
    }


def get_instagram_ytdlp_cookies_status() -> dict[str, Any]:
    uploaded = get_instagram_uploaded_cookie_metadata()
    if uploaded.get("configured"):
        return uploaded

    env_path = str(os.environ.get("YTDLP_INSTAGRAM_COOKIES") or os.environ.get("YTDLP_COOKIES") or "").strip()
    if env_path and Path(env_path).is_file():
        try:
            mtime = datetime.fromtimestamp(Path(env_path).stat().st_mtime, tz=timezone.utc).isoformat()
        except Exception:
            mtime = ""
        return {
            "configured": True,
            "source": "mounted",
            "filename": Path(env_path).name,
            "uploaded_at": mtime,
        }
    return {"configured": False, "source": "none", "filename": "", "uploaded_at": ""}


def clear_instagram_ytdlp_cookies_settings() -> None:
    existing = load_saved_settings_file()
    existing.pop("instagram_cookies_filename", None)
    existing.pop("instagram_cookies_uploaded_at", None)
    save_saved_settings_file(existing)


def save_instagram_ytdlp_cookies_upload(filename: str) -> None:
    existing = load_saved_settings_file()
    existing["instagram_cookies_filename"] = str(Path(filename or "cookies.txt").name)
    existing["instagram_cookies_uploaded_at"] = datetime.now(timezone.utc).isoformat()
    save_saved_settings_file(existing)


def find_instagram_cookies_source_file() -> str:
    uploaded_path = INSTAGRAM_YTDLP_COOKIES_FILE
    if uploaded_path.exists() and uploaded_path.is_file():
        try:
            if uploaded_path.stat().st_size > 0:
                return str(uploaded_path)
        except Exception:
            return str(uploaded_path)
    for env_name in ("YTDLP_INSTAGRAM_COOKIES", "YTDLP_COOKIES"):
        candidate = str(os.environ.get(env_name) or "").strip()
        if candidate and os.path.isfile(candidate):
            return candidate
    return ""


def load_instagram_session_cookie_values() -> dict[str, str]:
    if not INSTAGRAM_SESSION_FILE.exists() or not INSTAGRAM_SESSION_FILE.is_file():
        return {}
    try:
        with INSTAGRAM_SESSION_FILE.open("rb") as handle:
            payload = pickle.load(handle)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    cookies: dict[str, str] = {}
    for key, value in payload.items():
        name = str(key or "").strip()
        if not name:
            continue
        cookie_value = str(value or "")
        if not cookie_value:
            continue
        cookies[name] = cookie_value
    return cookies


def write_mozilla_cookie_file(path: Path, cookies: dict[str, str], *, domain: str = ".instagram.com") -> str:
    if not cookies:
        return ""
    path.parent.mkdir(parents=True, exist_ok=True)
    jar = MozillaCookieJar(str(path))
    expires_at = int(time.time()) + (180 * 24 * 60 * 60)
    for name, value in sorted((cookies or {}).items()):
        jar.set_cookie(Cookie(
            version=0, name=str(name).strip(),
            value=str(value).replace("\t", " ").replace("\r", " ").replace("\n", " "),
            port=None, port_specified=False,
            domain=domain, domain_specified=True, domain_initial_dot=domain.startswith("."),
            path="/", path_specified=True,
            secure=True, expires=expires_at,
            discard=False, comment=None, comment_url=None, rest={}, rfc2109=False,
        ))
    jar.save(ignore_discard=True, ignore_expires=True)
    return str(path)


def export_instagram_session_to_runtime_cookies() -> str:
    cookies = load_instagram_session_cookie_values()
    if not cookies:
        return ""
    try:
        return write_mozilla_cookie_file(INSTAGRAM_RUNTIME_COOKIES_FILE, cookies)
    except Exception:
        return ""


def prepare_runtime_cookies(source_url: str) -> str:
    site_category = detect_site_category(source_url)
    if site_category == "instagram":
        cookies_source = find_instagram_cookies_source_file()
        if cookies_source:
            try:
                INSTAGRAM_RUNTIME_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(cookies_source, INSTAGRAM_RUNTIME_COOKIES_FILE)
                return str(INSTAGRAM_RUNTIME_COOKIES_FILE)
            except Exception:
                return cookies_source
        exported_session = export_instagram_session_to_runtime_cookies()
        if exported_session and os.path.isfile(exported_session):
            return exported_session
        return ""
    candidate = str(os.environ.get("YTDLP_COOKIES") or "").strip()
    if candidate and os.path.isfile(candidate):
        return candidate
    return ""


def load_netscape_cookies_file(path: str) -> requests.cookies.RequestsCookieJar:
    jar = requests.cookies.RequestsCookieJar()
    if not path:
        return jar
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return jar
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        domain, _include_subdomains, cookie_path, secure_flag, expires_raw, name, value = parts[:7]
        try:
            expires = int(expires_raw)
        except Exception:
            expires = None
        try:
            cookie = requests.cookies.create_cookie(
                name=name, value=value, domain=domain,
                path=cookie_path or "/",
                secure=str(secure_flag or "").upper() == "TRUE",
                expires=expires,
            )
            jar.set_cookie(cookie)
        except Exception:
            continue
    return jar


# ── Instaloader client / auth ─────────────────────────────────────────────────

def create_instaloader_client(staging_root: Path) -> instaloader.Instaloader:
    return instaloader.Instaloader(
        quiet=True,
        dirname_pattern=str(staging_root / "{target}"),
        filename_pattern="{date_utc:%Y-%m-%d_%H-%M-%S_UTC}",
        download_pictures=True,
        download_videos=True,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        sanitize_paths=True,
    )


def try_instaloader_cookie_login(loader: instaloader.Instaloader) -> dict[str, Any] | None:
    cookie_path = find_instagram_cookies_source_file() or prepare_runtime_cookies("https://www.instagram.com/")
    if not cookie_path or not os.path.isfile(cookie_path):
        return None
    jar = load_netscape_cookies_file(cookie_path)
    if not jar:
        return None
    try:
        session = getattr(loader.context, "_session", None)
        if session is None or not isinstance(session, requests.Session):
            session = requests.Session()
            setattr(loader.context, "_session", session)
        session.cookies = jar
        session.headers.setdefault("Referer", "https://www.instagram.com/")
        session.headers.setdefault("User-Agent", "Mozilla/5.0")
        csrftoken = ""
        for cookie in jar:
            if cookie.name == "csrftoken" and "instagram" in str(cookie.domain or "").lower():
                csrftoken = cookie.value
                break
        if csrftoken:
            session.headers["X-CSRFToken"] = csrftoken
        logged_in_as = loader.test_login()
        if not logged_in_as:
            return None
        try:
            if hasattr(loader.context, "username"):
                setattr(loader.context, "username", logged_in_as)
        except Exception:
            pass
        try:
            loader.save_session_to_file(str(INSTAGRAM_SESSION_FILE))
        except Exception:
            pass
        update_instagram_auth_settings(
            session_username=logged_in_as,
            last_login_at=datetime.now(timezone.utc).isoformat(),
            last_error="",
        )
        return {"logged_in": True, "username": logged_in_as, "source": "cookies"}
    except Exception:
        return None


def ensure_instagram_login(
    loader: instaloader.Instaloader, *, require_login: bool = False
) -> dict[str, Any]:
    auth = get_instagram_auth_settings()
    identifier_type = auth["identifier_type"]
    identifier = auth["identifier"]
    password = auth["password"]
    session_username = auth["session_username"] or (identifier if identifier_type == "username" else "")
    last_error = ""

    clear_instagram_pending_2fa()

    if session_username and INSTAGRAM_SESSION_FILE.exists() and INSTAGRAM_SESSION_FILE.is_file():
        try:
            loader.load_session_from_file(session_username, str(INSTAGRAM_SESSION_FILE))
            logged_in_as = loader.test_login()
            if logged_in_as:
                update_instagram_auth_settings(session_username=logged_in_as, last_error="")
                return {"logged_in": True, "username": logged_in_as, "source": "session"}
        except Exception as exc:
            last_error = str(exc)

    cookie_login = try_instaloader_cookie_login(loader)
    if cookie_login:
        return cookie_login

    if identifier and password:
        login_username = identifier.strip()
        if login_username:
            try:
                loader.login(login_username, password)
                loader.save_session_to_file(str(INSTAGRAM_SESSION_FILE))
                actual_username = loader.test_login() or session_username or login_username
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=actual_username,
                    last_login_at=datetime.now(timezone.utc).isoformat(),
                    last_error="",
                )
                return {"logged_in": True, "username": actual_username, "source": "password"}
            except TwoFactorAuthRequiredException:
                last_error = "2FA-enabled Instagram accounts are not supported. Turn off 2FA on the account for this to work."
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=auth.get("session_username", ""),
                    last_error=last_error,
                )
                if require_login:
                    raise RuntimeError(last_error)
                return {"logged_in": False, "username": "", "source": "unsupported_2fa"}
            except Exception as exc:
                raw_error = str(exc).strip()
                if raw_error:
                    last_error = raw_error
                if last_error and "Unexpected null login result" in last_error:
                    hint = " Upload Instagram cookies in Settings and use the same logged-in account for a session-based fallback."
                    last_error = f"{last_error}.{hint}" if not last_error.endswith(hint) else last_error
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=auth.get("session_username", ""),
                    last_error=last_error,
                )
                cookie_login = try_instaloader_cookie_login(loader)
                if cookie_login:
                    return cookie_login
                if require_login:
                    raise RuntimeError(f"Instagram login failed: {last_error}")

    if require_login:
        raise RuntimeError(last_error or "Instagram login is required for this Instagram URL. Configure it in Settings.")
    if last_error:
        update_instagram_auth_settings(last_error=last_error)
    return {"logged_in": False, "username": "", "source": "public"}


# ── Settings response helpers ─────────────────────────────────────────────────

import hashlib  # noqa: E402


def build_settings_signature(cfg: dict[str, Any]) -> str:
    effective = get_effective_saved_settings(cfg)
    payload = {
        "download_locations": normalize_download_locations(cfg),
        "site_default_locations": effective.get("site_locations", {}),
        "template_settings": effective.get("template_settings", {}),
        "instagram_auth": effective.get("instagram_auth", {}),
        "instagram_ytdlp_cookies": effective.get("instagram_ytdlp_cookies", {}),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def build_settings_response(cfg: dict[str, Any], saved: dict[str, Any] | None = None) -> dict[str, Any]:
    saved = saved or get_effective_saved_settings(cfg)
    return {
        "download_locations": normalize_download_locations(cfg),
        "site_default_locations": saved.get("site_locations", {}),
        "save_mode": saved.get("save_mode", "nas"),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "instagram_auth": saved.get("instagram_auth", get_instagram_auth_status()),
        "instagram_ytdlp_cookies": saved.get("instagram_ytdlp_cookies", get_instagram_ytdlp_cookies_status()),
    }
