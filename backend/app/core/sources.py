from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

FALLBACK_SOURCE_KEY = "others"

_COMMON_SECOND_LEVEL_TLDS = {
    "ac",
    "co",
    "com",
    "edu",
    "gov",
    "net",
    "org",
}
_COMMON_HOST_PREFIXES = {"amp", "m", "mobile", "www"}
_SOURCE_KEY_RE = re.compile(r"[^a-z0-9]+")


def normalize_source_key(value: Any) -> str:
    key = str(value or "").strip().lower()
    key = _SOURCE_KEY_RE.sub("-", key).strip("-")
    return key or FALLBACK_SOURCE_KEY


def normalize_host(value: Any) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if "://" in host:
        try:
            host = urlparse(host).hostname or host
        except Exception:
            pass
    host = host.strip().strip(".").lower()
    return host[2:] if host.startswith("*.") else host


def host_from_url(source_url: str) -> str:
    source_url = str(source_url or "").strip()
    if not source_url:
        return ""
    if "://" not in source_url:
        source_url = f"https://{source_url}"
    try:
        host = normalize_host(urlparse(source_url).hostname or "")
    except Exception:
        return ""
    return "" if not host or any(char.isspace() for char in host) else host


def _display_host(host: str) -> str:
    parts = [part for part in normalize_host(host).split(".") if part]
    while len(parts) > 2 and parts[0] in _COMMON_HOST_PREFIXES:
        parts.pop(0)
    return ".".join(parts)


def apex_host(value: Any) -> str:
    # Registrable host with generic www/m/mobile/amp prefixes stripped; keeps other subdomains distinct.
    return _display_host(str(value or ""))


def _domain_stem(host: str) -> str:
    parts = [part for part in _display_host(host).split(".") if part]
    if not parts:
        return FALLBACK_SOURCE_KEY
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in _COMMON_SECOND_LEVEL_TLDS:
        return parts[-3]
    if len(parts) >= 2:
        return parts[-2]
    return parts[0]


def source_label_from_key(key: str) -> str:
    key = normalize_source_key(key)
    if key == FALLBACK_SOURCE_KEY:
        return "Others"
    words: list[str] = []
    for part in re.split(r"[-_\s]+", key):
        chunks = re.findall(r"[a-z]+|\d+", part, flags=re.IGNORECASE)
        words.append("".join(chunk if chunk.isdigit() else chunk.capitalize() for chunk in chunks) or part.capitalize())
    return " ".join(word for word in words if word).strip() or "Source"


def _normalize_hosts(raw: Any) -> list[str]:
    if isinstance(raw, str):
        values: Iterable[Any] = [raw]
    elif isinstance(raw, Iterable) and not isinstance(raw, dict):
        values = raw
    else:
        values = []
    out: list[str] = []
    for value in values:
        host = normalize_host(value)
        if host and host not in out:
            out.append(host)
    return out


def favicon_url_for_host(host: str) -> str:
    host = _display_host(host)
    return f"https://www.google.com/s2/favicons?domain={host}&sz=64" if host else ""


def normalize_source_profile(raw: Any, fallback_key: str = "") -> dict[str, Any] | None:
    if isinstance(raw, str):
        raw = {"key": raw}
    if not isinstance(raw, dict):
        return None

    hosts = _normalize_hosts(raw.get("hosts") or raw.get("domains") or raw.get("host") or raw.get("domain"))
    key = normalize_source_key(
        raw.get("key")
        or raw.get("id")
        or fallback_key
        or _domain_stem(hosts[0] if hosts else "")
    )
    label = str(raw.get("label") or raw.get("name") or source_label_from_key(key)).strip()
    icon = str(raw.get("icon") or "").strip()
    icon_url = str(raw.get("icon_url") or raw.get("iconUrl") or "").strip()
    if not icon_url and hosts:
        icon_url = favicon_url_for_host(hosts[0])

    profile = {
        "key": key,
        "label": label or source_label_from_key(key),
        "hosts": hosts,
        "icon": icon,
        "icon_url": icon_url,
    }
    for source_name, target_name in (
        ("default_download_location", "default_download_location"),
        ("defaultDownloadLocation", "default_download_location"),
        ("download_location", "default_download_location"),
        ("downloadLocation", "default_download_location"),
        ("folder_template", "folder_template"),
        ("folderTemplate", "folder_template"),
        ("filename_template", "filename_template"),
        ("filenameTemplate", "filename_template"),
    ):
        if source_name in raw:
            profile[target_name] = str(raw.get(source_name) or "").strip()
    return profile


def _iter_profile_items(raw: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(raw, dict):
        for key, value in raw.items():
            yield str(key), value
    elif isinstance(raw, Iterable) and not isinstance(raw, str | bytes):
        for value in raw:
            yield "", value


def merge_source_profiles(*sources: Any, include_fallback: bool = False) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for source in sources:
        for fallback_key, raw in _iter_profile_items(source):
            profile = normalize_source_profile(raw, fallback_key)
            if not profile:
                continue
            key = profile["key"]
            if key not in merged:
                merged[key] = profile
                order.append(key)
                continue

            current = merged[key]
            hosts = list(current.get("hosts") or [])
            for host in profile.get("hosts") or []:
                if host not in hosts:
                    hosts.append(host)
            current.update({k: v for k, v in profile.items() if k != "hosts" and v not in ("", [], None)})
            current["hosts"] = hosts

    if FALLBACK_SOURCE_KEY in order:
        order = [key for key in order if key != FALLBACK_SOURCE_KEY] + [FALLBACK_SOURCE_KEY]
    elif include_fallback and FALLBACK_SOURCE_KEY not in merged:
        fallback = normalize_source_profile({"key": FALLBACK_SOURCE_KEY, "label": "Others"}) or {}
        merged[FALLBACK_SOURCE_KEY] = fallback
        order.append(FALLBACK_SOURCE_KEY)

    return [merged[key] for key in order]


def source_key_from_url(source_url: str, profiles: Iterable[dict[str, Any]] | None = None) -> str:
    host = host_from_url(source_url)
    if not host:
        return FALLBACK_SOURCE_KEY
    for profile in profiles or ():
        for pattern in profile.get("hosts") or []:
            pattern = normalize_host(pattern)
            if pattern and (host == pattern or host.endswith(f".{pattern}")):
                return normalize_source_key(profile.get("key"))
    return normalize_source_key(_domain_stem(host))


def source_profile_for_url(source_url: str, profiles: Iterable[dict[str, Any]] | None = None) -> dict[str, Any]:
    profiles = list(profiles or ())
    key = source_key_from_url(source_url, profiles)
    host = host_from_url(source_url)
    for profile in profiles:
        if normalize_source_key(profile.get("key")) == key:
            out = dict(profile)
            out["key"] = key
            hosts = list(out.get("hosts") or [])
            if host and host not in hosts:
                hosts.append(host)
            out["hosts"] = hosts
            if not out.get("icon_url") and host:
                out["icon_url"] = favicon_url_for_host(host)
            return out
    return {
        "key": key,
        "label": source_label_from_key(key),
        "hosts": [host] if host else [],
        "icon": "",
        "icon_url": favicon_url_for_host(host),
    }
