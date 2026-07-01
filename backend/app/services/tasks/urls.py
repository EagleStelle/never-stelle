from __future__ import annotations

from urllib.parse import urlparse


def canonicalize_source_url(source_url: str) -> str:
    source_url = str(source_url or "").strip()
    if not source_url:
        return ""
    if "://" not in source_url:
        source_url = f"https://{source_url}"
    try:
        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower()
        if host.endswith("instagram.com") or host.endswith("tiktok.com"):
            path = parsed.path or "/"
            if not path.endswith("/"):
                path += "/"
            return f"{parsed.scheme or 'https'}://{host}{path}"
    except Exception:
        return source_url
    return source_url


def detect_site_category(source_url: str) -> str:
    try:
        host = (urlparse(source_url).hostname or "").lower()
    except Exception:
        host = ""
    if host.endswith(("youtube.com", "youtu.be")):
        return "youtube"
    if host.endswith(("facebook.com", "fb.com", "fb.watch")):
        return "facebook"
    if host.endswith("instagram.com"):
        return "instagram"
    if host.endswith("tiktok.com"):
        return "tiktok"
    return "others"
