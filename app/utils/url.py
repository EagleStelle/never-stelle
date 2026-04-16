"""URL detection, normalization, and parsing utilities."""

import re
from urllib.parse import parse_qs, unquote, urlparse

import requests

from app.config import PROFILE_RE, VIDEO_ID_RE


# ── Shared HTTP session (lightweight, no auth) ───────────────────────────────
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "iwaradl-web-wrapper/1.0"})


# ── Basic string / path helpers ───────────────────────────────────────────────

def to_str(value) -> str:
    return "" if value is None else str(value)


# ── Site detection ────────────────────────────────────────────────────────────

def is_youtube_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("youtube.com") or host.endswith("youtu.be") or host.endswith("youtube-nocookie.com")


def is_facebook_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("facebook.com") or host.endswith("fb.watch") or host.endswith("fb.com")


def is_instagram_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("instagram.com")


def is_tiktok_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("tiktok.com")


def is_iwara_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("iwara.tv") or host.endswith("iwara.com")


def is_rule34video_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("rule34video.com")


def detect_site_category(url: str) -> str:
    if is_iwara_url(url):
        return "iwara"
    if is_youtube_url(url):
        return "youtube"
    if is_facebook_url(url):
        return "facebook"
    if is_instagram_url(url):
        return "instagram"
    if is_tiktok_url(url):
        return "tiktok"
    return "others"


# ── ID / slug extraction ──────────────────────────────────────────────────────

def extract_video_id(url: str) -> str:
    match = VIDEO_ID_RE.search(url)
    return match.group(1) if match else ""


def extract_profile_slug(url: str) -> str:
    match = PROFILE_RE.search(url)
    if not match:
        return ""
    return match.group(1).strip("/")


# ── Instagram URL parsing ─────────────────────────────────────────────────────

def parse_instagram_target(source_url: str) -> dict[str, str]:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise RuntimeError("Unsupported Instagram URL.")

    if parts[0] == "p" and len(parts) >= 2:
        return {"mode": "post", "shortcode": parts[1]}
    if parts[0] in {"reel", "reels"} and len(parts) >= 2:
        return {"mode": "reel", "shortcode": parts[1]}
    if parts[0] == "tv" and len(parts) >= 2:
        return {"mode": "post", "shortcode": parts[1]}
    if parts[0] == "stories":
        if len(parts) >= 3 and parts[1] == "highlights":
            return {"mode": "highlight", "highlight_id": parts[2]}
        if len(parts) >= 2:
            out: dict[str, str] = {"mode": "stories", "username": parts[1]}
            if len(parts) >= 3:
                out["story_id"] = parts[2]
            return out
        raise RuntimeError("Unsupported Instagram stories URL.")

    if parts[0] in {"explore", "accounts", "about", "developer", "privacy", "legal", "direct"}:
        raise RuntimeError("This Instagram URL type is not supported yet.")

    username = parts[0]
    if len(parts) == 1:
        return {"mode": "profile", "username": username}
    if parts[1] == "tagged":
        return {"mode": "tagged", "username": username}
    if parts[1] == "reels":
        return {"mode": "profile_reels", "username": username}
    if parts[1] in {"channel", "igtv"}:
        return {"mode": "igtv", "username": username}
    return {"mode": "profile", "username": username}


# ── Facebook redirect resolution ──────────────────────────────────────────────

def resolve_facebook_redirect_url(source_url: str, *, max_hops: int = 3) -> str:
    current = str(source_url or "").strip()
    for _ in range(max_hops):
        if not current:
            return ""
        try:
            parsed = urlparse(current)
            host = (parsed.hostname or "").lower()
            if not (host.endswith("facebook.com") or host.endswith("fb.com") or host.endswith("fb.watch")):
                return current

            if parsed.path == "/login.php":
                next_url = parse_qs(parsed.query).get("next", [""])[0].strip()
                if next_url:
                    current = unquote(next_url)
                    continue
                return current

            parts = [part for part in parsed.path.split("/") if part]
            if parts and parts[0] == "share":
                try:
                    response = HTTP.get(current, timeout=15, allow_redirects=True)
                    response.raise_for_status()
                    final_url = str(response.url or "").strip()
                    if final_url and final_url != current:
                        current = final_url
                        continue
                except Exception:
                    return current

            return current
        except Exception:
            return current
    return current


# ── URL canonicalization ──────────────────────────────────────────────────────

def canonicalize_source_url(source_url: str) -> str:
    source_url = str(source_url or "").strip().replace("\r", "").replace("\n", "")
    if not source_url:
        return ""
    try:
        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower()
        if host in {"youtu.be"}:
            video_id = parsed.path.strip("/")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        if host.endswith("youtube.com"):
            parts = [part for part in parsed.path.split("/") if part]
            if parsed.path == "/watch":
                video_id = parse_qs(parsed.query).get("v", [""])[0].strip()
                if video_id:
                    return f"https://www.youtube.com/watch?v={video_id}"
            if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"}:
                video_id = parts[1].strip()
                if video_id:
                    return f"https://www.youtube.com/watch?v={video_id}"
        if host.endswith("instagram.com"):
            parts = [part for part in parsed.path.split("/") if part]
            if not parts:
                return "https://www.instagram.com/"
            try:
                target = parse_instagram_target(source_url)
                mode = target.get("mode")
                if mode == "post":
                    shortcode = str(target.get("shortcode") or "").strip()
                    if shortcode:
                        return f"https://www.instagram.com/p/{shortcode}/"
                if mode == "reel":
                    shortcode = str(target.get("shortcode") or "").strip()
                    if shortcode:
                        return f"https://www.instagram.com/reel/{shortcode}/"
                if mode == "highlight":
                    highlight_id = str(target.get("highlight_id") or "").strip()
                    if highlight_id:
                        return f"https://www.instagram.com/stories/highlights/{highlight_id}/"
                if mode == "stories":
                    username = str(target.get("username") or "").strip()
                    if username:
                        story_id = str(target.get("story_id") or "").strip()
                        suffix = f"/{story_id}" if story_id else ""
                        return f"https://www.instagram.com/stories/{username}{suffix}/"
                if mode == "profile":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/"
                if mode == "tagged":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/tagged/"
                if mode == "profile_reels":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/reels/"
                if mode == "igtv":
                    username = str(target.get("username") or "").strip()
                    if username:
                        section = parts[1] if len(parts) >= 2 and parts[1] in {"channel", "igtv"} else "channel"
                        return f"https://www.instagram.com/{username}/{section}/"
            except Exception:
                pass
            normalized_path = "/" + "/".join(parts)
            if normalized_path != "/":
                normalized_path += "/"
            return f"https://www.instagram.com{normalized_path}"
        if host.endswith("tiktok.com"):
            parts = [part for part in parsed.path.split("/") if part]
            normalized_host = host if host in {"vm.tiktok.com", "vt.tiktok.com"} else "www.tiktok.com"
            normalized_path = "/"
            if parts:
                if parts[0].startswith("@") and len(parts) >= 3 and parts[1] in {"video", "photo"}:
                    normalized_path = f"/{parts[0]}/{parts[1]}/{parts[2]}/"
                else:
                    normalized_path = "/" + "/".join(parts) + "/"
            return f"https://{normalized_host}{normalized_path}"
        if host.endswith("facebook.com") or host.endswith("fb.com") or host.endswith("fb.watch"):
            return resolve_facebook_redirect_url(source_url)
    except Exception:
        return source_url
    return source_url
