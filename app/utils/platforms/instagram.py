"""Instagram-specific context builders and file management helpers."""

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from app.utils.media import (
    choose_best_media_file,
    list_media_files,
    safe_component,
    unique_output_path,
)
from app.utils.templates import render_template_string
from app.utils.ytdlp import first_ytdlp_entry


_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "iwaradl-web-wrapper/1.0"})

INSTAGRAM_HIGHLIGHT_OWNER_PATTERNS = [
    re.compile(r'"owner_username":"([A-Za-z0-9._]+)"'),
    re.compile(r'"owner"\s*:\s*\{[^{}]*"username"\s*:\s*"([A-Za-z0-9._]+)"'),
    re.compile(r'"username":"([A-Za-z0-9._]+)"'),
    re.compile(r'https://www\.instagram\.com/stories/([A-Za-z0-9._]+)/'),
    re.compile(r'Stories\s*[•·-]\s*([A-Za-z0-9._]+)', re.IGNORECASE),
]


def to_str(value: Any) -> str:
    return "" if value is None else str(value)


# ── Instagram handle helpers ──────────────────────────────────────────────────

def _looks_like_instagram_handle(value: Any) -> bool:
    text = to_str(value).strip()
    if not text or text.isdigit():
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9._]+", text))


def _looks_like_numeric_instagram_id(value: Any) -> bool:
    text = to_str(value).strip()
    return bool(text) and text.isdigit()


def _pick_instagram_creator(existing: Any, *candidates: Any) -> str:
    existing_text = to_str(existing).strip()
    cleaned = [to_str(c).strip() for c in candidates if to_str(c).strip()]

    if _looks_like_instagram_handle(existing_text):
        return existing_text
    for candidate in cleaned:
        if _looks_like_instagram_handle(candidate):
            return candidate
    if existing_text and not _looks_like_numeric_instagram_id(existing_text):
        return existing_text
    for candidate in cleaned:
        if not _looks_like_numeric_instagram_id(candidate):
            return candidate
    return existing_text or (cleaned[0] if cleaned else "")


# ── Title helpers ─────────────────────────────────────────────────────────────

def instagram_title_from_text(value: Any, fallback: str) -> str:
    text = to_str(value)
    if text:
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text[:120]
    return fallback


def normalize_instagram_ytdlp_title(value: Any, fallback: str) -> str:
    text = to_str(value).strip()
    lowered = text.lower()
    generic_prefixes = (
        "instagram reel by ",
        "instagram video by ",
        "instagram post by ",
        "instagram highlight by ",
        "instagram story by ",
        "instagram photo by ",
        "story by ",
        "highlight by ",
        "stories by ",
    )
    if not text or lowered in {"reel", "highlight", "story", "instagram"} or lowered.startswith(generic_prefixes):
        return instagram_title_from_text("", fallback)
    return instagram_title_from_text(text, fallback)


# ── Context builders ──────────────────────────────────────────────────────────

def enrich_instagram_context_from_ytdlp_info(
    context: dict[str, Any],
    info: dict[str, Any],
    *,
    fallback_title: str,
) -> dict[str, Any]:
    merged = dict(context or {})
    entry = first_ytdlp_entry(info)
    existing_creator = to_str(merged.get("creator") or merged.get("author_nickname") or merged.get("author"))
    creator = _pick_instagram_creator(
        existing_creator,
        info.get("uploader"), info.get("channel"), info.get("playlist_uploader"),
        entry.get("uploader"), entry.get("channel"), entry.get("playlist_uploader"),
        info.get("uploader_id"), info.get("channel_id"), info.get("playlist_uploader_id"),
        entry.get("uploader_id"), entry.get("channel_id"), entry.get("playlist_uploader_id"),
    )
    video_id = (
        to_str(info.get("id") or info.get("display_id") or info.get("playlist_id"))
        or to_str(entry.get("id") or entry.get("display_id"))
        or to_str(merged.get("video_id") or merged.get("id"))
    )
    raw_title = (
        entry.get("description") or info.get("description") or info.get("title")
        or info.get("playlist_title") or entry.get("title") or merged.get("title")
    )
    title = normalize_instagram_ytdlp_title(raw_title, fallback_title)
    raw_title_text = to_str(raw_title).strip()
    if raw_title_text and video_id and raw_title_text == video_id:
        title = instagram_title_from_text("", fallback_title)

    if creator:
        merged["author"] = creator
        merged["author_nickname"] = creator
        merged["creator"] = creator
    if video_id:
        merged["video_id"] = video_id
        merged["id"] = video_id
    if title:
        merged["title"] = title
    if not merged.get("quality"):
        merged["quality"] = fallback_title
    return merged


def build_instagram_post_context(post, *, creator_hint: str = "") -> dict[str, Any]:
    creator = to_str(creator_hint or getattr(post, "owner_username", "") or getattr(post, "profile", ""))
    title = instagram_title_from_text(
        getattr(post, "title", "") or getattr(post, "caption", "") or getattr(post, "pcaption", ""),
        getattr(post, "shortcode", "instagram"),
    )
    publish_time = getattr(post, "date_utc", None) or getattr(post, "date_local", None)
    return {
        "title": title,
        "video_id": to_str(getattr(post, "shortcode", "")),
        "id": to_str(getattr(post, "shortcode", "")),
        "author": creator,
        "author_nickname": creator,
        "creator": creator,
        "quality": "video" if bool(getattr(post, "is_video", False)) else "image",
        "publish_time": publish_time,
    }


def build_instagram_profile_pic_context(username: str) -> dict[str, Any]:
    creator = to_str(username)
    return {
        "title": "profile picture",
        "video_id": "profile_pic",
        "id": "profile_pic",
        "author": creator,
        "author_nickname": creator,
        "creator": creator,
        "quality": "profile_picture",
        "publish_time": datetime.now(timezone.utc),
    }


def build_instagram_url_context(target: dict[str, str], *, fallback_title: str = "instagram") -> dict[str, Any]:
    creator = to_str(target.get("username", ""))
    video_id = to_str(
        target.get("story_id") or target.get("highlight_id") or target.get("shortcode") or creator or "instagram"
    )
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title, "video_id": video_id, "id": video_id,
        "author": creator, "author_nickname": creator, "creator": creator,
        "quality": fallback_title, "publish_time": None,
    }


def build_instagram_story_url_context(target: dict[str, str], *, fallback_title: str = "story") -> dict[str, Any]:
    username = to_str(target.get("username", ""))
    story_id = to_str(target.get("story_id", "") or username or "story")
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title, "video_id": story_id, "id": story_id,
        "author": username, "author_nickname": username, "creator": username,
        "quality": "video", "publish_time": None,
    }


def build_instagram_highlight_url_context(
    target: dict[str, str],
    source_url: str = "",
    *,
    fallback_title: str = "highlight",
) -> dict[str, Any]:
    username = to_str(target.get("username", "")) or resolve_instagram_highlight_owner_username(source_url)
    highlight_id = to_str(target.get("highlight_id", "") or username or "highlight")
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title, "video_id": highlight_id, "id": highlight_id,
        "author": username, "author_nickname": username, "creator": username,
        "quality": "highlight", "publish_time": None,
    }


def default_instagram_basename(context: dict[str, Any]) -> str:
    creator = safe_component(to_str(context.get("creator") or context.get("author_nickname") or context.get("author") or "Unknown"))
    title = safe_component(to_str(context.get("title") or context.get("id") or context.get("video_id") or "instagram"))
    media_id = safe_component(to_str(context.get("video_id") or context.get("id") or "NA"))
    return f"{creator} - {title} [{media_id}]"


# ── Filename building ─────────────────────────────────────────────────────────

def build_instagram_final_filename(
    filename_template: str,
    context: dict[str, Any],
    ext: str,
    *,
    index: int = 1,
    total: int = 1,
) -> str:
    file_ctx = dict(context or {})
    file_ctx["ext"] = ext
    rendered = (render_template_string(filename_template, file_ctx) or "").replace("/", "_").replace("\\", "_").strip()
    ext_with_dot = f".{ext.lower()}" if ext else ""
    stem = rendered[: -len(ext_with_dot)] if ext_with_dot and rendered.lower().endswith(ext_with_dot) else rendered
    if not stem:
        stem = default_instagram_basename(context)
    stem = safe_component(stem)
    if total > 1:
        stem = f"{stem} [{index}]"
    return f"{stem}.{ext}" if ext else stem


def build_instagram_archive_name(
    filename_template: str,
    context: dict[str, Any],
    *,
    total: int,
) -> str:
    rendered = (render_template_string(filename_template, dict(context or {}, ext="zip")) or "").replace("/", "_").replace("\\", "_").strip()
    stem = rendered[:-4] if rendered.lower().endswith(".zip") else rendered
    if not stem:
        stem = default_instagram_basename(context)
    stem = safe_component(stem)
    if total > 1:
        stem = f"{stem} [all]"
    return f"{stem}.zip"


def build_instagram_post_url(shortcode: str, *, mode: str = "post") -> str:
    shortcode = to_str(shortcode)
    if not shortcode:
        raise RuntimeError("Missing Instagram shortcode.")
    path_mode = "reel" if mode == "reel" else "p"
    return f"https://www.instagram.com/{path_mode}/{shortcode}/"


# ── File movement helpers ─────────────────────────────────────────────────────

def move_instagram_downloads(
    downloaded_files: list[Path],
    output_root: Path,
    folder_template: str,
    filename_template: str,
    context: dict[str, Any],
) -> tuple[list[Path], Path]:
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    moved: list[Path] = []
    total = len(downloaded_files)
    for index, src in enumerate(sorted(downloaded_files, key=lambda p: p.name.lower()), start=1):
        ext = src.suffix.lstrip(".").lower()
        final_name = build_instagram_final_filename(filename_template, context, ext, index=index, total=total)
        target_path = unique_output_path(target_dir / final_name)
        shutil.move(str(src), str(target_path))
        moved.append(target_path)
    return moved, target_dir


def summarize_instagram_paths(paths: list[Path], fallback_folder: str) -> tuple[str, str, str]:
    if len(paths) == 1:
        path = paths[0]
        return str(path), str(path.parent), path.name
    return "", fallback_folder, ""


def prepare_instagram_story_ytdlp_output(
    output_root: Path,
    folder_template: str,
    filename_template: str,
    target: dict[str, str],
    *,
    fallback_title: str = "story",
) -> tuple[dict[str, Any], Path, str, str]:
    context = build_instagram_story_url_context(target, fallback_title=fallback_title)
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    desired_name = build_instagram_final_filename(filename_template, context, "mp4")
    unique_target = unique_output_path(target_dir / desired_name)
    output_template = str(unique_target.with_suffix(".%(ext)s"))
    return context, target_dir, output_template, unique_target.name


def prepare_instagram_post_ytdlp_output(
    output_root: Path,
    folder_template: str,
    filename_template: str,
    context: dict[str, Any],
) -> tuple[Path, str, str]:
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    desired_name = build_instagram_final_filename(filename_template, context, "mp4")
    unique_target = unique_output_path(target_dir / desired_name)
    output_template = str(unique_target.with_suffix(".%(ext)s"))
    return target_dir, output_template, unique_target.name


def resolve_instagram_highlight_owner_username(source_url: str) -> str:
    try:
        response = _HTTP.get(source_url, timeout=20)
        response.raise_for_status()
        html = response.text or ""
    except Exception:
        return ""

    def clean_username(value: str) -> str:
        username = to_str(value).strip().strip(".@ ")
        if not _looks_like_instagram_handle(username):
            return ""
        if username.lower() in {"media", "stories", "story", "instagram"}:
            return ""
        return username

    for pattern in INSTAGRAM_HIGHLIGHT_OWNER_PATTERNS:
        for match in pattern.finditer(html):
            username = clean_username(match.group(1))
            if username:
                return username
    return ""


def append_instagram_log(task_id: str, message: str, *, progress_pct: float | None = None, **extra: Any) -> None:
    from app.storage.task_store import load_non_iwara_task, update_non_iwara_task
    current = load_non_iwara_task(task_id)
    log_lines = list(current.get("last_log_lines") or [])
    log_lines.append(str(message))
    log_lines = log_lines[-30:]
    payload: dict[str, Any] = {"last_log_lines": log_lines}
    if progress_pct is not None:
        payload["progress_pct"] = max(0.0, min(100.0, float(progress_pct)))
    payload.update(extra)
    update_non_iwara_task(task_id, **payload)
