from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.domains.downloads.postprocessing.ffmpeg import _ffprobe_streams, _run_ffmpeg
from backend.app.domains.downloads.postprocessing.options import SUBTITLE_LANGUAGES_ALL
from backend.app.domains.downloads.postprocessing.payloads import _publish_bytes
from backend.app.domains.downloads.postprocessing.session import _header_map
from backend.app.domains.downloads.postprocessing.thumbnails import _MP4_EXTENSIONS
from backend.app.runtime.processes import raise_if_cancelled
from backend.app.runtime.scratch import remove_scratch_path, scratch_temp_dir, scratch_temp_path

logger = logging.getLogger(__name__)


_SUBTITLE_BUNDLE_BATCH_SIZE = 24


_SUBTITLE_FORMAT_PREFERENCE = ("vtt", "srt", "ass", "ssa", "ttml")


def _ordered_subtitle_languages(
    payload: dict[str, Any],
    captions: dict[str, Any],
    *,
    automatic: bool,
    languages: list[str] | None = None,
) -> list[str]:
    from yt_dlp.utils import ISO639Utils

    available = [
        str(language).strip()
        for language in captions
        if str(language).strip() and str(language).strip().lower() != "live_chat"
    ]
    source_language = str(payload.get("language") or "").strip()

    def variants(request: str) -> list[str]:
        """One requested code expanded to the tracks that satisfy it."""
        wanted = request.casefold()
        if not wanted:
            return []
        # Sites tag the same language as `en`, `en-US` or `eng-US`.
        bases = {wanted, str(ISO639Utils.short2long(wanted) or "").casefold()} - {""}
        exact = [language for language in available if language.casefold() == wanted]
        regional = [
            language
            for language in available
            if language not in exact and language.casefold().partition("-")[0] in bases
        ]
        # yt-dlp exposes translated automatic captions for hundreds of languages.
        # The `*-orig` track is the actual ASR output; preferring the plain code
        # selected a translated endpoint that commonly answers HTTP 429 or empty.
        if not automatic:
            return exact + regional
        original = [language for language in regional if language.casefold().endswith("-orig")]
        return original + exact + [language for language in regional if language not in original]

    requested = [text for language in (languages or []) if (text := str(language).strip())]
    if any(language.casefold() == SUBTITLE_LANGUAGES_ALL for language in requested):
        every_original = (
            [language for language in available if language.casefold().endswith("-orig")]
            if automatic
            else []
        )
        selected = [*variants(source_language), *every_original, *variants("en"), *available]
    elif requested:
        selected = [name for language in requested for name in variants(language)]
    else:
        # Default to the source track alone. Embedding every translated caption
        # is what made this path slow enough to be rate limited.
        selected = (variants(source_language) or variants("en") or available[:1])[:1]

    ordered: list[str] = []
    for language in selected:
        if language not in ordered:
            ordered.append(language)
    return ordered


def _ordered_subtitle_formats(formats: Any) -> list[dict[str, Any]]:
    candidates = [item for item in formats if isinstance(item, dict)] if isinstance(formats, list) else []
    ordered: list[dict[str, Any]] = []
    for extension in _SUBTITLE_FORMAT_PREFERENCE:
        matches = [item for item in candidates if str(item.get("ext") or "").lower() == extension]
        ordered.extend(reversed(matches))
    ordered.extend(item for item in reversed(candidates) if item not in ordered)
    return ordered


def _subtitle_extension(item: dict[str, Any]) -> str:
    extension = str(item.get("ext") or "").strip().lower().lstrip(".")
    if not extension:
        extension = Path(urlparse(str(item.get("url") or "")).path).suffix.lower().lstrip(".")
    extension = "".join(character for character in extension if character.isalnum())
    return extension or "vtt"


def _fetch_subtitle(ydl: Any, payload: dict[str, Any], item: dict[str, Any]) -> bytes:
    """Download one subtitle format with yt-dlp, which handles every subtitle protocol."""
    headers = {**_header_map(payload), **_header_map(item)}
    if source_url := str(payload.get("webpage_url") or payload.get("original_url") or "").strip():
        headers.setdefault("Referer", source_url)
    download = {**item, "http_headers": headers} if headers else dict(item)
    workspace = scratch_temp_dir(prefix="nvs-subtitle-download-")
    try:
        target = workspace / f"subtitle.{_subtitle_extension(item)}"
        ydl.dl(str(target), download, subtitle=True)
        return target.read_bytes()
    finally:
        remove_scratch_path(workspace)


def _safe_subtitle_language(language: str) -> str:
    value = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in str(language).strip()
    ).strip("-_")
    return value or "und"


def _subtitle_track(
    ydl: Any, payload: dict[str, Any], language: str, item: dict[str, Any], *, automatic: bool
) -> dict[str, Any] | None:
    """One caption format as a track, inline data as-is or downloaded by yt-dlp."""
    from yt_dlp.utils import YoutubeDLError

    data = item["data"].encode("utf-8") if isinstance(item.get("data"), str) else b""
    if not data and str(item.get("url") or "").strip():
        try:
            raise_if_cancelled()
            data = _fetch_subtitle(ydl, payload, item)
        except (OSError, TypeError, ValueError, YoutubeDLError) as exc:
            raise_if_cancelled()
            label = "Auto-generated subtitle" if automatic else "Subtitle"
            logger.warning("%s extraction skipped for %s: %s", label, language, exc)
    if not data:
        return None
    return {
        "language": _safe_subtitle_language(language),
        "automatic": automatic,
        "extension": _subtitle_extension(item),
        "data": data,
    }


def _subtitle_tracks(
    ydl: Any, payload: dict[str, Any], *, manual: bool, automatic: bool, languages: list[str]
) -> list[dict[str, Any]]:
    tracks: list[dict[str, Any]] = []
    for key, is_automatic in (("subtitles", False), ("automatic_captions", True)):
        captions = payload.get(key)
        if not (automatic if is_automatic else manual) or not isinstance(captions, dict):
            continue
        for language in _ordered_subtitle_languages(payload, captions, automatic=is_automatic, languages=languages):
            # Entries for one language are alternate formats of the same captions.
            for item in _ordered_subtitle_formats(captions.get(language))[:2]:
                if track := _subtitle_track(ydl, payload, language, item, automatic=is_automatic):
                    tracks.append(track)
                    break
    return tracks


def _write_subtitle_sidecar(path: Path, track: dict[str, Any]) -> Path:
    automatic = ".auto" if track["automatic"] else ""
    name = f"{path.stem}.{track['language']}{automatic}.{track['extension']}"
    return _publish_bytes(path.with_name(name), track["data"])


def _materialize_subtitle_track(track: dict[str, Any]) -> Path:
    subtitle_path = scratch_temp_path(prefix="nvs-subtitle-input-", suffix=f".{track['extension']}")
    subtitle_path.write_bytes(track["data"])
    return subtitle_path


def _subtitle_codec(path: Path) -> str:
    # ISO BMFF cannot mux WebVTT/SRT/ASS; Matroska and WebM keep the acquired codec.
    return "mov_text" if path.suffix.lower() in _MP4_EXTENSIONS else "copy"


def _subtitle_stream_count(ffmpeg: str, path: Path) -> int:
    return sum(
        1 for stream in _ffprobe_streams(ffmpeg, path) if stream.get("codec_type") == "subtitle"
    )


def _subtitle_stream_metadata(cmd: list[str], stream_index: int, track: dict[str, Any]) -> None:
    from yt_dlp.utils import ISO639Utils

    language = track["language"]
    title = language + (" (auto-generated)" if track["automatic"] else "")
    # MP4 drops anything but ISO 639-2, which every container accepts.
    base = language.partition("-")[0]
    code = ISO639Utils.short2long(base) or (base if len(base) == 3 else language)
    cmd.extend([f"-metadata:s:s:{stream_index}", f"language={code}"])
    cmd.extend([f"-metadata:s:s:{stream_index}", f"title={title}"])
    # MP4/MOV exposes the subtitle picker label through handler_name rather
    # than the generic title tag. Matroska/WebM safely preserve it as well.
    cmd.extend([f"-metadata:s:s:{stream_index}", f"handler_name={title}"])


def _build_subtitle_bundle(ffmpeg: str, tracks: list[dict[str, Any]]) -> Path | None:
    """Package every track into one Matroska carrier, batched under Windows' command-line limit."""
    bundle: Path | None = None
    kept: Path | None = None
    staged: list[Path] = []
    try:
        for start in range(0, len(tracks), _SUBTITLE_BUNDLE_BATCH_SIZE):
            batch = tracks[start : start + _SUBTITLE_BUNDLE_BATCH_SIZE]
            subtitle_paths = [_materialize_subtitle_track(track) for track in batch]
            staged.extend(subtitle_paths)
            next_bundle = scratch_temp_path(prefix="nvs-subtitle-bundle-", suffix=".mkv")
            staged.append(next_bundle)
            sources = [bundle, *subtitle_paths] if bundle is not None else subtitle_paths
            cmd = [ffmpeg, "-y", "-loglevel", "error", *(arg for source in sources for arg in ("-i", str(source)))]
            first_track_input = 1 if bundle is not None else 0
            if bundle is not None:
                cmd.extend(["-map", "0:s"])
            for offset in range(len(subtitle_paths)):
                cmd.extend(["-map", f"{first_track_input + offset}:0"])
            cmd.extend(["-c:s", "copy"])
            for offset, track in enumerate(batch):
                _subtitle_stream_metadata(cmd, start + offset, track)
            produced, detail = _run_ffmpeg([*cmd, str(next_bundle)], next_bundle)
            if not produced or _subtitle_stream_count(ffmpeg, next_bundle) != start + len(batch):
                logger.warning("Subtitle embed skipped: %s", detail or "the bundle kept too few subtitle streams")
                return None
            bundle = next_bundle
        kept = bundle
        return kept
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Subtitle embed skipped: %s", exc)
        return None
    finally:
        for path in staged:
            if path != kept:
                remove_scratch_path(path)
