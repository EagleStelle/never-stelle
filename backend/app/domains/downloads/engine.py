from __future__ import annotations

from typing import Any

from . import gallerydl, ytdlp
from .constants import PROGRESS_RE
from .files import extract_downloaded_path


class Engine:
    """Downloader-backend contract the worker drives every task through."""

    name: str = ""
    id_prefix: str = ""
    needs_ffmpeg: bool = False
    # True when the backend reports its own byte-percentage (else count-based).
    emits_progress: bool = False

    def count_items(
        self,
        source_url: str,
        *,
        cookies_file: str = "",
        excluded_extensions: set[str] | None = None,
    ) -> int:
        return 0

    def build_output_template(
        self,
        source_url: str,
        output_dir: str,
        template_settings: dict[str, str] | None = None,
        quality: dict[str, str] | None = None,
        extra_tokens: dict[str, str] | None = None,
    ) -> str:
        raise NotImplementedError

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        cookies_file: str = "",
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        excluded_extensions: set[str] | None = None,
        quality: dict[str, str] | None = None,
    ) -> list[str]:
        raise NotImplementedError

    def parse_progress(self, line: str) -> float | None:
        return None

    def extract_output_path(self, line: str) -> str:
        return ""

    def read_creator(self, sidecar_path: str, source_url: str) -> str:
        return ""


class YtdlpEngine(Engine):
    name = "ytdlp"
    id_prefix = "ytdlp"
    needs_ffmpeg = True
    emits_progress = True

    def build_output_template(
        self,
        source_url: str,
        output_dir: str,
        template_settings: dict[str, str] | None = None,
        quality: dict[str, str] | None = None,
        extra_tokens: dict[str, str] | None = None,
    ) -> str:
        return ytdlp.build_output_template(source_url, output_dir, template_settings, quality, extra_tokens)

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        cookies_file: str = "",
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        excluded_extensions: set[str] | None = None,
        quality: dict[str, str] | None = None,
    ) -> list[str]:
        return ytdlp.build_ytdlp_command(
            source_url,
            ffmpeg_location,
            output_template,
            cookies_file=cookies_file,
            creator_sidecar=creator_sidecar,
            metadata_sidecar=metadata_sidecar,
            quality=quality,
        )

    def parse_progress(self, line: str) -> float | None:
        match = PROGRESS_RE.search(str(line or ""))
        return float(match.group(1)) if match else None

    def extract_output_path(self, line: str) -> str:
        return extract_downloaded_path(line)

    def read_creator(self, sidecar_path: str, source_url: str) -> str:
        return ytdlp.read_creator_sidecar(sidecar_path)


class GallerydlEngine(Engine):
    name = "gallerydl"
    id_prefix = "gallerydl"
    needs_ffmpeg = False
    emits_progress = False

    def count_items(
        self,
        source_url: str,
        *,
        cookies_file: str = "",
        excluded_extensions: set[str] | None = None,
    ) -> int:
        # Avoid a second gallery-dl extraction pass before every task. The worker
        # uses an unknown-total progress curve as file paths are emitted.
        return 0

    def build_output_template(
        self,
        source_url: str,
        output_dir: str,
        template_settings: dict[str, str] | None = None,
        quality: dict[str, str] | None = None,
        extra_tokens: dict[str, str] | None = None,
    ) -> str:
        return gallerydl.build_gallerydl_output_template(
            source_url, output_dir, template_settings, quality, extra_tokens
        )

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        cookies_file: str = "",
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        excluded_extensions: set[str] | None = None,
        quality: dict[str, str] | None = None,
    ) -> list[str]:
        return gallerydl.build_gallerydl_command(
            source_url,
            output_dir,
            output_template,
            cookies_file=cookies_file,
            metadata_sidecar=metadata_sidecar,
            excluded_extensions=excluded_extensions,
            quality=quality,
        )

    def parse_progress(self, line: str) -> float | None:
        match = PROGRESS_RE.search(str(line or ""))
        return float(match.group(1)) if match else None

    def extract_output_path(self, line: str) -> str:
        return gallerydl.extract_gallerydl_path(line) or extract_downloaded_path(line)

    def read_creator(self, sidecar_path: str, source_url: str) -> str:
        return ""


# gallery-dl is the default broker; yt-dlp remains available for legacy rows.
_YTDLP = YtdlpEngine()
_GALLERYDL = GallerydlEngine()
_ENGINES: tuple[Engine, ...] = (_GALLERYDL, _YTDLP)
_BY_NAME: dict[str, Engine] = {engine.name: engine for engine in _ENGINES}


def all_engines() -> tuple[Engine, ...]:
    return _ENGINES


def select_engine(source_url: str) -> Engine:
    return _GALLERYDL


def engine_by_name(name: str) -> Engine:
    return _BY_NAME.get(str(name or "").strip().lower(), _GALLERYDL)


def engine_for_task(task: dict[str, Any]) -> Engine:
    name = str(task.get("engine") or "").strip().lower()
    if name in _BY_NAME:
        return _BY_NAME[name]
    return select_engine(str(task.get("source_url") or ""))
