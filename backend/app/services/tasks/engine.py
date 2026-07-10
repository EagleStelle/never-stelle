from __future__ import annotations

from typing import Any

from . import gallerydl, ytdlp
from .constants import PROGRESS_RE
from .files import extract_downloaded_path
from .formats import creator_from_url


class Engine:
    """Downloader-backend contract the worker drives every task through."""

    name: str = ""
    id_prefix: str = ""
    needs_ffmpeg: bool = False
    # True when the backend streams its own byte-percentage (yt-dlp). False
    # backends get count-based progress: one tick per completed file.
    emits_progress: bool = False

    def matches(self, source_url: str) -> bool:
        raise NotImplementedError

    def count_items(self, source_url: str) -> int:
        # Total files this URL will yield, for count-based progress. 0 = unknown.
        return 0

    def build_output_template(self, source_url: str, output_dir: str) -> str:
        raise NotImplementedError

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        with_cookies: bool = False,
        creator_sidecar: str = "",
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

    def matches(self, source_url: str) -> bool:
        return True  # catch-all default; handles anything not claimed first

    def build_output_template(self, source_url: str, output_dir: str) -> str:
        return ytdlp.build_output_template(source_url, output_dir)

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        with_cookies: bool = False,
        creator_sidecar: str = "",
    ) -> list[str]:
        return ytdlp.build_ytdlp_command(
            source_url,
            ffmpeg_location,
            output_template,
            with_cookies=with_cookies,
            creator_sidecar=creator_sidecar,
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

    def matches(self, source_url: str) -> bool:
        return gallerydl.supports(source_url)

    def count_items(self, source_url: str) -> int:
        return gallerydl.count_gallerydl_items(source_url)

    def build_output_template(self, source_url: str, output_dir: str) -> str:
        return gallerydl.build_gallerydl_output_template(source_url, output_dir)

    def build_command(
        self,
        source_url: str,
        *,
        output_dir: str,
        ffmpeg_location: str,
        output_template: str,
        with_cookies: bool = False,
        creator_sidecar: str = "",
    ) -> list[str]:
        return gallerydl.build_gallerydl_command(
            source_url,
            output_dir,
            output_template,
            with_cookies=with_cookies,
        )

    def extract_output_path(self, line: str) -> str:
        return gallerydl.extract_gallerydl_path(line)

    def read_creator(self, sidecar_path: str, source_url: str) -> str:
        return creator_from_url(source_url)


# gallery-dl claims image hosts first; yt-dlp is the catch-all default last.
_YTDLP = YtdlpEngine()
_ENGINES: tuple[Engine, ...] = (GallerydlEngine(), _YTDLP)
_BY_NAME: dict[str, Engine] = {engine.name: engine for engine in _ENGINES}


def select_engine(source_url: str) -> Engine:
    for engine in _ENGINES:
        if engine.matches(source_url):
            return engine
    return _YTDLP


def engine_by_name(name: str) -> Engine:
    return _BY_NAME.get(str(name or "").strip().lower(), _YTDLP)


def engine_for_task(task: dict[str, Any]) -> Engine:
    name = str(task.get("engine") or "").strip().lower()
    if name in _BY_NAME:
        return _BY_NAME[name]
    return select_engine(str(task.get("source_url") or ""))
