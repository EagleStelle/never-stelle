from __future__ import annotations

from typing import Any

from backend.app.domains.access.rotation import AccessIdentity
from backend.app.domains.downloads.constants import PROGRESS_RE
from backend.app.domains.downloads.engines import gallerydl, ytdlp
from backend.app.domains.downloads.engines.probe import gallerydl_reads, ytdlp_single_video
from backend.app.domains.downloads.files import extract_downloaded_path
from backend.app.domains.downloads.store import load_route_facts
from backend.app.domains.formats.analysis import media_id_from_url


class Engine:
    """Downloader-backend contract the worker drives every task through."""

    name: str = ""
    needs_ffmpeg: bool = False
    # True when the backend reports its own byte-percentage (else count-based).
    emits_progress: bool = False
    # True when one post lands as several files (numbered images, a soundtrack) of one source item.
    bundles_post_files: bool = False
    # True when output lines and filenames leave template fields a metadata probe must fill.
    sparse_metadata: bool = False

    def reads(self, url: str) -> bool:
        """Whether an extractor of the backend takes the link, beyond reading any page generically."""
        return True

    def count_items(self, source_url: str) -> int:
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
        access: AccessIdentity | None = None,
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        part_directory: str = "",
        quality: dict[str, str] | None = None,
        post_processing: dict[str, Any] | None = None,
        cleaning: dict[str, Any] | None = None,
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
    needs_ffmpeg = True
    emits_progress = True

    def reads(self, url: str) -> bool:
        return ytdlp_single_video(url) is not None

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
        access: AccessIdentity | None = None,
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        part_directory: str = "",
        quality: dict[str, str] | None = None,
        post_processing: dict[str, Any] | None = None,
        cleaning: dict[str, Any] | None = None,
    ) -> list[str]:
        return ytdlp.build_ytdlp_command(
            source_url,
            ffmpeg_location,
            output_template,
            output_dir=output_dir,
            access=access,
            creator_sidecar=creator_sidecar,
            metadata_sidecar=metadata_sidecar,
            part_directory=part_directory,
            quality=quality,
            post_processing=post_processing,
            cleaning=cleaning,
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
    bundles_post_files = True
    sparse_metadata = True

    def reads(self, url: str) -> bool:
        return gallerydl_reads(url) is not None

    def count_items(self, source_url: str) -> int:
        # Counting for real means a second extraction pass, which costs a request and
        # sometimes a cookie. A URL naming one media item answers it for free; a
        # profile or tag falls back to the unknown-total curve as paths are emitted.
        return 1 if media_id_from_url(source_url) else 0

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
        access: AccessIdentity | None = None,
        creator_sidecar: str = "",
        metadata_sidecar: str = "",
        part_directory: str = "",
        quality: dict[str, str] | None = None,
        post_processing: dict[str, Any] | None = None,
        cleaning: dict[str, Any] | None = None,
    ) -> list[str]:
        return gallerydl.build_gallerydl_command(
            source_url,
            output_dir,
            output_template,
            access=access,
            metadata_sidecar=metadata_sidecar,
            part_directory=part_directory,
            quality=quality,
            post_processing=post_processing,
            cleaning=cleaning,
        )

    def parse_progress(self, line: str) -> float | None:
        match = PROGRESS_RE.search(str(line or ""))
        return float(match.group(1)) if match else None

    def extract_output_path(self, line: str) -> str:
        return gallerydl.extract_gallerydl_path(line) or extract_downloaded_path(line)

    def read_creator(self, sidecar_path: str, source_url: str) -> str:
        return ""


# Run order: gallery-dl brokers every URL (handing yt-dlp what it cannot fetch itself);
# standalone yt-dlp only runs when gallery-dl ends with no media.
_ENGINES: tuple[Engine, ...] = (GallerydlEngine(), YtdlpEngine())


# Downloads on a route before its answer reorders the engines.
_ENGINE_SAMPLES = 3
# Counts fade past this many, so a route whose site changed is re-learned within a few downloads.
ENGINE_WINDOW = 20


def all_engines() -> tuple[Engine, ...]:
    return _ENGINES


def engine_fact(engine: Engine) -> str:
    return f"engine:{engine.name}"


def _engine_rank(stats: dict[str, Any] | None) -> int:
    """0 when the engine gets media on the route, 1 while unknown, 2 when it mostly does not."""
    hits = int((stats or {}).get("hits") or 0)
    misses = int((stats or {}).get("misses") or 0)
    if hits + misses < _ENGINE_SAMPLES:
        return 1
    return 0 if hits >= misses else 2


def engine_order(shape: str) -> tuple[Engine, ...]:
    """Every engine, the ones that get media on this route shape first; ties keep the run order."""
    facts = load_route_facts(shape)
    return tuple(sorted(_ENGINES, key=lambda engine: _engine_rank(facts.get(engine_fact(engine)))))


def default_engine() -> Engine:
    return _ENGINES[0]
