from __future__ import annotations

import subprocess
from pathlib import Path

from .constants import (
    AUDIO_BITRATE_PRESETS,
    audio_output_extension,
    is_lossless_audio,
    normalize_quality_selection,
)
from .naming import detect_ffmpeg_location


def _read_head(path: Path) -> bytes:
    try:
        with open(path, "rb") as handle:
            return handle.read(16)
    except OSError:
        return b""


def _is_adts_frame(head: bytes) -> bool:
    return head[0] == 0xFF and (head[1] & 0xF6) == 0xF0


def _is_mp3_frame(head: bytes) -> bool:
    return head[0] == 0xFF and (head[1] & 0xE0) == 0xE0 and (head[1] & 0x06) == 0x02


def audio_container_matches(path: Path, suffix: str) -> bool:
    """True when the bytes on disk are already the container `suffix` names."""
    head = _read_head(path)
    if len(head) < 12:
        return False
    key = str(suffix or "").strip().lower()
    if key == ".opus":
        return head[:4] == b"OggS"
    if key == ".flac":
        return head[:4] == b"fLaC"
    if key == ".wav":
        return head[:4] == b"RIFF" and head[8:12] == b"WAVE"
    if key == ".m4a":
        return head[4:8] == b"ftyp"
    if key == ".mp3":
        return head[:3] == b"ID3" or _is_mp3_frame(head)
    if key == ".aac":
        return _is_adts_frame(head)
    return False


def _encode_args(selection: dict[str, str]) -> list[str]:
    if is_lossless_audio(selection["audio_format"]):
        return []
    kbps = AUDIO_BITRATE_PRESETS[selection["audio_bitrate"]]["kbps"]
    return ["-b:a", f"{kbps}k"] if kbps else []


def _run_ffmpeg(ffmpeg: str, source: Path, target: Path, codec_args: list[str]) -> bool:
    cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(source), "-vn", *codec_args, str(target)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError):
        return False
    if result.returncode == 0 and target.is_file() and target.stat().st_size > 0:
        return True
    try:
        target.unlink(missing_ok=True)
    except OSError:
        pass
    return False


def convert_audio_output(source: Path, target: Path, quality: dict[str, str] | None) -> bool:
    """Rewrite `source` into the selected container, stream copying when the codec already fits."""
    selection = normalize_quality_selection(quality)
    if not audio_output_extension(selection) or not (ffmpeg := detect_ffmpeg_location()):
        return False
    # `target`'s extension picks the muxer, and on the encode pass its default encoder.
    if _run_ffmpeg(ffmpeg, source, target, ["-c:a", "copy"]):
        return True
    # The stream is in another codec, so the postprocessor never converted it.
    return _run_ffmpeg(ffmpeg, source, target, _encode_args(selection))
