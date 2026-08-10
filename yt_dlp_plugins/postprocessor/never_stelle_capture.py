from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from yt_dlp.postprocessor.common import PostProcessor


class NeverStelleCapturePP(PostProcessor):
    """Preserve delegated yt-dlp metadata for Never Stelle finalization."""

    def __init__(self, downloader: Any = None, directory: str = "") -> None:
        super().__init__(downloader)
        self._directory = Path(directory)

    def run(self, info: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
        filepath = str(info.get("filepath") or info.get("_filename") or "").strip()
        if not filepath or not self._directory:
            self.report_warning("Cannot capture extractor metadata without an output path and directory")
            return [], info

        self._directory.mkdir(parents=True, exist_ok=True)
        target = self._directory / f"{Path(filepath).name}.info.json"
        temporary = target.with_name(f".{target.name}.temp")
        payload = self._downloader.sanitize_info(info, remove_private_keys=False)
        try:
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        return [], info
