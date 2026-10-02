from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.db.database as database_module
import backend.app.domains.downloads.links.urls as urls_module
from backend.app.domains.downloads.engines.engine import Engine, all_engines
from backend.app.domains.downloads.workers.completion.finalize import FinalizedCompletionOutput
from backend.app.domains.formats.learning import learn_download


def engine_by_name(name: str) -> Engine:
    """Pick one backend to exercise. Production runs the default or the whole run order."""
    return next(engine for engine in all_engines() if engine.name == name)


def use_temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Closed first: the shared connection would otherwise keep serving the old file.
    database_module.close_database()
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)


def patch_head(monkeypatch, final_url=None, exc=None):
    def fake_head(url, **kwargs):
        if exc is not None:
            raise exc
        return type("Resp", (), {"url": final_url if final_url is not None else url})()

    monkeypatch.setattr(urls_module.httpx, "head", fake_head)


def finalized_for(media: Path, title: str = "Title") -> FinalizedCompletionOutput:
    return FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title=title,
        keep_paths=[media],
    )


YTDLP_VIDEO_INFO = {
    "id": "abc",
    "extractor_key": "TikTok",
    "track": "original sound",
    "thumbnails": [{"url": "https://cdn.test/cover.jpg"}],
    "subtitles": {"eng-US": [{"ext": "vtt", "url": "https://cdn.test/en.vtt"}]},
    "automatic_captions": {},
}


def learned_youtube_twitter() -> dict:
    learned = learn_download({}, "https://www.youtube.com/watch?v=YtDemoVid04", "YtDemoVid04")
    learned = learn_download(learned, "https://www.youtube.com/watch?v=Yt-DemoVid2", "Yt-DemoVid2")
    return learn_download(learned, "https://twitter.com/DemoVT/status/2000000000000000001", "2000000000000000001")
