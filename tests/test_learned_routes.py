from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

import backend.app.domains.downloads.probe as probe_module
import backend.app.domains.downloads.routes as routes_module
import backend.app.domains.downloads.workers.completion.sidecars as sidecars_module
import backend.app.domains.downloads.workers.execution as worker_module
from backend.app.domains.downloads import store as store_module
from backend.app.domains.downloads.constants import normalize_post_processing
from backend.app.domains.downloads.engine import ENGINE_WINDOW, engine_order
from backend.app.domains.downloads.workers.completion.finalize import FinalizedCompletionOutput
from tests.support import engine_by_name

_URL = "https://example.test/reel/abc123"
_SHAPE = "example.test/reel/{}"
_PAYLOAD = {"category": "example", "display_url": "https://cdn.example.test/v/t51/1234567890abcdef_n.jpg?stp=a"}
_INFO = {"id": "abc", "thumbnail": "https://media.example.test/v/t51/1234567890abcdef_n.jpg?sig=b"}
_PROCESSING = normalize_post_processing(
    {"metadata": "embed", "thumbnail": "embed", "subtitles": "embed", "chapters": "embed"}
)


def _learn(fact: str, *, hits: int = 0, misses: int = 0, window: int = 0) -> None:
    for _ in range(hits):
        store_module.learn_route(_SHAPE, fact, hit=True, window=window)
    for _ in range(misses):
        store_module.learn_route(_SHAPE, fact, hit=False, window=window)


def _facts() -> dict:
    return store_module.load_route_facts(_SHAPE)


def _order(shape: str = _SHAPE) -> list[str]:
    return [engine.name for engine in engine_order(shape)]


def test_route_counts_add_up_keep_their_value_and_fade_at_the_window():
    store_module.learn_route(_SHAPE, "info:thumbnail", hit=False, value="display_url")
    store_module.learn_route(_SHAPE, "info:thumbnail", hit=False)
    _learn("engine:gallerydl", misses=5, window=4)

    assert _facts()["info:thumbnail"]["misses"] == 2
    assert _facts()["info:thumbnail"]["value"] == "display_url"
    # The fifth record finds four and halves them first.
    assert _facts()["engine:gallerydl"]["misses"] == 3


def test_an_unknown_route_keeps_the_run_order():
    _learn("engine:gallerydl", misses=2)

    assert _order() == ["gallerydl", "ytdlp"]


def test_the_engine_that_gets_media_on_a_route_goes_first():
    _learn("engine:gallerydl", misses=3)
    _learn("engine:ytdlp", hits=3)

    assert _order() == ["ytdlp", "gallerydl"]
    # Every other route keeps the run order.
    assert _order("example.test/post/{}") == ["gallerydl", "ytdlp"]


def test_a_promoted_engine_that_starts_failing_gives_its_place_back():
    _learn("engine:gallerydl", misses=3, window=ENGINE_WINDOW)
    _learn("engine:ytdlp", hits=ENGINE_WINDOW, window=ENGINE_WINDOW)
    _learn("engine:ytdlp", misses=10, window=ENGINE_WINDOW)
    assert _order() == ["ytdlp", "gallerydl"]

    _learn("engine:ytdlp", misses=1, window=ENGINE_WINDOW)

    assert _order() == ["gallerydl", "ytdlp"]


def test_a_blocked_run_teaches_nothing_about_the_engine(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(worker_module, "_task_log_tail", lambda task_id: "ERROR: HTTP Error 429: Too Many Requests")
    worker_module._learn_engine_outcome("task", _SHAPE, engine_by_name("gallerydl"), False)
    assert _facts() == {}

    monkeypatch.setattr(worker_module, "_task_log_tail", lambda task_id: "ERROR: Unsupported URL")
    worker_module._learn_engine_outcome("task", _SHAPE, engine_by_name("gallerydl"), False)
    worker_module._learn_engine_outcome("task", _SHAPE, engine_by_name("ytdlp"), True)

    assert _facts()["engine:gallerydl"]["misses"] == 1
    assert _facts()["engine:ytdlp"]["hits"] == 1


def _reads(monkeypatch: pytest.MonkeyPatch, *answers: tuple[dict, str]) -> list[str]:
    calls: list[str] = []

    def probe_media_info(url, **kwargs):
        calls.append(url)
        info, error = answers[min(len(calls), len(answers)) - 1]
        return dict(info), error

    monkeypatch.setattr(probe_module, "probe_media_info", probe_media_info)
    return calls


def _fill(tmp_path: Path, payload: dict = _PAYLOAD, processing: dict = _PROCESSING) -> dict:
    media = tmp_path / "Creator - Clip [abc].mp4"
    finalized = FinalizedCompletionOutput(
        source_url=_URL,
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Clip",
        keep_paths=[media],
    )
    # A fresh cache per call, as every task starts with its own.
    return sidecars_module._with_ytdlp_media_fields(
        dict(payload), finalized, processing, {}, single_item=True
    )


def test_a_read_that_never_adds_anything_stops_after_five_items(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    calls = _reads(monkeypatch, (_INFO, ""))

    results = [_fill(tmp_path) for _ in range(6)]

    assert len(calls) == 5
    # The read showed the payload already had the image, under a name now learned.
    assert _facts()["info:thumbnail"]["value"] == "display_url"
    assert results[-1]["thumbnail"] == _PAYLOAD["display_url"]


def test_one_read_that_adds_captions_keeps_the_read_for_good(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    captions = {"en": [{"ext": "vtt", "url": "https://media.example.test/en.vtt"}]}
    calls = _reads(monkeypatch, ({**_INFO, "subtitles": captions}, ""), (_INFO, ""))

    first = _fill(tmp_path)
    for _ in range(6):
        _fill(tmp_path)

    assert first["subtitles"] == captions
    assert len(calls) == 7


def test_an_old_answer_is_checked_once_more(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    calls = _reads(monkeypatch, (_INFO, ""))
    for _ in range(6):
        _fill(tmp_path)
    assert len(calls) == 5

    monkeypatch.setattr(routes_module, "ANSWER_TRUST", timedelta(seconds=-1))
    _fill(tmp_path)

    assert len(calls) == 6


def test_a_blocked_read_teaches_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _reads(monkeypatch, ({}, "ERROR: HTTP Error 429: Too Many Requests"))

    _fill(tmp_path)

    assert _facts() == {}


def test_no_read_when_the_payload_covers_what_is_asked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    calls = _reads(monkeypatch, (_INFO, ""))

    _fill(
        tmp_path,
        {**_PAYLOAD, "thumbnail": _PAYLOAD["display_url"]},
        normalize_post_processing({"thumbnail": "embed"}),
    )

    assert calls == []
