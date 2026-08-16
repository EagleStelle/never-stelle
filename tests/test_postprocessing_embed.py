from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.domains.downloads.postprocessing as postprocessing_module
from backend.app.domains.downloads.workers.completion_finalization import FinalizedCompletionOutput

ALL_ENABLED = {
    "metadata": True,
    "thumbnail": True,
    "subtitles": True,
    "automatic_subtitles": False,
    "chapters": True,
    "save_as": "embed",
}


def _finalized(media: Path) -> FinalizedCompletionOutput:
    return FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )


@pytest.fixture
def embed_harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Stub ffmpeg so the combined command can be inspected without running it."""
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    commands: list[list[str]] = []
    fallbacks: list[str] = []
    outcome = {"ok": True}

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", lambda *args, **kwargs: [])
    monkeypatch.setattr(postprocessing_module, "publish_scratch_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        postprocessing_module,
        "prepare_thumbnail_post_processing",
        lambda *args, **kwargs: (b"cover", ".jpg"),
    )
    monkeypatch.setattr(
        postprocessing_module,
        "prepare_subtitle_post_processing",
        lambda *args, **kwargs: [
            {"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}
        ],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "prepare_chapter_post_processing",
        lambda *args, **kwargs: [{"start_time": 0, "end_time": 1, "title": "Intro"}],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "finalized_metadata_payload",
        lambda *args, **kwargs: {"title": "Title", "artist": "Creator"},
    )

    # The verification probe must see the tracks the combined command claims to add.
    counts = iter([0] + [99] * 10)
    monkeypatch.setattr(postprocessing_module, "_subtitle_stream_count", lambda *args: next(counts))

    def fake_run(cmd, output_path, **kwargs):
        commands.append(list(cmd))
        Path(output_path).write_bytes(b"out")
        return (outcome["ok"], "" if outcome["ok"] else "stubbed failure")

    monkeypatch.setattr(postprocessing_module, "_run_ffmpeg", fake_run)
    for feature in ("metadata", "subtitle", "chapter", "thumbnail"):
        monkeypatch.setattr(
            postprocessing_module,
            f"apply_{feature}_post_processing",
            lambda *args, _name=feature, **kwargs: fallbacks.append(_name) or set(),
        )

    def run(post_processing: dict) -> None:
        postprocessing_module.apply_finalized_post_processing(
            [media],
            {},
            _finalized(media),
            post_processing=post_processing,
            quality={"mode": "video", "video_container": "mkv"},
            output_root=tmp_path,
            extractor_payload={},
        )

    return {"run": run, "commands": commands, "fallbacks": fallbacks, "outcome": outcome, "media": media}


def test_every_enabled_embed_rides_one_ffmpeg_pass(embed_harness):
    embed_harness["run"](dict(ALL_ENABLED))

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    # Chapters arrive as an ffmetadata input and the muxer is pointed at it.
    assert "ffmetadata" in command
    assert command[command.index("-map_chapters") + 1] != "0"
    # The payload replaces the container's own tags.
    assert command[command.index("-map_metadata") + 1] == "-1"
    assert "title=Title" in command
    # Subtitles and artwork are in the same command, not a later remux.
    assert "-c:s" in command
    assert "-attach" in command
    assert "-c" in command and "copy" in command
    # Nothing had to be redone afterwards.
    assert embed_harness["fallbacks"] == []


def test_disabled_features_contribute_nothing_to_the_pass(embed_harness):
    embed_harness["run"]({**ALL_ENABLED, "subtitles": False, "thumbnail": False})

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    assert "-c:s" not in command
    assert "-attach" not in command
    # Still carries the two that are on.
    assert "ffmetadata" in command
    assert "title=Title" in command
    assert embed_harness["fallbacks"] == []


def test_a_single_enabled_feature_is_left_to_its_own_embedder(embed_harness):
    embed_harness["run"]({**ALL_ENABLED, "subtitles": False, "thumbnail": False, "chapters": False})

    # One feature is one pass either way, and its own embedder reports failures better.
    assert embed_harness["commands"] == []
    assert embed_harness["fallbacks"] == ["metadata"]


def test_a_failed_combined_pass_falls_back_to_every_feature(embed_harness):
    embed_harness["outcome"]["ok"] = False

    embed_harness["run"](dict(ALL_ENABLED))

    assert embed_harness["fallbacks"] == ["metadata", "subtitle", "chapter", "thumbnail"]


def test_sidecar_mode_never_builds_a_combined_pass(embed_harness):
    embed_harness["run"]({**ALL_ENABLED, "save_as": "sidecar"})

    assert embed_harness["commands"] == []
    assert embed_harness["fallbacks"] == ["metadata", "subtitle", "chapter", "thumbnail"]
