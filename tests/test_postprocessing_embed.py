from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path

import pytest

import backend.app.domains.downloads.postprocessing as postprocessing_module
from backend.app.domains.downloads.workers.completion_finalization import FinalizedCompletionOutput

ALL_ENABLED = {
    "metadata": "embed",
    "thumbnail": "embed",
    "subtitles": "embed",
    "automatic_subtitles": "off",
    "chapters": "embed",
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


_SIDECAR_KINDS = ((".chapters.", "chapter"), (".en.vtt", "subtitle"), (".json", "metadata"), (".jpg", "thumbnail"))


@pytest.fixture
def embed_harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Stub ffmpeg and fetching so the embed commands can be inspected without running them."""
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    commands: list[list[str]] = []
    sidecars: list[str] = []
    outcome = {"ok": True}

    monkeypatch.setattr(postprocessing_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(postprocessing_module, "_ffprobe_streams", lambda *args, **kwargs: [])
    monkeypatch.setattr(postprocessing_module, "publish_staged_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(postprocessing_module, "_ytdlp_session", nullcontext)
    monkeypatch.setattr(postprocessing_module, "_fetch_thumbnail", lambda *args, **kwargs: (b"cover", ".jpg"))
    monkeypatch.setattr(
        postprocessing_module,
        "_subtitle_tracks",
        lambda *args, **kwargs: [{"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "_chapters",
        lambda payload: [{"start_time": 0, "end_time": 1, "title": "Intro"}],
    )
    monkeypatch.setattr(
        postprocessing_module,
        "finalized_metadata_payload",
        lambda payload, finalized: {"title": "Title", "artist": "Creator"},
    )
    # The verification probe must see the tracks an embed command claims to add.
    monkeypatch.setattr(
        postprocessing_module,
        "_subtitle_stream_count",
        lambda ffmpeg, path: 99 if Path(path).name.startswith("nvs-embed-") else 0,
    )

    def fake_run(cmd, output_path):
        commands.append(list(cmd))
        Path(output_path).write_bytes(b"out")
        return (outcome["ok"], "" if outcome["ok"] else "stubbed failure")

    def record_sidecar(target: Path, data: bytes) -> Path:
        kind = next(kind for marker, kind in _SIDECAR_KINDS if marker in target.name)
        if not sidecars or sidecars[-1] != kind:
            sidecars.append(kind)
        return target

    monkeypatch.setattr(postprocessing_module, "_run_ffmpeg", fake_run)
    monkeypatch.setattr(postprocessing_module, "_publish_bytes", record_sidecar)

    def run(post_processing: dict) -> None:
        postprocessing_module.apply_finalized_post_processing(
            [media],
            {},
            _finalized(media),
            post_processing=post_processing,
            quality={"mode": "video", "video_container": "mkv"},
            output_root=tmp_path,
        )

    return {"run": run, "commands": commands, "sidecars": sidecars, "outcome": outcome, "media": media}


def test_every_enabled_embed_rides_one_ffmpeg_pass(embed_harness):
    embed_harness["run"](dict(ALL_ENABLED))

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    # Chapters arrive as an ffmetadata input and the muxer is pointed at it.
    assert "ffmetadata" in command
    assert command[command.index("-map_chapters") + 1] != "0"
    # The payload replaces the container's global tags; chapter titles survive.
    assert command[command.index("-map_metadata:g") + 1] == "-1"
    assert "-map_metadata" not in command
    assert "title=Title" in command
    # Subtitles and artwork are in the same command, not a later remux.
    assert "-c:s" in command
    assert "-attach" in command
    assert "-c" in command and "copy" in command
    assert embed_harness["sidecars"] == []


def test_disabled_features_contribute_nothing_to_the_pass(embed_harness):
    embed_harness["run"]({**ALL_ENABLED, "subtitles": "off", "thumbnail": "off"})

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    assert "-c:s" not in command
    assert "-attach" not in command
    # Still carries the two that are on.
    assert "ffmetadata" in command
    assert "title=Title" in command
    assert embed_harness["sidecars"] == []


def test_a_single_enabled_feature_uses_the_same_pass(embed_harness):
    embed_harness["run"]({**ALL_ENABLED, "subtitles": "off", "thumbnail": "off", "chapters": "off"})

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    assert "title=Title" in command
    assert "ffmetadata" not in command
    assert embed_harness["sidecars"] == []


def test_a_failed_pass_retries_each_feature_alone_with_artwork_last(embed_harness):
    embed_harness["outcome"]["ok"] = False

    embed_harness["run"](dict(ALL_ENABLED))

    combined, *retries = embed_harness["commands"]
    assert "-attach" in combined and "-c:s" in combined
    assert ["title=Title" in command for command in retries] == [True, False, False, False]
    assert ["-c:s" in command for command in retries] == [False, True, False, False]
    assert ["ffmetadata" in command for command in retries] == [False, False, True, False]
    assert ["-attach" in command for command in retries] == [False, False, False, True]
    # Tags the muxer rejected are still kept beside the media.
    assert embed_harness["sidecars"] == ["metadata"]


def test_sidecar_mode_never_builds_a_pass(embed_harness):
    embed_harness["run"](dict.fromkeys(ALL_ENABLED, "sidecar"))

    assert embed_harness["commands"] == []
    assert embed_harness["sidecars"] == ["metadata", "subtitle", "chapter", "thumbnail"]


def test_both_embeds_in_one_pass_and_still_writes_every_sidecar(embed_harness):
    embed_harness["run"](dict.fromkeys(ALL_ENABLED, "both"))

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    assert "ffmetadata" in command
    assert "-c:s" in command
    assert "-attach" in command
    assert embed_harness["sidecars"] == ["metadata", "subtitle", "chapter", "thumbnail"]


def test_each_feature_follows_its_own_mode(embed_harness):
    embed_harness["run"](
        {
            "metadata": "embed",
            "subtitles": "embed",
            "automatic_subtitles": "off",
            "chapters": "sidecar",
            "thumbnail": "off",
        }
    )

    assert len(embed_harness["commands"]) == 1
    command = embed_harness["commands"][0]
    assert "title=Title" in command
    assert "-c:s" in command
    # Chapters were routed to a sidecar, artwork was off, so neither joins the pass.
    assert "ffmetadata" not in command
    assert "-attach" not in command
    assert embed_harness["sidecars"] == ["chapter"]


def test_the_retired_boolean_shape_selects_nothing(embed_harness):
    # Stored rows are converted by m0005, so a boolean reaching here is not a request.
    embed_harness["run"](
        {
            "metadata": True,
            "thumbnail": True,
            "subtitles": True,
            "chapters": True,
            "save_as": "embed",
        }
    )

    assert embed_harness["commands"] == []
    assert embed_harness["sidecars"] == []
