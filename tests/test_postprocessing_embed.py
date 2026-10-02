from __future__ import annotations

import json
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.app.domains.downloads.postprocessing.chapters as chapters_module
import backend.app.domains.downloads.postprocessing.containers as containers_module
import backend.app.domains.downloads.postprocessing.embed as embed_module
import backend.app.domains.downloads.postprocessing.ffmpeg as ffmpeg_module
import backend.app.domains.downloads.postprocessing.payloads as payloads_module
import backend.app.domains.downloads.postprocessing.subtitles as subtitles_module
import backend.app.domains.downloads.postprocessing.tags as tags_module
import backend.app.domains.downloads.postprocessing.thumbnails as thumbnails_module
import backend.app.domains.downloads.postprocessing.xmp as xmp_module
import backend.app.runtime.scratch as scratch_module
from tests.support import finalized_for

ALL_ENABLED = {
    "metadata": "embed",
    "thumbnail": "embed",
    "subtitles": "embed",
    "automatic_subtitles": "off",
    "chapters": "embed",
}


_SIDECAR_KINDS = ((".chapters.", "chapter"), (".en.vtt", "subtitle"), (".json", "metadata"), (".jpg", "thumbnail"))


@pytest.fixture
def embed_harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Stub ffmpeg and fetching so the embed commands can be inspected without running them."""
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    commands: list[list[str]] = []
    sidecars: list[str] = []
    outcome = {"ok": True}

    monkeypatch.setattr(embed_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    for module in (embed_module, subtitles_module):
        monkeypatch.setattr(module, "_ffprobe_streams", lambda *args, **kwargs: [])
    for module in (chapters_module, embed_module, payloads_module):
        monkeypatch.setattr(module, "publish_staged_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(embed_module, "_ytdlp_session", nullcontext)
    monkeypatch.setattr(embed_module, "_fetch_thumbnail", lambda *args, **kwargs: (b"cover", ".jpg"))
    monkeypatch.setattr(
        embed_module,
        "_subtitle_tracks",
        lambda *args, **kwargs: [{"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}],
    )
    monkeypatch.setattr(
        embed_module,
        "_chapters",
        lambda payload: [{"start_time": 0, "end_time": 1, "title": "Intro"}],
    )
    monkeypatch.setattr(
        embed_module,
        "finalized_metadata_payload",
        lambda payload, finalized: {"title": "Title", "artist": "Creator"},
    )
    # The verification probe must see the tracks an embed command claims to add.
    for module in (embed_module, subtitles_module):
        monkeypatch.setattr(
            module,
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

    for module in (chapters_module, embed_module, subtitles_module, thumbnails_module):
        monkeypatch.setattr(module, "_run_ffmpeg", fake_run)
    for module in (chapters_module, embed_module, payloads_module, subtitles_module, xmp_module):
        monkeypatch.setattr(module, "_publish_bytes", record_sidecar)

    def run(post_processing: dict) -> None:
        embed_module.apply_finalized_post_processing(
            [media],
            {},
            finalized_for(media),
            post_processing=post_processing,
            quality={"mode": "merged", "video_container": "mkv"},
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


_THREE_CHAPTERS = [
    {"start_time": 0, "end_time": 10, "title": "Intro"},
    {"start_time": 10, "end_time": 25, "title": "Setup: part/1"},
    {"start_time": 25, "end_time": 40, "title": "Outro"},
]


def test_split_chapters_copies_each_chapter_into_the_chapter_folder(embed_harness, monkeypatch):
    monkeypatch.setattr(embed_module, "_chapters", lambda payload: _THREE_CHAPTERS)
    published: list[Path] = []
    for module in (chapters_module, containers_module, embed_module, payloads_module):
        monkeypatch.setattr(
            module, "publish_staged_file", lambda source, target, **kwargs: published.append(target)
        )

    embed_harness["run"]({"split_chapters": True})

    folder = embed_harness["media"].parent / embed_harness["media"].stem
    assert published == [folder / "01 - Intro.mkv", folder / "02 - Setup_ part_1.mkv", folder / "03 - Outro.mkv"]
    first, second, _ = embed_harness["commands"]
    assert first[first.index("-ss") + 1] == "0.000" and first[first.index("-t") + 1] == "10.000"
    assert second[second.index("-ss") + 1] == "10.000" and second[second.index("-t") + 1] == "15.000"
    assert second[second.index("-map_chapters") + 1] == "-1"
    assert "track=2/3" in second and "copy" in second
    assert embed_harness["sidecars"] == []


def test_split_chapters_copies_the_embedded_file(embed_harness, monkeypatch):
    monkeypatch.setattr(embed_module, "_chapters", lambda payload: _THREE_CHAPTERS)

    embed_harness["run"]({"metadata": "embed", "split_chapters": True})

    embed, *chapters = embed_harness["commands"]
    assert "-map_metadata:g" in embed
    assert len(chapters) == 3 and all("-ss" in command for command in chapters)


def test_fewer_than_two_chapters_never_split(embed_harness):
    embed_harness["run"]({"split_chapters": True})

    assert embed_harness["commands"] == []


def _run_real(media: Path, payload: dict, post_processing: dict) -> None:
    embed_module.apply_finalized_post_processing(
        [media],
        payload,
        finalized_for(media),
        post_processing=post_processing,
        quality={"mode": "merged", "video_container": "auto"},
        output_root=media.parent,
    )


def test_images_never_split(tmp_path, monkeypatch):
    image = tmp_path / "post [abc].jpg"
    image.write_bytes(b"image")
    monkeypatch.setattr(embed_module, "_chapters", lambda payload: _THREE_CHAPTERS)
    monkeypatch.setattr(embed_module, "_split_chapter_files", pytest.fail)

    _run_real(image, {}, {"split_chapters": True})


def test_mtime_stamps_the_media_and_its_sidecars(tmp_path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")

    _run_real(media, {"timestamp": 1_700_000_000}, {"metadata": "sidecar", "mtime": True})

    sidecar = Path(f"{media}.json")
    assert sidecar.is_file()
    assert media.stat().st_mtime == pytest.approx(1_700_000_000)
    assert sidecar.stat().st_mtime == pytest.approx(1_700_000_000)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"upload_date": "20240102"}, "2024-01-02T00:00:00+00:00"),
        ({"date": "2024-01-02 03:04:05"}, "2024-01-02T03:04:05+00:00"),
        ({"release_date": "2024-01-02T03:04"}, "2024-01-02T03:04:00+00:00"),
        ({"upload_date": "2024"}, None),
        ({"upload_date": "2024-01"}, None),
        ({"upload_date": "20241350"}, None),
    ],
)
def test_upload_moment_needs_at_least_a_full_day(payload, expected):
    moment = tags_module._upload_moment(payload)
    assert (moment.isoformat() if moment else None) == expected


def test_mtime_without_a_full_date_leaves_the_file_alone(tmp_path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    before = media.stat().st_mtime

    _run_real(media, {"upload_date": "2024"}, {"mtime": True})

    assert media.stat().st_mtime == before


def _embed_request(**features: object) -> dict[str, object]:
    return {"metadata": None, "subtitles": None, "chapters": None, "thumbnail": None, **features}


_EMBED_WARNINGS_ON = dict.fromkeys(("metadata", "subtitles", "chapters", "thumbnail"), False)


_EMBED_WARNINGS_OFF = dict.fromkeys(_EMBED_WARNINGS_ON, True)


_AAC_EMBED_REQUEST = _embed_request(
    metadata={"title": "Title"},
    thumbnail=(b"cover", ".jpg"),
    subtitles=[{"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"}],
)


def test_user_metadata_sidecar_uses_the_final_settings_pipeline_values(tmp_path: Path):
    raw_folder = tmp_path / "@raw.creator"
    raw_folder.mkdir()
    raw = raw_folder / "raw.mp4"
    gallery_sidecar = Path(f"{raw}.json")
    ytdlp_sidecar = raw.with_suffix(".info.json")
    gallery_sidecar.write_text(
        '{"private":"kept","title":"Raw title","upload_date":"20260801"}',
        encoding="utf-8",
    )
    ytdlp_sidecar.write_text('{"comments":[{"text":"kept too"}]}', encoding="utf-8")
    sidecars = payloads_module.metadata_sidecars_for(raw)

    final = tmp_path / "Creator" / "Creator - Title [abc].mp4"
    embed_module.apply_finalized_post_processing(
        [final],
        payloads_module.extractor_payload_from_sidecars(sidecars, {"description": "Description"}),
        finalized_for(final),
        post_processing={"metadata": "sidecar"},
        quality={"mode": "merged", "video_container": "mp4"},
        sidecars=sidecars,
        output_root=tmp_path,
    )

    output = final.with_name(f"{final.name}.json")
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload == {
        "title": "Title",
        "artist": "Creator",
        "album_artist": "Creator",
        "date": "2026-08-01",
        "description": "Description",
        "comment": "https://example.test/post/abc",
        "source": "https://example.test/post/abc",
        "identifier": "abc",
        "publisher": "example",
    }
    assert not gallery_sidecar.exists()
    assert not ytdlp_sidecar.exists()
    assert not raw_folder.exists()


def test_embedded_metadata_uses_the_final_settings_pipeline_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"original")
    raw_sidecar = Path(f"{media}.json")
    raw_sidecar.write_text(
        '{"description":"Full extractor value","timestamp":1785542400}',
        encoding="utf-8",
    )
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[-1]).write_bytes(b"embedded")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(embed_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(ffmpeg_module.subprocess, "run", fake_run)

    embed_module.apply_finalized_post_processing(
        [media],
        payloads_module.extractor_payload_from_sidecars([raw_sidecar], {}),
        finalized_for(media),
        post_processing={"metadata": "embed"},
        quality={"mode": "merged", "video_container": "mkv"},
        sidecars=[raw_sidecar],
    )

    assert media.read_bytes() == b"embedded"
    assert captured["cmd"].count("-i") == 1
    assert "title=Title" in captured["cmd"]
    assert "artist=Creator" in captured["cmd"]
    assert "album_artist=Creator" in captured["cmd"]
    assert "date=2026-08-01" in captured["cmd"]
    assert "description=Full extractor value" in captured["cmd"]
    assert "comment=https://example.test/post/abc" in captured["cmd"]
    assert "source=https://example.test/post/abc" in captured["cmd"]
    assert "identifier=abc" in captured["cmd"]
    assert "publisher=example" in captured["cmd"]
    assert not raw_sidecar.exists()


def test_metadata_embed_discards_source_metadata_and_omits_empty_title(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.webm"
    media.write_bytes(b"media")
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[-1]).write_bytes(b"remuxed")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    for module in (containers_module, embed_module):
        monkeypatch.setattr(module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", fake_run)

    assert embed_module._embed_features(
        "/usr/bin/ffmpeg",
        media,
        _embed_request(metadata={"artist": "Creator"}),
        silent=_EMBED_WARNINGS_ON,
    ) == {"metadata"}
    metadata_index = captured["cmd"].index("-map_metadata:g")
    assert captured["cmd"][metadata_index + 1] == "-1"
    assert not any(value.startswith("title=") for value in captured["cmd"])
    assert "artist=Creator" in captured["cmd"]


def test_thumbnail_sidecar_uses_the_final_media_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"media")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text('{"thumbnail":"https://cdn.example.test/cover.webp"}', encoding="utf-8")

    monkeypatch.setattr(
        embed_module,
        "_fetch_thumbnail",
        lambda ydl, payload, **_: (b"thumbnail-bytes", ".webp"),
    )
    embed_module.apply_finalized_post_processing(
        [media],
        payloads_module.extractor_payload_from_sidecars([extractor_sidecar], {}),
        finalized_for(media),
        post_processing={"thumbnail": "sidecar"},
        quality={"mode": "merged", "video_container": "mp4"},
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert media.with_suffix(".webp").read_bytes() == b"thumbnail-bytes"
    assert not extractor_sidecar.exists()


def test_images_take_metadata_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Creator - Image [abc].webp"
    media.write_bytes(b"original-image")

    def no_fetch(*args, **kwargs):
        raise AssertionError("an image never fetches artwork or captions")

    monkeypatch.setattr(embed_module, "_fetch_thumbnail", no_fetch)
    monkeypatch.setattr(embed_module, "_subtitle_tracks", no_fetch)

    embed_module.apply_finalized_post_processing(
        [media],
        {"chapters": [{"start_time": 0, "end_time": 1, "title": "Intro"}]},
        finalized_for(media, "Image"),
        post_processing=dict.fromkeys(
            ("metadata", "subtitles", "automatic_subtitles", "chapters", "thumbnail"), "sidecar"
        ),
        quality=None,
    )

    assert media.read_bytes() == b"original-image"
    assert sorted(path.name for path in tmp_path.iterdir()) == [media.name, f"{media.name}.json"]
    assert "Extraction skipped" not in caplog.text


def test_manual_and_auto_subtitle_sidecars_are_separate_and_use_final_name(tmp_path: Path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "subtitles": {"en": [{"ext": "vtt", "data": "WEBVTT\n\nmanual"}]},
                "automatic_captions": {
                    "en": [{"ext": "vtt", "data": "WEBVTT\n\nautomatic"}]
                },
            }
        ),
        encoding="utf-8",
    )

    embed_module.apply_finalized_post_processing(
        [media],
        payloads_module.extractor_payload_from_sidecars([extractor_sidecar], {}),
        finalized_for(media),
        post_processing={"subtitles": "sidecar", "automatic_subtitles": "sidecar"},
        quality={"mode": "merged", "video_container": "mp4"},
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert media.with_name(f"{media.stem}.en.vtt").read_text(encoding="utf-8").endswith("manual")
    assert media.with_name(f"{media.stem}.en.auto.vtt").read_text(encoding="utf-8").endswith("automatic")
    assert not extractor_sidecar.exists()


def test_chapter_sidecar_uses_final_name_and_normalizes_boundaries(tmp_path: Path):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "duration": 30,
                "chapters": [
                    {"start_time": 0, "end_time": 5.5, "title": "Intro"},
                    {"start_time": 5.5, "title": "Main"},
                    {"start_time": 20, "title": ""},
                ],
            }
        ),
        encoding="utf-8",
    )

    embed_module.apply_finalized_post_processing(
        [media],
        payloads_module.extractor_payload_from_sidecars([extractor_sidecar], {}),
        finalized_for(media),
        post_processing={"chapters": "sidecar"},
        quality={"mode": "merged", "video_container": "mp4"},
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    ffmeta = media.with_name(f"{media.stem}.chapters.ffmeta")
    assert ffmeta.read_text(encoding="utf-8") == (
        ";FFMETADATA1\n"
        "[CHAPTER]\nTIMEBASE=1/1000\nSTART=0\nEND=5500\ntitle=Intro\n"
        "[CHAPTER]\nTIMEBASE=1/1000\nSTART=5500\nEND=20000\ntitle=Main\n"
        "[CHAPTER]\nTIMEBASE=1/1000\nSTART=20000\nEND=30000\ntitle=Chapter 3\n"
    )
    ogm = media.with_name(f"{media.stem}.chapters.txt")
    assert ogm.read_text(encoding="utf-8") == (
        "CHAPTER01=00:00:00.000\nCHAPTER01NAME=Intro\n"
        "CHAPTER02=00:00:05.500\nCHAPTER02NAME=Main\n"
        "CHAPTER03=00:00:20.000\nCHAPTER03NAME=Chapter 3\n"
    )
    assert not extractor_sidecar.exists()


def test_chapters_embed_from_the_same_normalized_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    captured: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        chapter_input = Path(cmd[cmd.index("ffmetadata") + 2])
        captured["chapters"] = chapter_input.read_text(encoding="utf-8")
        Path(cmd[-1]).write_bytes(b"embedded")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(embed_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(ffmpeg_module.subprocess, "run", fake_run)

    embed_module.apply_finalized_post_processing(
        [media],
        {
            "chapters": [{"start_time": 0.0, "end_time": 12.345, "title": "Intro; #1 = ready"}]
        },
        finalized_for(media),
        post_processing={"chapters": "embed"},
        quality={"mode": "merged", "video_container": "mkv"},
    )

    assert media.read_bytes() == b"embedded"
    cmd = captured["cmd"]
    assert cmd[cmd.index("-map_chapters") + 1] == "1"
    assert cmd.count("-i") == 2
    assert "START=0" in captured["chapters"]
    assert "END=12345" in captured["chapters"]
    assert r"title=Intro\; \#1 \= ready" in captured["chapters"]


def test_every_sidecar_mode_writes_from_one_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mp4"
    media.write_bytes(b"video")
    extractor_sidecar = media.with_suffix(".info.json")
    extractor_sidecar.write_text(
        json.dumps(
            {
                "title": "Extractor title",
                "thumbnail": "https://cdn.example.test/cover.jpg",
                "subtitles": {"en": [{"ext": "vtt", "data": "WEBVTT\n\nmanual"}]},
                "automatic_captions": {
                    "en": [{"ext": "vtt", "data": "WEBVTT\n\nautomatic"}]
                },
                "chapters": [{"start_time": 0, "end_time": 10, "title": "Intro"}],
            }
        ),
        encoding="utf-8",
    )
    finalized = finalized_for(media)
    monkeypatch.setattr(embed_module, "_fetch_thumbnail", lambda ydl, payload, **_: (b"cover", ".jpg"))

    assert embed_module.apply_finalized_post_processing(
        [media],
        payloads_module.extractor_payload_from_sidecars([extractor_sidecar], {}),
        finalized,
        post_processing={
            "metadata": "sidecar",
            "thumbnail": "sidecar",
            "subtitles": "sidecar",
            "automatic_subtitles": "sidecar",
            "chapters": "sidecar",
        },
        quality={"mode": "merged", "video_container": "mp4"},
        sidecars=[extractor_sidecar],
        output_root=tmp_path,
    )

    assert Path(f"{media}.json").is_file()
    assert media.with_suffix(".jpg").read_bytes() == b"cover"
    assert media.with_name(f"{media.stem}.en.vtt").is_file()
    assert media.with_name(f"{media.stem}.en.auto.vtt").is_file()
    assert media.with_name(f"{media.stem}.chapters.ffmeta").is_file()
    assert media.with_name(f"{media.stem}.chapters.txt").is_file()
    assert not extractor_sidecar.exists()


def test_manual_and_auto_subtitles_embed_as_distinct_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mkv"
    media.write_bytes(b"video")
    tracks = [
        {"language": "en", "automatic": False, "extension": "vtt", "data": b"WEBVTT\n"},
        {"language": "en", "automatic": True, "extension": "vtt", "data": b"WEBVTT\n"},
    ]
    captured: list[list[str]] = []
    stream_counts: dict[Path, int] = {media: 0}

    def fake_run(cmd, **kwargs):
        captured.append(cmd)
        output = Path(cmd[-1])
        output.write_bytes(b"embedded")
        stream_counts[output] = 2
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    for module in (containers_module, embed_module):
        monkeypatch.setattr(module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    for module in (embed_module, subtitles_module):
        monkeypatch.setattr(
            module,
            "_subtitle_stream_count",
            lambda ffmpeg, path: stream_counts.get(path, 0),
        )
    monkeypatch.setattr(ffmpeg_module.subprocess, "run", fake_run)

    assert embed_module._embed_features(
        "/usr/bin/ffmpeg",
        media, _embed_request(subtitles=tracks), silent=_EMBED_WARNINGS_ON
    ) == {"subtitles"}

    assert media.read_bytes() == b"embedded"
    assert len(captured) == 1
    embed_cmd = captured[0]
    assert embed_cmd.count("-i") == 3
    assert embed_cmd[embed_cmd.index("-c:s") + 1] == "copy"
    assert "title=en" in embed_cmd
    assert "title=en (auto-generated)" in embed_cmd


def test_many_subtitles_embed_through_bounded_bundle_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].webm"
    media.write_bytes(b"video")
    tracks = [
        {
            "language": f"translated-{index:03d}",
            "automatic": True,
            "extension": "vtt",
            "data": f"WEBVTT\n\ncaption {index}".encode(),
        }
        for index in range(100)
    ]
    calls: list[list[str]] = []
    stream_counts: dict[Path, int] = {media: 0}

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        output = Path(cmd[-1])
        output.write_bytes(b"muxed")
        if "nvs-subtitle-bundle-" in output.name:
            prior = next(
                (
                    stream_counts[Path(cmd[index + 1])]
                    for index, value in enumerate(cmd)
                    if value == "-i" and Path(cmd[index + 1]) in stream_counts
                ),
                0,
            )
            new_tracks = sum(1 for value in cmd if "nvs-subtitle-input-" in value)
            stream_counts[output] = prior + new_tracks
        else:
            bundle = next(
                Path(cmd[index + 1])
                for index, value in enumerate(cmd)
                if value == "-i" and "nvs-subtitle-bundle-" in cmd[index + 1]
            )
            stream_counts[output] = stream_counts[bundle]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    for module in (containers_module, embed_module):
        monkeypatch.setattr(module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    for module in (embed_module, subtitles_module):
        monkeypatch.setattr(
            module,
            "_subtitle_stream_count",
            lambda ffmpeg, path: stream_counts.get(path, 0),
        )
    monkeypatch.setattr(ffmpeg_module.subprocess, "run", fake_run)

    assert embed_module._embed_features(
        "/usr/bin/ffmpeg",
        media, _embed_request(subtitles=tracks), silent=_EMBED_WARNINGS_ON
    ) == {"subtitles"}

    bundle_calls = [cmd for cmd in calls if "nvs-subtitle-bundle-" in cmd[-1]]
    assert len(bundle_calls) == 5
    assert all(cmd.count("-i") <= subtitles_module._SUBTITLE_BUNDLE_BATCH_SIZE + 1 for cmd in bundle_calls)
    assert len(calls[-1]) < 40
    assert media.read_bytes() == b"muxed"


def test_thumbnail_embed_uses_container_aware_tags_after_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "Creator - Title [abc].mp3"
    media.write_bytes(b"original")
    captured: dict[str, Path] = {}

    def fake_embed(path: Path, thumbnail: Path) -> bool:
        captured["path"] = path
        captured["thumbnail"] = thumbnail
        assert thumbnail.read_bytes() == b"thumbnail-bytes"
        path.write_bytes(b"embedded")
        return True

    monkeypatch.setattr(
        embed_module, "_fetch_thumbnail", lambda ydl, payload, **_: (b"thumbnail-bytes", ".jpg")
    )
    monkeypatch.setattr(embed_module, "_embed_thumbnail_with_mutagen", fake_embed)

    embed_module.apply_finalized_post_processing(
        [media],
        {},
        finalized_for(media),
        post_processing={"thumbnail": "embed"},
        quality={"mode": "audio", "audio_format": "mp3"},
    )

    assert media.read_bytes() == b"embedded"
    assert captured["path"] == media
    assert captured["thumbnail"].suffix == ".jpg"
    assert not media.with_suffix(".jpg").exists()


def test_webp_thumbnail_is_converted_before_mp4_embedding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media = tmp_path / "Title [abc].mp4"
    media.write_bytes(b"video")
    converted = scratch_module.scratch_temp_path(prefix="nvs-converted-cover-", suffix=".png")
    converted.write_bytes(b"png")
    captured: dict[str, Path] = {}

    for module in (containers_module, embed_module):
        monkeypatch.setattr(module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        thumbnails_module,
        "_convert_thumbnail_for_embedding",
        lambda ffmpeg, thumbnail: converted,
    )
    monkeypatch.setattr(
        embed_module,
        "_embed_thumbnail_with_mutagen",
        lambda path, thumbnail: captured.update(path=path, thumbnail=thumbnail) is None,
    )

    assert embed_module._embed_features(
        "/usr/bin/ffmpeg",
        media, _embed_request(thumbnail=(b"webp", ".webp")), silent=_EMBED_WARNINGS_ON
    ) == {"thumbnail"}
    assert captured == {"path": media, "thumbnail": converted}
    assert not converted.exists()


def test_unsupported_metadata_embed_does_not_fail_the_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].mkv"
    media.write_bytes(b"original")

    monkeypatch.setattr(embed_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(
        ffmpeg_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="unsupported metadata", stdout=""),
    )

    embed_module.apply_finalized_post_processing(
        [media],
        {},
        finalized_for(media),
        post_processing={"metadata": "embed"},
        quality={"mode": "merged", "video_container": "mkv"},
    )

    assert media.read_bytes() == b"original"
    assert "unsupported metadata" in caplog.text
    # The rejected tags are kept beside the media instead.
    assert json.loads(Path(f"{media}.json").read_text(encoding="utf-8"))["title"] == "Title"


def test_auto_output_silently_skips_unsupported_embed_targets(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].aac"
    media.write_bytes(b"audio")

    silent = _EMBED_WARNINGS_OFF
    assert embed_module._embed_features("/usr/bin/ffmpeg", media, _AAC_EMBED_REQUEST, silent=silent) == set()

    assert not caplog.text


def test_explicit_unsupported_embed_targets_remain_diagnostic(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    media = tmp_path / "Title [abc].aac"
    media.write_bytes(b"audio")

    silent = _EMBED_WARNINGS_ON
    assert embed_module._embed_features("/usr/bin/ffmpeg", media, _AAC_EMBED_REQUEST, silent=silent) == set()

    assert "Metadata embed skipped" in caplog.text
    assert "Thumbnail embed skipped" in caplog.text
    assert "Subtitle embed skipped" in caplog.text


def test_image_metadata_embed_writes_lossless_xmp_without_sidecar(tmp_path: Path):
    media = tmp_path / "Creator - Photo [abc].jpg"
    original_scan = b"compressed-image-data"
    media.write_bytes(b"\xff\xd8\xff\xe0\x00\x04JF\xff\xda" + original_scan)

    embed_module.apply_finalized_post_processing(
        [media],
        {"description": "Gallery description", "upload_date": "20260801"},
        finalized_for(media, "Photo"),
        post_processing={"metadata": "embed"},
        quality=None,
    )

    embedded = media.read_bytes()
    assert embedded.endswith(original_scan)
    assert b"http://ns.adobe.com/xap/1.0/" in embedded
    assert b"Photo" in embedded
    assert b"Creator" in embedded
    assert b"Gallery description" in embedded
    assert b"https://example.test/post/abc" in embedded
    assert b"<dc:identifier>abc</dc:identifier>" in embedded
    assert b"<rdf:li>example</rdf:li>" in embedded
    assert b"2026-08-01" in embedded
    assert b"xmlns:nvs" not in embedded
    assert not Path(f"{media}.json").exists()


def test_image_metadata_embed_falls_back_to_sidecar_when_lossless_writer_is_unavailable(
    tmp_path: Path,
):
    media = tmp_path / "Creator - Photo [abc].webp"
    media.write_bytes(b"RIFFnot-a-real-webp")

    embed_module.apply_finalized_post_processing(
        [media],
        {},
        finalized_for(media, "Photo"),
        post_processing={"metadata": "embed"},
        quality=None,
    )

    sidecar = Path(f"{media}.json")
    assert json.loads(sidecar.read_text(encoding="utf-8"))["title"] == "Photo"


def test_metadata_title_follows_naming_but_keeps_special_and_illegal_characters(tmp_path: Path):
    media = tmp_path / "Creator - Photo [abc].webp"
    media.write_bytes(b"RIFFnot-a-real-webp")
    finalized = replace(
        finalized_for(media, ""),
        naming={"charset": "remove", "invalid_chars": "dash", "case": "lowercase", "strip_hashtags": True},
    )

    track = tags_module.finalized_metadata_payload({"track": "Café: Song #live"}, finalized)
    embed_module.apply_finalized_post_processing(
        [media],
        {"title": "Café: A/B Photo? #tag"},
        finalized,
        post_processing={"metadata": "sidecar"},
        quality=None,
    )

    assert track["title"] == "café: song"
    assert json.loads(Path(f"{media}.json").read_text(encoding="utf-8"))["title"] == "café: a/b photo?"
