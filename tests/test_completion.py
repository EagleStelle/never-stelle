from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.app.domains.downloads.engines.probe as probe_module
import backend.app.domains.downloads.library.history as history_module
import backend.app.domains.downloads.metadata.creators as creators_module
import backend.app.domains.downloads.metadata.folders as folders_module
import backend.app.domains.downloads.metadata.pipeline as pipeline_module
import backend.app.domains.downloads.postprocessing.tags as tags_module
import backend.app.domains.downloads.workers.completion.finalize as finalize_module
import backend.app.domains.downloads.workers.completion.outputs as outputs_module
import backend.app.domains.downloads.workers.completion.sidecars as sidecars_module
from backend.app.core.paths import path_key
from backend.app.domains.options.post_processing import normalize_post_processing
from tests.support import YTDLP_VIDEO_INFO, engine_by_name, finalized_for, use_temp_db

_GALLERYDL_VIDEO_PAYLOAD = {"category": "tiktok", "desc": "", "video": {"cover": "https://cdn.test/cover"}}


def _probe_media_info_calls(monkeypatch: pytest.MonkeyPatch, info: dict) -> list[str]:
    calls: list[str] = []
    monkeypatch.setattr(
        probe_module,
        "probe_media_info",
        lambda url, **kwargs: calls.append(url) or (dict(info), ""),
    )
    return calls


_FIELDS_URL = "https://example.test/watch/abc123"


_FIELDS_TEMPLATES = {
    "folder_template": "{{username}}",
    "filename_template": "{{username}} - {{title}} [{{id}}]",
}


def _save_example_fields(**roles: list[str]) -> None:
    from backend.app.domains.settings import load_saved_settings_file, save_saved_settings_file

    payload = load_saved_settings_file()
    payload["source_fields"] = {"example": roles}
    save_saved_settings_file(payload)


def _finalized_creator(tmp_path: Path, raw: Path, metadata: dict[str, str]) -> str:
    return finalize_module._finalize_completed_output(
        source_url=_FIELDS_URL,
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata=metadata,
        media_id="abc123",
        template_settings=_FIELDS_TEMPLATES,
        cache_dropper=None,
    ).creator


def test_gallerydl_video_payload_gains_ytdlp_artwork_and_captions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    calls = _probe_media_info_calls(monkeypatch, YTDLP_VIDEO_INFO)
    finalized = finalized_for(tmp_path / "Creator - Title [abc].mp4")
    probed: dict = {}

    payload = sidecars_module._with_ytdlp_media_fields(
        dict(_GALLERYDL_VIDEO_PAYLOAD),
        finalized,
        normalize_post_processing({"metadata": "embed", "thumbnail": "embed", "subtitles": "embed"}),
        probed,
        single_item=False,
    )
    sidecars_module._with_ytdlp_media_fields(
        dict(_GALLERYDL_VIDEO_PAYLOAD),
        finalized,
        normalize_post_processing({"thumbnail": "embed"}),
        probed,
        single_item=False,
    )

    assert calls == ["https://example.test/post/abc"]
    assert payload["thumbnails"] == YTDLP_VIDEO_INFO["thumbnails"]
    assert payload["subtitles"] == YTDLP_VIDEO_INFO["subtitles"]
    # Tags stay with the downloader's payload, so a sound name never becomes the title.
    assert "track" not in payload
    assert payload["video"] == _GALLERYDL_VIDEO_PAYLOAD["video"]


@pytest.mark.parametrize(
    ("name", "payload", "processing", "single_item", "info_id"),
    [
        ("Photo [abc].jpg", _GALLERYDL_VIDEO_PAYLOAD, {"thumbnail": "embed"}, True, "abc"),
        ("Clip [abc].mp4", _GALLERYDL_VIDEO_PAYLOAD, {"metadata": "embed"}, True, "abc"),
        ("Clip [abc].mp4", YTDLP_VIDEO_INFO, {"thumbnail": "embed"}, True, "abc"),
        ("Clip [abc].mp4", _GALLERYDL_VIDEO_PAYLOAD, {"thumbnail": "embed"}, False, "other"),
    ],
)
def test_ytdlp_media_fields_are_left_out_when_they_cannot_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name, payload, processing, single_item, info_id
):
    _probe_media_info_calls(monkeypatch, {**YTDLP_VIDEO_INFO, "id": info_id})

    result = sidecars_module._with_ytdlp_media_fields(
        dict(payload),
        finalized_for(tmp_path / name),
        normalize_post_processing(processing),
        {},
        single_item=single_item,
    )

    assert result == payload


def test_username_folder_and_nickname_filename_stay_distinct_for_handle_metadata(tmp_path: Path):
    media_id = "7100000000000000001"
    source_url = f"https://www.tiktok.com/@fakeacc.com/video/{media_id}"
    template_settings = {
        "folder_template": "{{username}}",
        "filename_template": "{{nickname}} - {{title}} [{{id}}]",
    }
    raw_path = tmp_path / "fakeacc.com" / f"fakeacc.com - Clip [{media_id}].mp4"
    raw_path.parent.mkdir()
    raw_path.write_bytes(b"video")
    metadata = {
        "webpage_url": source_url,
        "channel": "Clip Demo",
        "uploader": "fakeacc.com",
        "uploader_url": "https://www.tiktok.com/@fakeacc.com",
    }

    creator = creators_module._filename_creator(
        raw_path,
        template_settings["filename_template"],
        metadata,
        source_url,
        media_id,
    )
    nickname = creators_module._filename_nickname(
        raw_path,
        template_settings["filename_template"],
        template_settings["folder_template"],
        creators_module._template_folder_text(tmp_path, raw_path),
        metadata,
        creator,
    )
    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        raw_path,
        template_settings,
        "tiktok",
        creator_hint=creator,
        media_id_hint=media_id,
        nickname_hint=nickname,
        title_hint="Clip",
    )
    final_path = folders_module.move_group_to_template_folder(
        final_path,
        tmp_path,
        template_settings,
        creator,
        media_id,
        nickname,
    )

    expected = tmp_path / "fakeacc.com" / f"Clip Demo - Clip [{media_id}].mp4"
    assert creator == "fakeacc.com"
    assert nickname == "Clip Demo"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not raw_path.exists()


def test_clean_resolved_filename_renames_real_file_using_settings_template(tmp_path: Path):
    source_url = "https://twitter.com/DemoVT/status/2000000000000000001"
    media_file = tmp_path / "DemoVT - 2000000000000000001 - Video by DemoVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{username}} - {{id}} - {{title}}"},
        "twitter",
    )

    expected = tmp_path / "DemoVT - 2000000000000000001.mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_rerenders_selected_quality(tmp_path: Path):
    source_url = "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/"
    media_file = tmp_path / "source - Video by Artist [4483553].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{quality}} - {{title}} [{{id}}]"},
        "rule34video",
        creator_hint="Artist",
        media_id_hint="4483553",
        quality={"mode": "merged", "video_quality": "1080p"},
    )

    expected = tmp_path / "1080p - [4483553].mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_finalization_runs_scraper_fields_templates_and_naming_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    raw = tmp_path / "Extractor title [abc123].mp4"
    raw.write_bytes(b"video")
    monkeypatch.setattr(
        pipeline_module,
        "get_effective_title_cleaning",
        lambda source_url: {"strip_hashtags": True},
    )

    finalized = finalize_module._finalize_completed_output(
        source_url="https://example.test/watch/abc123",
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"id": "abc123", "title": "Extractor title"},
        template_settings={
            "folder_template": "{{title}}",
            "filename_template": "{{title}} [{{id}}]",
        },
        extra_tokens={"title": "Scraped title #ignored"},
        cache_dropper=None,
    )

    expected = tmp_path / "Scraped title" / "Scraped title [abc123].mp4"
    assert finalized.final_path == expected
    assert finalized.display_filename == expected.name
    assert finalized.title == "Scraped title"
    assert expected.is_file()
    assert not raw.exists()


def test_finalized_title_keeps_the_characters_only_the_filename_replaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    raw = tmp_path / "Extractor title [abc123].mp4"
    raw.write_bytes(b"video")
    monkeypatch.setattr(
        pipeline_module,
        "get_effective_title_cleaning",
        lambda source_url: {"charset": "remove", "case": "capitalized"},
    )

    finalized = finalize_module._finalize_completed_output(
        source_url="https://example.test/watch/abc123",
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"id": "abc123", "title": "café: what?"},
        template_settings={"folder_template": "", "filename_template": "{{title}} [{{id}}]"},
        cache_dropper=None,
    )

    assert finalized.display_filename == "Cafe_ What_ [abc123].mp4"
    assert finalized.title == "Café: What?"


def test_scraped_title_and_creator_reach_metadata_unreplaced(tmp_path: Path):
    raw = tmp_path / "Extractor title [abc123].mp4"
    raw.write_bytes(b"video")

    finalized = finalize_module._finalize_completed_output(
        source_url="https://example.test/watch/abc123",
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"id": "abc123", "title": "Extractor title"},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        extra_tokens={"title": "Live: A/B?", "username": "AC/DC"},
        cache_dropper=None,
    )
    tags = tags_module.finalized_metadata_payload({}, finalized)

    assert finalized.final_path == tmp_path / "AC_DC" / "Live_ A_B_ [abc123].mp4"
    assert (tags["title"], tags["artist"]) == ("Live: A/B?", "AC/DC")


def test_coerce_audio_output_extension_prefers_postprocessed_target(tmp_path: Path):
    raw = tmp_path / "clip.webm"
    final = tmp_path / "clip.opus"
    final.write_bytes(b"opus")
    group_paths = [raw]

    result = outputs_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "opus"},
    )

    assert result == final
    assert group_paths == [final]


def test_coerce_audio_output_extension_renames_ytdlp_aac_m4a(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def fail(*args):
        raise AssertionError("already the target container")

    monkeypatch.setattr(outputs_module, "convert_audio_output", fail)
    # yt-dlp writes ADTS AAC under a `.m4a` name.
    raw = tmp_path / "clip.m4a"
    raw.write_bytes(b"\xff\xf1" + bytes(16))
    group_paths = [raw]

    result = outputs_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "aac"},
    )

    expected = tmp_path / "clip.aac"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_coerce_audio_output_extension_keeps_m4a_when_remux_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(outputs_module, "convert_audio_output", lambda *args: False)
    raw = tmp_path / "clip.m4a"
    raw.write_bytes(bytes(4) + b"ftypM4A " + bytes(8))
    group_paths = [raw]

    result = outputs_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "aac"},
    )

    assert result == raw
    assert group_paths == [raw]
    assert raw.is_file()


def test_coerce_audio_output_extension_renames_relabeled_gallerydl_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def fail(*args):
        raise AssertionError("already the target container")

    monkeypatch.setattr(outputs_module, "convert_audio_output", fail)
    # gallery-dl finalizes yt-dlp's converted Ogg Opus under the source extension.
    raw = tmp_path / "clip.webm"
    raw.write_bytes(b"OggS" + bytes(16))
    group_paths = [raw]

    result = outputs_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "opus"},
    )

    expected = tmp_path / "clip.opus"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_coerce_audio_output_extension_converts_unconverted_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def convert(source: Path, target: Path, quality: dict[str, str]) -> bool:
        assert quality["audio_format"] == "wav"
        target.write_bytes(b"RIFF____WAVE")
        return True

    monkeypatch.setattr(outputs_module, "convert_audio_output", convert)
    raw = tmp_path / "clip.webm"
    raw.write_bytes(b"\x1a\x45\xdf\xa3" + bytes(16))
    group_paths = [raw]

    result = outputs_module._coerce_audio_output_extension(
        raw,
        group_paths,
        {"mode": "audio", "audio_format": "wav"},
    )

    expected = tmp_path / "clip.wav"
    assert result == expected
    assert group_paths == [expected]
    assert expected.is_file()
    assert not raw.exists()


def test_clean_resolved_filename_rebuilds_sparse_gallerydl_name_from_title_hint(tmp_path: Path):
    source_url = "https://example.com/alice/post/abc123"
    media_file = tmp_path / "[abc123]_1.jpg"
    media_file.write_bytes(b"image")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "example",
        creator_hint="alice",
        media_id_hint="abc123",
        title_hint="Nice clip",
    )

    # The post's only file carries no number.
    expected = tmp_path / "alice - Nice clip [abc123].jpg"
    assert final_path == expected
    assert display_filename == "alice - Nice clip [abc123].jpg"
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_keeps_the_number_of_a_file_beside_its_post_siblings(tmp_path: Path):
    media_file = tmp_path / "[abc123]_1.jpg"
    media_file.write_bytes(b"image")
    (tmp_path / "[abc123]_2.jpg").write_bytes(b"image")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        "https://example.com/alice/post/abc123",
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "example",
        group_paths=[media_file],
        creator_hint="alice",
        media_id_hint="abc123",
        title_hint="Nice clip",
    )

    assert final_path == tmp_path / "alice - Nice clip [abc123]_1.jpg"
    assert display_filename == "alice - Nice clip [abc123].jpg"


def test_clean_resolved_filename_title_only_template_falls_back_to_media_id(tmp_path: Path):
    source_url = "https://twitter.com/DemoVT/status/2000000000000000001"
    media_file = tmp_path / "Video by DemoVT.mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "", "filename_template": "{{title}}"},
        "twitter",
    )

    expected = tmp_path / "2000000000000000001.mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_strips_at_from_username(tmp_path: Path):
    source_url = "https://video.example/watch?v=YtDemoVid05"
    media_file = tmp_path / "@mock - Glass Garden [YtDemoVid05].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "",
        creator_hint="mock",
        media_id_hint="YtDemoVid05",
        nickname_hint="Mock",
        title_hint="Glass Garden",
    )

    expected = tmp_path / "mock - Glass Garden [YtDemoVid05].mp4"
    assert final_path == expected
    assert display_filename == expected.name
    assert expected.is_file()
    assert not media_file.exists()


def test_clean_resolved_filename_keeps_authoritative_creator_over_url_handle(tmp_path: Path):
    # An authoritative (configured) creator must not be clobbered by a URL-derived handle.
    source_url = "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001"
    media_file = tmp_path / "UC1234567890 - Clip [7100000000000000001].mp4"
    media_file.write_bytes(b"video")

    final_path, display_filename = outputs_module._clean_resolved_filename(
        source_url,
        media_file,
        {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"},
        "tiktok",
        creator_hint="UC1234567890",
        media_id_hint="7100000000000000001",
        title_hint="Clip",
        creator_authoritative=True,
    )

    expected = tmp_path / "UC1234567890 - Clip [7100000000000000001].mp4"
    assert final_path == expected
    assert display_filename == expected.name


def test_complete_sidecar_metadata_skips_completion_enrichment(tmp_path: Path):
    path = tmp_path / "ChannelHandle - Nice clip [abc123].mp4"
    path.write_bytes(b"video")

    needed = sidecars_module._metadata_enrichment_needed(
        [path],
        engine_by_name("gallerydl"),
        {
            path_key(path): {
                "id": "abc123",
                "channel": "ChannelHandle",
                "title": "Nice clip",
            }
        },
        {"filename_template": "{{username}} - {{title}} [{{id}}]"},
        "https://example.test/watch/abc123",
    )

    assert needed is False


def test_probe_fills_the_top_field_the_engine_left_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader", "username", "uploader_id"])
    raw = tmp_path / "unknown - Nice clip [abc123].mp4"
    raw.write_bytes(b"video")
    # The engine carried an id but none of the names above it.
    metadata_by_path = {path_key(raw): {"filepath": str(raw), "id": "abc123", "username": "", "user_id": "1001"}}
    monkeypatch.setattr(
        sidecars_module,
        "probe_link_metadata",
        lambda url, key: {"uploader": "Alice Example", "uploader_id": "1001", "title": "Probed"},
    )

    sidecars_module._probe_output_metadata_inline(
        [raw], engine_by_name("gallerydl"), metadata_by_path, _FIELDS_URL, "example", _FIELDS_TEMPLATES
    )

    assert metadata_by_path[path_key(raw)]["user_id"] == "1001"
    assert _finalized_creator(tmp_path, raw, metadata_by_path[path_key(raw)]) == "Alice Example"


def test_the_next_field_names_the_download_when_the_top_one_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader", "username", "uploader_id"])
    raw = tmp_path / "raw [abc123].mp4"
    raw.write_bytes(b"video")

    creator = _finalized_creator(
        tmp_path, raw, {"filepath": str(raw), "username": "alice_handle", "uploader_id": "1001", "title": "Clip"}
    )

    assert creator == "alice_handle"


def test_a_field_outside_the_fields_order_does_not_skip_the_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader_id"])
    path = tmp_path / "ChannelHandle - Nice clip [abc123].mp4"
    path.write_bytes(b"video")

    needed = sidecars_module._metadata_enrichment_needed(
        [path],
        engine_by_name("gallerydl"),
        {path_key(path): {"id": "abc123", "channel": "ChannelHandle", "title": "Nice clip"}},
        _FIELDS_TEMPLATES,
        _FIELDS_URL,
    )

    assert needed is True


def test_one_probe_names_every_output_of_a_multi_file_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader", "username"])
    paths = [tmp_path / f"unknown - [abc123]_{index}.jpg" for index in (1, 2)]
    metadata_by_path = {
        path_key(path): {"filepath": str(path), "id": "abc123", "title": f"Photo {index}"}
        for index, path in enumerate(paths, start=1)
    }
    probes: list[str] = []

    def probe(url: str, key: str) -> dict[str, str]:
        probes.append(url)
        return {"uploader": "Alice Example", "title": "Post"}

    monkeypatch.setattr(sidecars_module, "probe_link_metadata", probe)

    sidecars_module._probe_output_metadata_inline(
        paths, engine_by_name("gallerydl"), metadata_by_path, _FIELDS_URL, "example", _FIELDS_TEMPLATES
    )

    assert probes == [_FIELDS_URL]
    # The creator fits every output; each keeps its own title.
    assert [metadata_by_path[path_key(path)]["uploader"] for path in paths] == ["Alice Example"] * 2
    assert [metadata_by_path[path_key(path)]["title"] for path in paths] == ["Photo 1", "Photo 2"]


_ECHO_TITLE = "1.2K views · 30 reactions | Morning walk | Alice Example"
# What an extractor that finds no owner copies raw from the page title.
_ECHO_CREATOR = "1.2K views &#xb7; 30 reactions | Morning walk | Alice Example"


def test_a_creator_that_only_echoes_the_title_falls_to_the_next_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader", "username", "uploader_id"])
    raw = tmp_path / "raw [abc123].mp4"
    raw.write_bytes(b"video")

    creator = _finalized_creator(
        tmp_path,
        raw,
        {"filepath": str(raw), "uploader": _ECHO_CREATOR, "uploader_id": "1001", "title": _ECHO_TITLE},
    )

    assert creator == "1001"


def test_an_engine_creator_line_that_echoes_the_title_is_not_the_creator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader"], nickname=["uploader"])
    raw = tmp_path / "[abc123].mp4"
    raw.write_bytes(b"video")

    finalized = finalize_module._finalize_completed_output(
        source_url=_FIELDS_URL,
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"filepath": str(raw), "uploader": _ECHO_CREATOR, "title": _ECHO_TITLE},
        media_id="abc123",
        template_settings=_FIELDS_TEMPLATES,
        creator_fallback=lambda url: _ECHO_CREATOR,
        cache_dropper=None,
    )

    assert finalized.creator == ""


def test_a_title_echo_in_the_creator_is_looked_up_again(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader", "username", "uploader_id"])
    raw = tmp_path / "raw [abc123].mp4"
    raw.write_bytes(b"video")
    metadata_by_path = {
        path_key(raw): {"filepath": str(raw), "uploader": _ECHO_CREATOR, "uploader_id": "1001", "title": _ECHO_TITLE}
    }
    monkeypatch.setattr(
        sidecars_module,
        "probe_link_metadata",
        lambda url, key: {"uploader": "Alice Example", "uploader_id": "1001", "title": _ECHO_TITLE},
    )

    unanswered = sidecars_module._probe_output_metadata_inline(
        [raw], engine_by_name("ytdlp"), metadata_by_path, _FIELDS_URL, "example", _FIELDS_TEMPLATES
    )

    assert unanswered is False
    assert _finalized_creator(tmp_path, raw, metadata_by_path[path_key(raw)]) == "Alice Example"


def test_a_download_named_by_its_display_name_is_filed_under_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    # gallery-dl's yt-dlp handoff leaves its own creator fields null.
    raw = tmp_path / "None" / "None - Clip [abc123]_None.mp4"
    raw.parent.mkdir()
    raw.write_bytes(b"video")

    finalized = finalize_module._finalize_completed_output(
        source_url=_FIELDS_URL,
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"filepath": str(raw), "uploader": "AliceExample", "uploader_id": "1001", "title": "Clip"},
        media_id="abc123",
        template_settings=_FIELDS_TEMPLATES,
        cache_dropper=None,
    )

    assert finalized.final_path == tmp_path / "AliceExample" / "AliceExample - Clip [abc123].mp4"


def test_gallerydl_distinct_metadata_urls_split_rows_dynamically(tmp_path: Path):
    first = tmp_path / "Poster - Image [asset-a]_1.jpg"
    second = tmp_path / "Poster - Image [asset-b]_2.jpg"
    for path in (first, second):
        path.write_bytes(b"image")
    metadata = {
        path_key(first): {"webpage_url": "https://www.example.test/item/asset-a"},
        path_key(second): {"webpage_url": "https://www.example.test/item/asset-b"},
    }

    groups = outputs_module._download_groups(
        [first, second],
        engine_by_name("gallerydl"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://www.example.test/",
    )

    assert len(groups) == 2
    assert {group["media_id"] for group in groups} == {"asset-a", "asset-b"}


def test_gallerydl_source_url_id_groups_distinct_child_metadata_urls(tmp_path: Path):
    first = tmp_path / "Poster - Image [child-a]_1.jpg"
    second = tmp_path / "Poster - Image [child-b]_2.mp4"
    for path in (first, second):
        path.write_bytes(b"media")
    metadata = {
        path_key(first): {"webpage_url": "https://www.example.test/item/child-a"},
        path_key(second): {"webpage_url": "https://www.example.test/item/child-b"},
    }

    groups = outputs_module._download_groups(
        [first, second],
        engine_by_name("gallerydl"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://www.example.test/post/root123",
    )

    assert len(groups) == 1
    assert groups[0]["media_id"] == "root123"


def test_duplicate_library_cleanup_removes_history_row_for_duplicate_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    keep = tmp_path / "Creator - Clip [abc123].mp4"
    duplicate = tmp_path / "Old Creator - Clip [abc123].mp4"
    keep.write_bytes(b"current")
    duplicate.write_bytes(b"stale")
    removed: list[str] = []
    queried: list[str] = []

    def fake_load_history_entry_for_path(path: str):
        queried.append(path)
        return ("disk:old-abc123", {"resolved_full_path": path}) if path == str(duplicate) else (None, None)

    monkeypatch.setattr(
        outputs_module,
        "load_history_entry_for_path",
        fake_load_history_entry_for_path,
    )
    monkeypatch.setattr(outputs_module, "load_history_entries_for_media_id", lambda media_id: [])
    monkeypatch.setattr(outputs_module, "remove_history_records", removed.extend)

    outputs_module._cleanup_duplicate_library_media(tmp_path, "abc123", [keep])

    assert queried == [str(duplicate)]
    assert removed == ["disk:old-abc123"]
    assert not duplicate.exists()
    assert keep.exists()


def test_duplicate_library_cleanup_uses_history_index_for_different_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    keep = tmp_path / "new" / "Creator - Clip [abc123].mp4"
    duplicate = tmp_path / "old" / "Old Creator - Clip [abc123].mp4"
    keep.parent.mkdir()
    duplicate.parent.mkdir()
    keep.write_bytes(b"current")
    duplicate.write_bytes(b"stale")
    removed: list[str] = []

    monkeypatch.setattr(
        outputs_module,
        "load_history_entries_for_media_id",
        lambda media_id: [
            (
                "disk:old-abc123",
                {
                    "media_id": media_id,
                    "resolved_full_path": str(duplicate),
                },
            )
        ],
    )
    monkeypatch.setattr(
        outputs_module,
        "load_history_entry_for_path",
        lambda path: (_ for _ in ()).throw(AssertionError("sibling fallback should not be used")),
    )
    monkeypatch.setattr(outputs_module, "remove_history_records", removed.extend)

    outputs_module._cleanup_duplicate_library_media(tmp_path, "abc123", [keep])

    assert removed == ["disk:old-abc123"]
    assert not duplicate.exists()
    assert keep.exists()


def test_read_metadata_sidecar_accepts_gallerydl_jsonl(tmp_path: Path):
    media_file = tmp_path / "clip.mp4"
    sidecar = tmp_path / "metadata.jsonl"
    sidecar.write_text(
        json.dumps(
            {
                "filepath": str(media_file),
                "id": "child-a",
                "user": {"name": "poster"},
                "tags": ["one", "two"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    metadata = sidecars_module._read_metadata_sidecar(str(sidecar))

    row = metadata[path_key(media_file)]
    assert row["id"] == "child-a"
    assert row["user[name]"] == "poster"
    assert row["tags"] == "one, two"


def test_after_move_metadata_path_wins_over_scratch_progress_paths(tmp_path: Path):
    final = tmp_path / "Templated Creator" / "Templated title [abc123].webm"
    final.parent.mkdir()
    final.write_bytes(b"video")
    missing_scratch_intermediate = tmp_path / "scratch" / "raw.f399.webm"

    paths = sidecars_module._metadata_output_paths(
        {
            path_key(final): {
                "filepath": str(final),
                "id": "abc123",
                "title": "Templated title",
            },
            path_key(missing_scratch_intermediate): {
                "filepath": str(missing_scratch_intermediate),
            },
        }
    )

    assert paths == [final]


@pytest.mark.parametrize(
    ("folder_template", "folder", "resolved"),
    [("{{username}} {{nickname}}", "handle Display", {"nickname": "Display"}), ("{{username}}", "handle", {})],
)
def test_a_download_records_the_tokens_it_was_filed_by(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, folder_template: str, folder: str, resolved: dict
):
    use_temp_db(tmp_path, monkeypatch)
    _save_example_fields(username=["uploader_id"], nickname=["uploader"])
    raw = tmp_path / "Clip [abc123].mp4"
    raw.write_bytes(b"video")
    saved: dict[str, dict] = {}
    monkeypatch.setattr(
        history_module,
        "save_history_entry_row",
        lambda task_id, payload: saved.update({task_id: payload}),
    )

    finalized = finalize_module._finalize_completed_output(
        source_url=_FIELDS_URL,
        source_key="example",
        output_root=tmp_path,
        raw_path=raw,
        metadata={"uploader_id": "handle", "uploader": "Display", "title": "Clip"},
        media_id="abc123",
        template_settings={"folder_template": folder_template, "filename_template": "{{title}} [{{id}}]"},
        cache_dropper=None,
    )
    history_module.save_history_entry("gallerydl:abc123", finalized.history_fields())

    assert finalized.final_path == tmp_path / folder / "Clip [abc123].mp4"
    row = saved["gallerydl:abc123"]
    assert (row["creator"], row["title"], row["resolved_tokens"]) == ("handle", "Clip", resolved)
