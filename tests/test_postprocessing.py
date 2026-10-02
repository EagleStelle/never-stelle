from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from yt_dlp import YoutubeDL

import backend.app.domains.downloads.postprocessing.containers as containers_module
import backend.app.domains.downloads.postprocessing.ffmpeg as ffmpeg_module
import backend.app.domains.downloads.postprocessing.payloads as payloads_module
import backend.app.domains.downloads.postprocessing.session as session_module
import backend.app.domains.downloads.postprocessing.subtitles as subtitles_module
import backend.app.domains.downloads.postprocessing.tags as tags_module
import backend.app.domains.downloads.postprocessing.thumbnails as thumbnails_module
import backend.app.domains.downloads.postprocessing.xmp as xmp_module
from backend.app.domains.downloads.workers.completion.finalize import FinalizedCompletionOutput

_CAPTION_PAYLOAD = {
    "language": "ja",
    "subtitles": {
        "en": [{"ext": "vtt", "data": "WEBVTT\n\nEnglish"}],
        "ja": [{"ext": "vtt", "data": "WEBVTT\n\nJapanese"}],
        "fr": [{"ext": "vtt", "data": "WEBVTT\n\nFrench"}],
    },
    "automatic_captions": {
        "en": [{"ext": "vtt", "data": "WEBVTT\n\nTranslated"}],
        "ja-orig": [{"ext": "vtt", "data": "WEBVTT\n\nOriginal ASR"}],
        "fr": [{"ext": "vtt", "data": "WEBVTT\n\nFrench auto"}],
        "de": [{"ext": "vtt", "data": "WEBVTT\n\nGerman auto"}],
        "es": [{"ext": "vtt", "data": "WEBVTT\n\nSpanish auto"}],
        "zh-Hans": [{"ext": "vtt", "data": "WEBVTT\n\nChinese auto"}],
    },
}


def _caption_tracks(languages: list[str]) -> list[tuple[str, bool]]:
    tracks = subtitles_module._subtitle_tracks(
        None, _CAPTION_PAYLOAD, manual=True, automatic=True, languages=languages
    )
    return [(track["language"], track["automatic"]) for track in tracks]


def test_extractor_metadata_sidecars_are_discovered_in_task_scratch(tmp_path: Path):
    output_root = tmp_path / "media"
    raw = output_root / "creator" / "raw.mp4"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"media")
    extractor_root = tmp_path / "scratch" / "task" / "extractor"
    nested = extractor_root / "creator"
    nested.mkdir(parents=True)
    sidecar = nested / "raw.info.json"
    sidecar.write_text('{"title":"Raw title"}', encoding="utf-8")

    index = payloads_module.scratch_payload_index((extractor_root, tmp_path / "missing"))
    assert payloads_module.metadata_sidecars_for(raw, index) == [sidecar]


def test_delegated_ytdlp_info_json_preserves_subtitles_for_gallerydl(tmp_path: Path):
    media = tmp_path / "media" / "clip.mkv"
    media.parent.mkdir()
    media.write_bytes(b"media")
    parts_root = tmp_path / "parts"
    parts_root.mkdir()
    info = {
        "id": "clip",
        "title": "clip",
        "ext": "mkv",
        "extractor": "generic",
        "extractor_key": "Generic",
        "subtitles": {"en": [{"ext": "vtt", "url": "https://example.test/en.vtt"}]},
        "automatic_captions": {"ja-orig": [{"ext": "vtt", "url": "https://example.test/ja.vtt"}]},
    }

    # The same params gallery-dl's handoff passes, and the outtmpl it sets for a part file.
    ydl = YoutubeDL({"quiet": True, "writeinfojson": True, "clean_infojson": False})
    ydl.params["outtmpl"] = {"default": str(parts_root / "clip.%(ext)s")}
    assert ydl._write_info_json("video", info, ydl.prepare_filename(info, "infojson"))

    index = payloads_module.scratch_payload_index((parts_root,))
    sidecars = payloads_module.metadata_sidecars_for(media, index)
    assert len(sidecars) == 1
    payload = payloads_module.extractor_payload_from_sidecars(sidecars, {})
    assert payload["subtitles"] == info["subtitles"]
    assert payload["automatic_captions"] == info["automatic_captions"]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"release_date": "2026"}, "2026"),
        ({"release_date": "2026-08"}, "2026-08"),
        ({"release_date": "2026-08-01"}, "2026-08-01"),
        ({"release_date": "20260801"}, "2026-08-01"),
        ({"release_date": "2026-08-01T19:42:31+08:00"}, "2026-08-01"),
        ({"upload_date": "2026-08-01 19:42:31"}, "2026-08-01"),
        ({"timestamp": 1785542400}, "2026-08-01"),
        ({"timestamp": 1785542400000}, "2026-08-01"),
    ],
)
def test_metadata_date_preserves_calendar_precision_without_time(
    payload: dict[str, object], expected: str
):
    assert tags_module._metadata_date(payload) == expected


def test_song_metadata_uses_real_track_numbers_and_portable_music_fields(tmp_path: Path):
    media = tmp_path / "song.mp3"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=YtDemoVid01",
        source_key="youtube",
        creator="Uploader channel",
        media_id="YtDemoVid01",
        final_path=media,
        display_filename=media.name,
        title="Video title",
        keep_paths=[media],
    )
    payload = tags_module.finalized_metadata_payload({
            "track": "Actual song title",
            "track_number": 3,
            "track_count": 12,
            "playlist_index": 99,
            "disc_number": "2",
            "disc_count": 2,
            "artists": ["Track Artist", "Guest Artist"],
            "album_artists": ["Album Artist"],
            "composers": ["Composer One", "Composer Two"],
            "performers": ["Orchestra"],
            "genres": ["Rock", "Pop"],
            "album": "Album",
        }, finalized)

    assert payload["title"] == "Actual song title"
    assert payload["artist"] == "Track Artist, Guest Artist"
    assert payload["album_artist"] == "Album Artist"
    assert payload["composer"] == "Composer One, Composer Two"
    assert payload["performer"] == "Orchestra"
    assert payload["genre"] == "Rock, Pop"
    assert payload["track"] == "3/12"
    assert payload["disc"] == "2/2"


def test_song_title_and_playlist_position_are_never_used_as_track_number(tmp_path: Path):
    media = tmp_path / "song.m4a"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=x",
        source_key="youtube",
        creator="Artist",
        media_id="x",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )

    payload = tags_module.finalized_metadata_payload({"track": "Song title", "playlist_index": 7}, finalized)

    assert "track" not in payload


def test_metadata_title_rejects_carousel_position_and_synthetic_delegation_url(
    tmp_path: Path,
):
    media = tmp_path / "post.webm"
    finalized = FinalizedCompletionOutput(
        source_url="https://example.test/post/abc",
        source_key="example",
        creator="Creator",
        media_id="abc",
        final_path=media,
        display_filename=media.name,
        title="",
        keep_paths=[media],
    )

    positional = tags_module.finalized_metadata_payload({"title": "20", "num": 20, "count": 21}, finalized)
    delegated = tags_module.finalized_metadata_payload({
            "title": "20",
            "original_url": "https://example.test/post/abc/20.mp4",
        }, finalized)
    legitimate = tags_module.finalized_metadata_payload(
        {"title": "20", "original_url": "https://example.test/post/abc"}, finalized
    )
    empty = tags_module.finalized_metadata_payload({"title": "None"}, finalized)

    assert "title" not in positional
    assert "title" not in delegated
    assert legitimate["title"] == "20"
    assert "title" not in empty


def test_youtube_generated_song_description_supplies_portable_credits(tmp_path: Path):
    media = tmp_path / "song.flac"
    finalized = FinalizedCompletionOutput(
        source_url="https://www.youtube.com/watch?v=x",
        source_key="youtube",
        creator="Artist",
        media_id="x",
        final_path=media,
        display_filename=media.name,
        title="Title",
        keep_paths=[media],
    )
    description = (
        "Provided to YouTube by Distributor\n\n"
        "Title · Artist\n\nAlbum\n\n℗ 2020 Label\n\n"
        "Composer: Composer Name\nWriter: Writer Name\n"
        "Associated Performer: Orchestra\n\nAuto-generated by YouTube."
    )

    payload = tags_module.finalized_metadata_payload({"description": description}, finalized)

    assert payload["composer"] == "Composer Name, Writer Name"
    assert payload["performer"] == "Orchestra"
    assert payload["copyright"] == "℗ 2020 Label"


def test_music_cover_art_precedes_scraped_thumbnail_and_has_a_fallback():
    cover = "https://yt3.googleusercontent.com/music-cover=w544-h544-l90-rj"
    scraped = "https://i.ytimg.com/vi/YtDemoVid01/maxresdefault.jpg"
    payload = {
        "track": "Song",
        "album": "Album",
        "thumbnail": scraped,
        "thumbnails": [
            {"url": cover, "width": 544, "height": 544, "preference": -39},
            {"url": scraped, "width": 1920, "height": 1080, "preference": -1},
        ],
    }

    assert thumbnails_module.thumbnail_url(payload, prefer_cover_art=True) == cover
    assert thumbnails_module.thumbnail_url(payload) == scraped


def test_regional_three_letter_caption_codes_match_a_two_letter_request():
    tracks = subtitles_module._subtitle_tracks(
        None,
        {
            "subtitles": {
                "spa-ES": [{"ext": "vtt", "data": "WEBVTT\n\nSpanish"}],
                "eng-US": [{"ext": "vtt", "data": "WEBVTT\n\nEnglish"}],
            }
        },
        manual=True,
        automatic=False,
        languages=[],
    )
    command: list[str] = []
    subtitles_module._subtitle_stream_metadata(command, 0, tracks[0])

    assert [track["language"] for track in tracks] == ["eng-US"]
    assert "language=eng" in command


def test_subtitles_default_to_the_source_language_alone():
    tracks = subtitles_module._subtitle_tracks(None, _CAPTION_PAYLOAD, manual=True, automatic=True, languages=[])

    assert [(track["language"], track["automatic"]) for track in tracks] == [
        ("ja", False),
        ("ja-orig", True),
    ]
    assert tracks[0]["data"].endswith(b"Japanese")
    # The ASR original, never the translated `en` endpoint.
    assert tracks[1]["data"].endswith(b"Original ASR")


def test_subtitles_honor_a_requested_language_list():
    assert _caption_tracks(["en", "zh"]) == [
        ("en", False),
        ("en", True),
        ("zh-Hans", True),
    ]


def test_subtitles_collect_every_manual_and_auto_caption_language_on_request():
    assert _caption_tracks(["all"]) == [
        ("ja", False),
        ("en", False),
        ("fr", False),
        ("ja-orig", True),
        ("en", True),
        ("fr", True),
        ("de", True),
        ("es", True),
        ("zh-Hans", True),
    ]


def test_subtitle_urls_download_through_ytdlp():
    vtt = b"WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n"
    url = "data:text/vtt;base64,V0VCVlRUCgowMDowMC4wMDAgLS0+IDAwOjAxLjAwMApoZWxsbwo="

    with session_module._ytdlp_session() as ydl:
        tracks = subtitles_module._subtitle_tracks(
            ydl, {"subtitles": {"en": [{"ext": "vtt", "url": url}]}}, manual=True, automatic=False, languages=[]
        )

    assert tracks == [{"language": "en", "automatic": False, "extension": "vtt", "data": vtt}]


def test_failed_subtitle_download_is_skipped_with_a_warning(caplog: pytest.LogCaptureFixture):
    from yt_dlp.utils import DownloadError

    def refuse(name, info, subtitle=False):
        assert subtitle
        assert info["http_headers"]["Referer"] == "https://example.test/watch"
        raise DownloadError("HTTP Error 429: Too Many Requests")

    tracks = subtitles_module._subtitle_tracks(
        SimpleNamespace(dl=refuse),
        {
            "webpage_url": "https://example.test/watch",
            "subtitles": {"en": [{"ext": "vtt", "url": "https://cdn.example.test/en.vtt"}]},
        },
        manual=True,
        automatic=False,
        languages=[],
    )

    assert tracks == []
    assert "HTTP Error 429" in caplog.text


def test_thumbnail_downloads_through_ytdlp_and_sniffs_its_format():
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
    requests: list[object] = []

    class Response:
        headers = {"Content-Type": "application/octet-stream"}

        def __init__(self) -> None:
            self._body = [png]

        def __enter__(self):
            return self

        def __exit__(self, *exc) -> None:
            return None

        def read(self, size: int) -> bytes:
            return self._body.pop() if self._body else b""

        def close(self) -> None:
            return None

    def urlopen(request):
        requests.append(request)
        return Response()

    data, extension = thumbnails_module._fetch_thumbnail(
        SimpleNamespace(urlopen=urlopen),
        {"thumbnail": "https://cdn.example.test/cover", "http_headers": {"Referer": "https://example.test/"}},
        prefer_cover_art=False,
    )

    assert (data, extension) == (png, ".png")
    assert requests[0].url == "https://cdn.example.test/cover"
    assert requests[0].headers["Referer"] == "https://example.test/"


def test_mp3_thumbnail_is_written_as_a_real_front_cover_tag(tmp_path: Path):
    from mutagen.id3 import ID3, PictureType

    media = tmp_path / "Title [abc].mp3"
    media.write_bytes(b"audio-payload")
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"jpeg-payload")

    assert thumbnails_module._embed_thumbnail_with_mutagen(media, cover)

    pictures = ID3(media).getall("APIC")
    assert len(pictures) == 1
    assert pictures[0].type == PictureType.COVER_FRONT
    assert pictures[0].mime == "image/jpeg"
    assert pictures[0].data == b"jpeg-payload"
    assert media.read_bytes().endswith(b"audio-payload")


def test_webp_metadata_embed_adds_standard_xmp_without_reencoding_pixels():
    # A minimal lossless WebP bitstream with a 2x3 canvas. The writer must add
    # VP8X/XMP chunks while preserving the original compressed VP8L payload.
    dimensions = 1 | (2 << 14)
    vp8l = b"\x2f" + dimensions.to_bytes(4, "little")
    chunk = xmp_module._webp_chunk(b"VP8L", vp8l)
    body = b"WEBP" + chunk
    original = b"RIFF" + len(body).to_bytes(4, "little") + body
    xmp = xmp_module._image_xmp_packet(
        {
            "title": "Photo",
            "artist": "Creator",
            "source": "https://example.test/post/abc",
            "identifier": "abc",
        }
    )

    embedded = xmp_module._webp_with_xmp(original, xmp)

    assert embedded is not None
    assert b"VP8X" in embedded
    assert b"XMP " in embedded
    assert vp8l in embedded
    assert b"<dc:source>https://example.test/post/abc</dc:source>" in embedded
    assert int.from_bytes(embedded[4:8], "little") == len(embedded) - 8


def test_image_xmp_drops_only_what_xml_cannot_carry():
    xmp = xmp_module._image_xmp_packet({"title": "Live:\x01 A/B?\x1f <3"})

    assert b'<rdf:li xml:lang="x-default">Live: A/B? &lt;3</rdf:li>' in xmp


def test_finished_video_repairs_codec_mismatches_from_any_extractor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisomvp9-input")
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, path):
        if Path(path) == media:
            return [
                {"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"},
                {"codec_type": "audio", "codec_name": "opus", "codec_tag_string": "Opus"},
            ]
        return [
            {"codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1"},
            {"codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a"},
        ]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"portable-output")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(containers_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(containers_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", fake_run)

    assert containers_module.ensure_container_codec_compatibility(
        [media], {"mode": "merged", "video_container": "mp4"}
    )
    assert media.read_bytes() == b"portable-output"
    assert "libx264" in commands[0]
    assert "aac" in commands[0]


def test_finished_video_auto_mode_never_runs_compatibility_transcode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisomvp9-input")

    def compatible_streams(_ffmpeg, _path):
        return [
            {"codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1"},
            {"codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a"},
        ]

    def unexpected_run(*_args, **_kwargs):
        raise AssertionError("Compatible Auto output must not be remuxed or transcoded")

    monkeypatch.setattr(containers_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(containers_module, "_ffprobe_streams", compatible_streams)
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", unexpected_run)

    assert not containers_module.ensure_container_codec_compatibility(
        [media],
        {
            "mode": "merged",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
        },
    )


def test_finished_video_losslessly_repairs_empty_vpcc_in_auto_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def box(kind: bytes, payload: bytes) -> bytes:
        return (len(payload) + 8).to_bytes(4, "big") + kind + payload

    empty_vpcc = box(b"vpcC", b"")
    sample_entry = box(b"vp09", (b"\0" * 78) + empty_vpcc)
    stsd = box(b"stsd", (b"\0" * 4) + (1).to_bytes(4, "big") + sample_entry)
    media = tmp_path / "clip.mp4"
    stbl = box(b"stbl", stsd)
    media.write_bytes(
        box(b"ftyp", b"isom")
        + box(b"moov", box(b"trak", box(b"mdia", box(b"minf", stbl))))
    )
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, path):
        if b"\x00\x00\x00\x08vpcC" in Path(path).read_bytes():
            return []
        return [{"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"}]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(Path(cmd[cmd.index("-i") + 1]).read_bytes())
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(containers_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(containers_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", fake_run)

    paths = [media]
    updates: dict[Path, Path] = {}
    assert containers_module.ensure_container_codec_compatibility(
        paths,
        {
            "mode": "merged",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
        },
        path_updates=updates,
    )
    assert commands and commands[0][commands[0].index("-c") + 1] == "copy"
    assert "libx264" not in commands[0]
    assert paths[0].suffix == ".webm"
    assert paths[0].is_file()
    assert not media.exists()
    assert updates == {media: paths[0]}


def test_finished_video_auto_remuxes_vp9_mp4_to_webm_without_encoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"\x00\x00\x00\x18ftypisom-valid-vp9")
    paths = [media]
    updates: dict[Path, Path] = {}
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, _path):
        return [
            {"codec_type": "video", "codec_name": "vp9", "codec_tag_string": "vp09"},
            {"codec_type": "audio", "codec_name": "opus", "codec_tag_string": "Opus"},
        ]

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"webm-stream-copy")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(containers_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(containers_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", fake_run)

    assert containers_module.ensure_container_codec_compatibility(
        paths,
        {
            "mode": "merged",
            "video_quality": "best",
            "video_container": "auto",
            "video_codec": "auto",
        },
        path_updates=updates,
    )
    assert paths[0] == tmp_path / "clip.webm"
    assert paths[0].read_bytes() == b"webm-stream-copy"
    assert not media.exists()
    assert commands[0][commands[0].index("-c") + 1] == "copy"
    assert all("libvpx" not in argument and "libx" not in argument for argument in commands[0])


def test_finished_video_only_strips_audio_only_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    muxed = tmp_path / "muxed.mp4"
    muxed.write_bytes(b"\x00\x00\x00\x18ftypisom-muxed")
    silent = tmp_path / "silent.mp4"
    silent.write_bytes(b"\x00\x00\x00\x18ftypisom-silent")
    commands: list[list[str]] = []

    def fake_streams(_ffmpeg, path):
        streams = [{"codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1"}]
        if Path(path) == muxed:
            streams.append({"codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a"})
        return streams

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"video-stream-copy")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(containers_module, "detect_ffmpeg_location", lambda: "/usr/bin/ffmpeg")
    monkeypatch.setattr(containers_module, "_ffprobe_streams", fake_streams)
    monkeypatch.setattr(ffmpeg_module, "run_task_subprocess", fake_run)

    # Merged keeps its audio.
    assert not containers_module.ensure_container_codec_compatibility([muxed, silent], {"mode": "merged"})
    assert not commands

    assert containers_module.ensure_container_codec_compatibility([muxed, silent], {"mode": "video"})
    # Only the file that carried audio costs an ffmpeg pass, and it copies streams.
    assert len(commands) == 1
    assert commands[0][commands[0].index("-i") + 1] == str(muxed)
    assert commands[0][commands[0].index("-0:a") - 1] == "-map"
    assert commands[0][commands[0].index("-c") + 1] == "copy"
    assert muxed.read_bytes() == b"video-stream-copy"
    assert silent.read_bytes() == b"\x00\x00\x00\x18ftypisom-silent"
