from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.services.tasks.gallerydl as gallerydl
from backend.app.services.tasks import engine_by_name, engine_for_task, select_engine
from backend.app.services.tasks.engine import all_engines
from backend.app.services.tasks.worker import _count_progress, _looks_unsupported


@pytest.mark.parametrize(
    "url",
    [
        "https://www.youtube.com/watch?v=1",
        "https://www.pixiv.net/en/artworks/12345",
        "https://i.imgur.com/abc.jpg",
        "not a url",
    ],
)
def test_select_engine_always_defaults_to_ytdlp(url):
    assert select_engine(url).name == "ytdlp"


def test_all_engines_includes_both_backends():
    assert {engine.name for engine in all_engines()} == {"ytdlp", "gallerydl"}


def test_engine_by_name_falls_back_to_ytdlp():
    assert engine_by_name("gallerydl").name == "gallerydl"
    assert engine_by_name("ytdlp").name == "ytdlp"
    assert engine_by_name("bogus").name == "ytdlp"


def test_engine_for_task_prefers_explicit_engine_over_url():
    task = {"engine": "gallerydl", "source_url": "https://www.youtube.com/watch?v=1"}
    assert engine_for_task(task).name == "gallerydl"


def test_engine_for_task_defaults_to_ytdlp_when_untagged():
    assert engine_for_task({"source_url": "https://www.pixiv.net/en/artworks/1"}).name == "ytdlp"


def test_looks_unsupported_flags_wrong_engine_errors():
    assert _looks_unsupported({"last_log_lines": ["ERROR: Unsupported URL: https://tiktok.com/@x/photo/1"]}) is True
    assert _looks_unsupported({"last_log_lines": ["yt_dlp.utils.UnsupportedError: Unsupported URL: ..."]}) is True
    assert _looks_unsupported({"last_log_lines": ["No suitable extractor found"]}) is True
    assert _looks_unsupported({"last_log_lines": ["ERROR: Video unavailable"]}) is False
    assert _looks_unsupported({}) is False


def test_ytdlp_engine_progress_and_path_parsing():
    engine = engine_by_name("ytdlp")
    assert engine.parse_progress("[download]  50.0% of 10MiB") == 50.0
    assert engine.parse_progress("[info] not a progress line") is None
    assert engine.extract_output_path("[download] Destination: /media/a.mp4") == "/media/a.mp4"


def test_gallerydl_engine_progress_and_path_parsing():
    engine = engine_by_name("gallerydl")
    assert engine.parse_progress("no percentages here") is None
    assert engine.extract_output_path("/media/imgur/artist/photo.jpg") == "/media/imgur/artist/photo.jpg"
    assert engine.extract_output_path('"/media/imgur/artist/photo.png"') == "/media/imgur/artist/photo.png"
    assert engine.extract_output_path("[download] skipping existing file") == ""
    assert engine.extract_output_path("/media/imgur/notes.txt") == ""


def test_convert_template_to_gallerydl_maps_fields_and_resolves_creator():
    result = gallerydl.convert_template_to_gallerydl(
        "{{creator}} - {{title}} [{{id}}]",
        "https://twitter.com/DohaVT/status/2073635724684054528",
    )
    assert result.startswith("DohaVT - ")
    assert '{title|content|"untitled"}' in result
    assert '{id|num|"NA"}' in result


def test_convert_template_to_gallerydl_falls_back_to_metadata_creator():
    # No creator segment in the URL -> emit a gallery-dl field with fallbacks.
    result = gallerydl.convert_template_to_gallerydl("{{creator}}", "https://imgur.com/abc")
    assert result == '{user[name]|username|author|"unknown"}'


def test_engine_progress_style_flags():
    assert engine_by_name("ytdlp").emits_progress is True
    assert engine_by_name("gallerydl").emits_progress is False


def test_count_progress_known_total_caps_below_100():
    assert _count_progress(0, 4) == 0.0
    assert _count_progress(2, 4) == 50.0
    assert _count_progress(4, 4) == 99.0
    assert _count_progress(9, 4) == 99.0


def test_count_progress_unknown_total_is_monotonic_below_100():
    first = _count_progress(1, 0)
    second = _count_progress(2, 0)
    later = _count_progress(20, 0)
    assert 0 < first < second < later < 100


def test_build_gallerydl_output_template_adds_num_for_slideshows():
    template = gallerydl.build_gallerydl_output_template("https://www.tiktok.com/@x/photo/1", "/media/tiktok")
    _, _, filename = template.partition(gallerydl._TEMPLATE_SEP)
    assert filename.endswith("_{num}.{extension}")


def test_build_gallerydl_command_layout():
    sep = gallerydl._TEMPLATE_SEP
    output_template = f"artist{sep}Clip [id].{{extension}}"
    cmd = gallerydl.build_gallerydl_command(
        "https://imgur.com/a/x",
        "/media/imgur",
        output_template,
    )
    assert cmd[0] == "gallery-dl"
    directory = cmd[cmd.index("--directory") + 1]
    assert directory == str(Path("/media/imgur") / "artist")
    assert cmd[cmd.index("--filename") + 1] == "Clip [id].{extension}"
    assert "--cookies" not in cmd
    assert cmd[-1] == "https://imgur.com/a/x"
