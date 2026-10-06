from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.domains.downloads.links.urls as urls_module
import backend.app.domains.downloads.metadata.creators as creators_module
import backend.app.domains.downloads.metadata.folders as folders_module
import backend.app.domains.downloads.metadata.pipeline as pipeline_module
import backend.app.domains.downloads.metadata.values as values_module
import backend.app.domains.downloads.workers.completion.sidecars as sidecars_module
from tests.support import patch_head

_SUBFOLDER_TEMPLATES = {
    "folder_template": "{{username}}",
    "subfolder_template": "{{id}}",
    "filename_template": "{{title}} [{{id}}]",
}


def _downloaded_group(tmp_path: Path, names: list[str]) -> list[Path]:
    source = tmp_path / "Creator"
    source.mkdir()
    paths = []
    for name in names:
        path = source / name
        path.write_bytes(b"media")
        paths.append(path)
    return paths


def test_filename_creator_ignores_nickname_token_for_handle():
    # A {{nickname}} filename must NOT feed the {{username}} handle with the display name.
    path = Path("/media/instagram/nasa/NASA - Cool Rocket [ABC123].jpg")
    creator = creators_module._filename_creator(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        {},
        "https://www.instagram.com/p/Cxyz/",
        "ABC123",
    )
    assert creator == ""


def test_render_template_folder_takes_the_nickname_for_a_missing_username():
    folder = folders_module.render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{username}}"},
        creator="",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder == Path("/media/instagram/NASA")


def test_render_template_folder_names_a_missing_value_unknown():
    folder = folders_module.render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{username}}/{{title}}"},
        creator="",
        media_id="ABC123",
    )
    assert folder == Path("/media/instagram/Unknown/Unknown")


def test_render_template_folder_renders_nickname_distinct_from_username():
    folder = folders_module.render_template_folder(
        Path("/media/instagram"),
        {"folder_template": "{{nickname}}"},
        creator="nasa",
        media_id="ABC123",
        nickname="NASA",
    )
    assert folder == Path("/media/instagram/NASA")


def test_render_template_folder_renders_selected_quality():
    folder = folders_module.render_template_folder(
        Path("/media/rule34video"),
        {"folder_template": "{{quality}}/{{username}}"},
        creator="artist",
        media_id="4483553",
        quality={"mode": "merged", "video_quality": "1080p"},
    )

    assert folder == Path("/media/rule34video/1080p/artist")


def test_render_template_folder_handle_at_cleanup_can_be_disabled():
    root = Path("/media/tiktok")
    template = {"folder_template": "{{username}}"}

    assert folders_module.render_template_folder(root, template, "@alice", "abc123") == root / "alice"
    assert (
        folders_module.render_template_folder(
            root,
            template,
            "@alice",
            "abc123",
            cleaning={"strip_handle_at": False},
        )
        == root / "@alice"
    )


def test_filename_nickname_recovers_display_name_from_gallerydl_folder():
    # gallery-dl ships no metadata; the display name only survives in the folder it wrote.
    root = Path("/media/instagram")
    path = root / "NASA" / "nasa - Cool Rocket [ABC123].jpg"
    nickname = creators_module._filename_nickname(
        path,
        "{{username}} - {{title}} [{{id}}]",
        "{{nickname}}",
        creators_module._template_folder_text(root, path),
        {},
    )
    assert nickname == "NASA"


def test_filename_nickname_skips_username_value_and_uses_display_metadata():
    root = Path("/media/tiktok")
    path = root / "fakeacc.com" / "fakeacc.com - Clip [7100000000000000001].mp4"
    metadata = {
        "webpage_url": "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001",
        "channel": "Clip Demo",
        "uploader": "fakeacc.com",
        "uploader_url": "https://www.tiktok.com/@fakeacc.com",
    }

    nickname = creators_module._filename_nickname(
        path,
        "{{nickname}} - {{title}} [{{id}}]",
        "{{username}}",
        creators_module._template_folder_text(root, path),
        metadata,
        "fakeacc.com",
    )

    assert nickname == "Clip Demo"


def test_metadata_creator_prefers_resolved_handle_over_display_name(monkeypatch):
    patch_head(monkeypatch, "https://www.facebook.com/demopage")
    metadata = {
        "uploader": "Pagename",
        "channel": "Pagename",
        "uploader_id": "100000000000001",
        "original_url": "https://www.facebook.com/reel/800000000000001",
    }
    assert creators_module._metadata_creator(metadata, "800000000000001") == "demopage"


def test_metadata_creator_skips_mobile_host_wall(monkeypatch):
    # yt-dlp's webpage_url is often a mobile host that walls a bare-id fetch; the apex/www host must win.
    def fake_head(url, **kwargs):
        target = "https://m.facebook.com/login/?next=x" if "m.facebook.com" in url else "https://www.facebook.com/demopage"
        return type("Resp", (), {"url": target})()

    monkeypatch.setattr(urls_module.httpx, "head", fake_head)
    metadata = {
        "uploader": "Pagename",
        "uploader_id": "100000000000001",
        "webpage_url": "https://m.facebook.com/watch/?v=1000000000000001",
        "original_url": "https://www.facebook.com/reel/1000000000000001",
    }
    assert creators_module._metadata_creator(metadata, "1000000000000001") == "demopage"


def test_metadata_creator_prefers_at_handle_metadata():
    metadata = {
        "channel": "Mock",
        "uploader": "Mock",
        "creator": "Mock",
        "uploader_id": "@mock",
        "channel_id": "UC-DemoChannel0000000001",
        "webpage_url": "https://video.example/watch?v=YtDemoVid05",
    }

    assert creators_module._metadata_creator(metadata, "YtDemoVid05") == "mock"


def test_metadata_creator_rejects_opaque_id_metadata():
    metadata = {
        "channel": "UC-DemoChannel0000000001",
        "uploader": "",
        "channel_id": "UC-DemoChannel0000000001",
        "webpage_url": "https://video.example/watch?v=YtDemoVid05",
    }

    assert creators_module._metadata_creator(metadata, "YtDemoVid05") == ""


def test_filename_creator_uses_handle_metadata_without_at():
    metadata = {
        "channel": "Mock",
        "uploader": "Mock",
        "creator": "Mock",
        "uploader_id": "@mock",
        "channel_id": "UC-DemoChannel0000000001",
        "webpage_url": "https://video.example/watch?v=YtDemoVid05",
    }

    creator = creators_module._filename_creator(
        Path("@mock - Glass Garden [YtDemoVid05].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        metadata,
        "https://video.example/watch?v=YtDemoVid05",
        "YtDemoVid05",
    )

    assert creator == "mock"


def test_filename_creator_strips_at_from_filename_username():
    creator = creators_module._filename_creator(
        Path("@mock - Glass Garden [YtDemoVid05].mp4"),
        "{{username}} - {{title}} [{{id}}]",
        {},
        "https://video.example/watch?v=YtDemoVid05",
        "YtDemoVid05",
    )

    assert creator == "mock"


def test_configured_title_fields_are_authoritative():
    assert values_module.metadata_title(
        sidecars_module._extractor_metadata_fields(
            {"headline": "Configured headline", "title": "Extractor title"}
        ),
        ["headline", "title"],
    ) == "Configured headline"


def test_configured_title_fields_do_not_fall_through_to_extractor_title():
    assert values_module.metadata_title(
        {"description": "Configured caption", "title": "20"},
        ["description"],
    ) == "Configured caption"
    assert values_module.metadata_title(
        {"description": "", "title": "20"},
        ["description"],
    ) == ""


def test_configured_field_value_honors_opaque_id_in_priority_order():
    # channel_id first in the configured order must win, even though the handle
    # heuristics reject it as an opaque identifier.
    metadata = {
        "uploader": "Mock",
        "channel": "Mock",
        "channel_id": "UC-DemoChannel0000000001",
    }

    assert (
        creators_module.configured_field_value(metadata, ["channel_id", "uploader"])
        == "UC-DemoChannel0000000001"
    )


def test_configured_field_value_empty_order_defers_to_heuristics():
    metadata = {"channel_id": "UC-DemoChannel0000000001", "uploader": "Mock"}

    assert creators_module.configured_field_value(metadata, []) == ""


def test_gallerydl_parent_group_keeps_pasted_source_url_for_child_metadata():
    source_url = "https://www.example.test/post/root123"
    metadata = {
        "webpage_url": "https://www.example.test/item/child-a",
    }

    assert (
        pipeline_module._item_source_url(source_url, "example", "root123", "poster", metadata)
        == source_url
    )


@pytest.mark.parametrize("placeholder", ["None", "null"])
def test_a_download_filed_under_a_placeholder_creator_moves_to_unknown(tmp_path: Path, placeholder: str):
    # The engine's own folder token came back null, so it invented a directory.
    stranded = tmp_path / placeholder
    stranded.mkdir()
    path = stranded / "Clip [abc123].mp4"
    path.write_bytes(b"video")

    final_path = folders_module.move_group_to_template_folder(
        path,
        tmp_path,
        {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        "",
        "abc123",
    )

    assert final_path == tmp_path / "Unknown" / path.name
    assert final_path.is_file()
    assert not stranded.exists()


def test_a_real_creator_folder_is_left_alone(tmp_path: Path):
    kept = tmp_path / "Creator"
    kept.mkdir()
    path = kept / "Clip [abc123].mp4"
    path.write_bytes(b"video")

    final_path = folders_module.move_group_to_template_folder(
        path,
        tmp_path,
        {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
        "",
        "abc123",
    )

    assert final_path == path
    assert path.is_file()


def test_a_multi_file_post_moves_into_its_own_subfolder(tmp_path: Path):
    paths = _downloaded_group(tmp_path, ["Cap [abc123]_1.jpg", "Cap [abc123]_2.jpg"])

    final_path = folders_module.move_group_to_template_folder(
        paths[0],
        tmp_path,
        _SUBFOLDER_TEMPLATES,
        "Creator",
        "abc123",
        group_paths=paths,
    )

    subfolder = tmp_path / "Creator" / "abc123"
    assert final_path == subfolder / "Cap [abc123]_1.jpg"
    assert sorted(path.name for path in subfolder.iterdir()) == [
        "Cap [abc123]_1.jpg",
        "Cap [abc123]_2.jpg",
    ]


def test_a_single_file_download_skips_the_subfolder(tmp_path: Path):
    paths = _downloaded_group(tmp_path, ["Clip [abc123].mp4"])

    final_path = folders_module.move_group_to_template_folder(
        paths[0],
        tmp_path,
        _SUBFOLDER_TEMPLATES,
        "Creator",
        "abc123",
        group_paths=paths,
    )

    assert final_path == tmp_path / "Creator" / "Clip [abc123].mp4"


def test_an_empty_subfolder_template_keeps_a_multi_file_post_flat(tmp_path: Path):
    paths = _downloaded_group(tmp_path, ["Cap [abc123]_1.jpg", "Cap [abc123]_2.jpg"])

    final_path = folders_module.move_group_to_template_folder(
        paths[0],
        tmp_path,
        {**_SUBFOLDER_TEMPLATES, "subfolder_template": ""},
        "Creator",
        "abc123",
        group_paths=paths,
    )

    assert final_path == tmp_path / "Creator" / "Cap [abc123]_1.jpg"
