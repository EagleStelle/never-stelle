from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.domains.downloads.rename as rename_module
import backend.app.domains.downloads.scan as scan_module

OLD_TEMPLATE = "{{username}} - {{title}} [{{id}}]"
NEW_TEMPLATE = "{{title}} ({{username}}) [{{id}}]"


def _pin_template(monkeypatch: pytest.MonkeyPatch, filename_template: str) -> None:
    """Pin what the settings currently say, both the answer and the set it comes from."""
    monkeypatch.setattr(
        rename_module,
        "get_effective_template_settings",
        lambda source_url="": {"folder_template": "{{username}}", "filename_template": filename_template},
    )
    monkeypatch.setattr(rename_module, "possible_filename_templates", lambda source_key: {filename_template})


def _rename_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, template: str = NEW_TEMPLATE):
    """A scan wired to an in-memory history, with the effective template pinned."""
    media_root = tmp_path / "media"
    media_root.mkdir()
    rows: dict[str, dict] = {}
    journal: dict[str, tuple[str, str]] = {}

    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_history", lambda: {"entries": dict(rows)})
    monkeypatch.setattr(scan_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(scan_module, "save_history_entry_rows", lambda batch: rows.update(dict(batch)))
    monkeypatch.setattr(scan_module, "remove_task_record", lambda task_id: None)
    monkeypatch.setattr(scan_module, "remove_history_record", lambda task_id: rows.pop(task_id, None))
    monkeypatch.setattr(scan_module, "resolution_revision", lambda: "rev-1")
    monkeypatch.setattr(scan_module, "seeded_download_ids", set)
    monkeypatch.setattr(scan_module, "mark_downloads_seeded", lambda ids: None)

    monkeypatch.setattr(rename_module, "save_history_entry_rows", lambda batch: rows.update(dict(batch)))
    _pin_template(monkeypatch, template)
    monkeypatch.setattr(rename_module, "get_effective_title_cleaning", lambda source_url="": {})
    monkeypatch.setattr(
        rename_module, "begin_rename", lambda task_id, old, new: journal.__setitem__(task_id, (old, new))
    )
    monkeypatch.setattr(
        rename_module,
        "finish_renames",
        lambda task_ids: [journal.pop(task_id, None) for task_id in task_ids],
    )
    monkeypatch.setattr(rename_module, "open_renames", list)
    return media_root, rows, journal


def _row(path: Path, **overrides) -> dict:
    row = {
        "engine": "gallerydl",
        "source_url": "https://example.com/p/abc123",
        "source_key": "example",
        "creator": "Creator",
        "title": "Clip",
        "media_id": "abc123",
        "resolved_full_path": str(path),
        "resolved_folder": str(path.parent),
        "resolved_filename": path.name,
        "folder_template": "{{username}}",
        "filename_template": OLD_TEMPLATE,
        "scan_mtime_ns": 111,
        "scan_revision": "rev-1",
    }
    row.update(overrides)
    return row


def test_template_change_renames_the_file_and_the_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)

    result = scan_module.scan_media_library([media_root])

    new_file = media_root / "Clip (Creator) [abc123].mp4"
    assert new_file.exists()
    assert not old_file.exists()
    assert result["renamed"] == 1
    row = rows["gallerydl:1"]
    assert row["resolved_full_path"] == str(new_file)
    assert row["resolved_filename"] == new_file.name
    # The stamp moves with the file, so the next pass sees the row as current.
    assert row["filename_template"] == NEW_TEMPLATE


@pytest.mark.parametrize(
    ("recorded", "expected"),
    [({"quality": "1080"}, "Clip [abc123] 1080.mp4"), ({}, "Clip [abc123] source.mp4")],
)
def test_a_quality_token_renders_what_the_row_has_or_calls_it_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorded: dict, expected: str
):
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch, template="{{title}} [{{id}}] {{quality}}")
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    # A history row carries no quality selection, only what its download recorded.
    rows["gallerydl:1"] = _row(old_file, resolved_tokens=recorded)

    result = scan_module.scan_media_library([media_root])

    # Reading the absent selection as a selection stamped every row "source"; treating
    # the token as unanswerable left rows flagged for something no probe can supply.
    assert (media_root / expected).exists()
    assert result["needs_resolve"] == 0


def test_unchanged_template_renames_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch, template=OLD_TEMPLATE)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)

    result = scan_module.scan_media_library([media_root])

    assert result["renamed"] == 0
    assert result["needs_resolve"] == 0
    assert old_file.exists()
    assert rows["gallerydl:1"]["resolved_full_path"] == str(old_file)


def test_rename_preserves_the_resolution_signature(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A rename changes neither the bytes nor the rules, so the renamed file must be
    # skipped by the walk that follows it rather than resolved (and probed) again.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    stat_result = old_file.stat()
    resolved: list[str] = []
    real_parse = scan_module._parse_media_fields
    monkeypatch.setattr(
        scan_module,
        "_parse_media_fields",
        lambda path, pattern: (resolved.append(str(path)), real_parse(path, pattern))[1],
    )
    rows["disk:abc123"] = _row(
        old_file,
        engine="disk",
        scan_mtime_ns=stat_result.st_mtime_ns,
        file_size=stat_result.st_size,
    )

    result = scan_module.scan_media_library([media_root])
    row = rows["disk:abc123"]

    assert result["renamed"] == 1
    # Carried across the rename, so the walk still recognises the file at its new name.
    assert row["scan_mtime_ns"] == stat_result.st_mtime_ns
    assert row["scan_revision"] == "rev-1"
    assert result["unchanged"] == 1
    assert resolved == []


def test_rename_avoids_collisions_with_a_numbered_suffix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    first = media_root / "Creator - Clip [abc123].mp4"
    second = media_root / "Creator - Clip [def456].mp4"
    first.write_bytes(b"video")
    second.write_bytes(b"video")
    # Both rows render to the same stem once the id token is dropped from the template.
    rows["gallerydl:1"] = _row(first)
    rows["gallerydl:2"] = _row(second, media_id="def456", source_url="https://example.com/p/def456")
    _pin_template(monkeypatch, "{{title}} - {{username}}")

    result = scan_module.scan_media_library([media_root])

    assert result["renamed"] == 2
    landed = sorted(path.name for path in media_root.iterdir())
    assert landed == ["Clip - Creator.mp4", "Clip - Creator_1.mp4"]


def test_multiple_possible_templates_still_resolve_per_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # The stored template being one the source can produce is only conclusive when it
    # is the only one; otherwise the effective template still has to be resolved.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)
    monkeypatch.setattr(
        rename_module, "possible_filename_templates", lambda source_key: {OLD_TEMPLATE, NEW_TEMPLATE}
    )

    result = scan_module.scan_media_library([media_root])

    assert result["renamed"] == 1
    assert (media_root / "Clip (Creator) [abc123].mp4").exists()


def test_rename_keeps_the_numbered_suffix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Siblings from one post are distinguished only by _1/_2, and the rest of the
    # pipeline groups them by it, so re-templating must carry it across.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    first = media_root / "Creator - Clip [abc123].mp4"
    second = media_root / "Creator - Clip [abc123]_1.mp4"
    third = media_root / "Creator - Clip [abc123]_2.mp4"
    for path in (first, second, third):
        path.write_bytes(b"video")
    rows["gallerydl:1"] = _row(first)
    rows["gallerydl:2"] = _row(second)
    rows["gallerydl:3"] = _row(third)

    result = scan_module.scan_media_library([media_root])

    assert result["renamed"] == 3
    assert sorted(path.name for path in media_root.iterdir()) == [
        "Clip (Creator) [abc123].mp4",
        "Clip (Creator) [abc123]_1.mp4",
        "Clip (Creator) [abc123]_2.mp4",
    ]


def test_missing_token_defers_to_resolve_instead_of_renaming(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The row has no nickname, and the creator tokens cannot cover a scraped token, so
    # renaming would silently drop it. The file is left alone for a resolve pass.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)
    _pin_template(monkeypatch, "{{uploader}} - {{title}} [{{id}}]")

    result = scan_module.scan_media_library([media_root])

    assert result["renamed"] == 0
    assert result["needs_resolve"] == 1
    assert old_file.exists()
    assert rows["gallerydl:1"]["filename_template"] == OLD_TEMPLATE


def test_nickname_falls_back_to_username_without_deferring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The two creator tokens name the same person, so substituting one for the other
    # loses nothing and must not be treated as missing data.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)
    _pin_template(monkeypatch, "{{nickname}} - {{title}} [{{id}}]")

    result = scan_module.scan_media_library([media_root])

    assert result["needs_resolve"] == 0
    assert (media_root / "Creator - Clip [abc123].mp4").exists()


def test_failed_rename_leaves_the_row_on_its_old_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # If the disk rename does not happen, the row must keep pointing at the file that
    # is actually there, or the next scan reads it as deleted.
    media_root, rows, journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - Clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file)

    def blocked(old: Path, new: Path) -> None:
        raise OSError("locked")

    monkeypatch.setattr(rename_module, "_swap_on_disk", blocked)

    result = scan_module.scan_media_library([media_root])

    assert result["rename_failed"] == 1
    assert result["renamed"] == 0
    assert old_file.exists()
    assert rows["gallerydl:1"]["resolved_full_path"] == str(old_file)
    # Nothing may be left behind in the journal for a rename that never happened.
    assert journal == {}


def test_interrupted_rename_is_settled_against_whichever_path_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    media_root = tmp_path / "media"
    media_root.mkdir()
    landed = media_root / "Clip (Creator) [abc123].mp4"
    landed.write_bytes(b"video")
    rows = {"gallerydl:1": _row(media_root / "Creator - Clip [abc123].mp4")}
    settled: list[str] = []

    monkeypatch.setattr(
        rename_module,
        "open_renames",
        lambda: [
            {
                "task_id": "gallerydl:1",
                "old_path": str(media_root / "Creator - Clip [abc123].mp4"),
                "new_path": str(landed),
            }
        ],
    )
    monkeypatch.setattr(rename_module, "finish_renames", settled.extend)
    monkeypatch.setattr(rename_module, "load_history_entry", lambda task_id: rows.get(task_id, {}))
    monkeypatch.setattr(
        rename_module, "save_history_entry_row", lambda task_id, payload: rows.__setitem__(task_id, payload)
    )

    assert rename_module.recover_interrupted_renames() == 1
    # The disk is the authority: the row is pointed at the name that actually landed.
    assert rows["gallerydl:1"]["resolved_full_path"] == str(landed)
    assert settled == ["gallerydl:1"]


def test_case_only_rename_lands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Windows treats a case-only rename as a no-op unless it goes through an
    # intermediate name.
    media_root, rows, _journal = _rename_env(tmp_path, monkeypatch)
    old_file = media_root / "Creator - clip [abc123].mp4"
    old_file.write_bytes(b"video")
    rows["gallerydl:1"] = _row(old_file, title="clip")
    _pin_template(monkeypatch, "{{username}} - {{title}} [{{id}}] ")
    monkeypatch.setattr(rename_module, "render_template_filename", lambda *a, **k: "Creator - Clip [abc123].mp4")

    scan_module.scan_media_library([media_root])

    landed = [path.name for path in media_root.iterdir()]
    assert landed == ["Creator - Clip [abc123].mp4"]


def test_resolution_revision_ignores_template_edits(monkeypatch: pytest.MonkeyPatch):
    # The whole point of the rename pass: editing a template must not invalidate every
    # resolved row in the library.
    import backend.app.db.repositories.settings as settings_repo

    payload = {"template_settings": {"filename_template": OLD_TEMPLATE}, "source_locations": {"a": {}}}
    monkeypatch.setattr(settings_repo, "load_settings_payload", lambda: payload)
    before = settings_repo.resolution_settings_revision()

    payload["template_settings"] = {"filename_template": NEW_TEMPLATE}
    payload["source_templates"] = {"example": {"": {"filename_template": NEW_TEMPLATE}}}
    assert settings_repo.resolution_settings_revision() == before

    payload["source_locations"] = {"a": {"": "elsewhere"}}
    assert settings_repo.resolution_settings_revision() != before
