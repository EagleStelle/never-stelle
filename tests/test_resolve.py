from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.db.database as database_module
import backend.app.domains.downloads.rename as rename_module
import backend.app.domains.downloads.resolve as resolve_module
import backend.app.domains.downloads.scan as scan_module
import backend.app.domains.downloads.workers.enrichment as enrichment_module
from backend.app.domains.downloads.constants import RESOLVE_JOB_KIND
from backend.app.domains.downloads.serializers import history_to_api
from backend.app.domains.downloads.store import (
    claim_next_enrichment_job,
    history_resolve_flagged_count,
    load_enrichment_jobs,
    load_history,
    load_history_entry,
    save_history_entry_row,
    sync_history_resolve_flags,
)

STORED_TEMPLATE = "{{title}} [{{id}}]"
# Needs a creator the row may not have, which is what makes it a resolve case.
CURRENT_TEMPLATE = "{{username}} - {{title}} [{{id}}]"


def _use_temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
    database_module.initialize_database()


def _pin_template(monkeypatch: pytest.MonkeyPatch, filename_template: str = CURRENT_TEMPLATE) -> None:
    monkeypatch.setattr(
        rename_module,
        "get_effective_template_settings",
        lambda source_url="": {"folder_template": "{{username}}", "filename_template": filename_template},
    )
    monkeypatch.setattr(rename_module, "possible_filename_templates", lambda source_key: {filename_template})
    monkeypatch.setattr(rename_module, "get_effective_title_cleaning", lambda source_url="": {})


def _row(path: Path, **overrides) -> dict:
    row = {
        "engine": "gallerydl",
        "source_url": "https://example.com/p/abc123",
        "source_key": "example",
        "creator": "",
        "title": "Clip",
        "media_id": "abc123",
        "resolved_full_path": str(path),
        "resolved_folder": str(path.parent),
        "resolved_filename": path.name,
        "folder_template": "{{username}}",
        "filename_template": STORED_TEMPLATE,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    row.update(overrides)
    return row


def _seed(tmp_path: Path, task_id: str = "gallerydl:1", **overrides) -> tuple[Path, dict]:
    media_root = tmp_path / "media"
    media_root.mkdir(exist_ok=True)
    path = media_root / "Clip [abc123].mp4"
    path.write_bytes(b"video")
    row = _row(path, **overrides)
    save_history_entry_row(task_id, row)
    return path, row


def _refresh(records: dict[str, dict]) -> list[str]:
    """What the refresh pass does with the worklist, without walking the disk."""
    _plans, needs_resolve = rename_module.plan_history_renames(records)
    sync_history_resolve_flags(needs_resolve)
    return needs_resolve


def _probe_recorder(monkeypatch: pytest.MonkeyPatch, answers: dict[str, dict[str, str]]):
    calls: list[tuple[str, bool]] = []

    def probe(url: str, *, with_cookies: bool = False) -> dict[str, str]:
        calls.append((url, with_cookies))
        return dict(answers.get(url, {})) if not with_cookies else dict(answers.get(f"cookies:{url}", {}))

    monkeypatch.setattr(resolve_module, "_scan_probe_metadata", probe)
    monkeypatch.setattr(resolve_module, "load_learned_formats", dict)
    monkeypatch.setattr(resolve_module, "get_effective_fields", lambda source_url="": {"username": ["uploader"]})
    return calls


def test_refresh_flags_a_row_the_template_cannot_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)

    assert _refresh(load_history()["entries"]) == ["gallerydl:1"]
    assert history_resolve_flagged_count() == 1
    assert load_history_entry("gallerydl:1")["needs_resolve"] is True
    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["can_resolve"] is True


def test_refresh_clears_the_flag_once_the_row_can_be_named(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    assert history_resolve_flagged_count() == 1

    # The creator arrives from somewhere else; the template is satisfiable now.
    entry = load_history_entry("gallerydl:1")
    entry["creator"] = "Creator"
    save_history_entry_row("gallerydl:1", entry)
    _refresh(load_history()["entries"])

    assert history_resolve_flagged_count() == 0
    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["can_resolve"] is False


def test_a_satisfiable_row_is_never_flagged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, creator="Creator")

    assert _refresh(load_history()["entries"]) == []
    assert history_resolve_flagged_count() == 0


def test_the_scan_persists_the_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    media_root = tmp_path / "media"
    _seed(tmp_path)
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", dict)

    result = scan_module.scan_media_library([media_root])

    assert result["needs_resolve"] == 1
    assert history_resolve_flagged_count() == 1


def test_resolve_queues_nothing_when_nothing_is_flagged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, creator="Creator")
    calls = _probe_recorder(monkeypatch, {})

    assert resolve_module.start_resolve() == 0
    assert load_enrichment_jobs() == []
    assert calls == []


def test_resolve_queues_only_the_flagged_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])

    assert resolve_module.start_resolve() == 1
    assert [job["id"] for job in load_enrichment_jobs()] == ["resolve:gallerydl:1"]


def test_resolve_everything_queues_every_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)

    assert resolve_module.start_resolve("all") == 2
    jobs = load_enrichment_jobs()
    assert {job["id"] for job in jobs} == {"resolve:gallerydl:1", "resolve:gallerydl:2"}
    # Everything is the deliberate full re-probe, so satisfied rows are probed too.
    assert all(job["payload"]["force"] for job in jobs)


def test_resolve_one_row_queues_only_that_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)

    assert resolve_module.start_resolve(task_ids=["gallerydl:2"]) == 1
    assert [job["id"] for job in load_enrichment_jobs()] == ["resolve:gallerydl:2"]


def test_resolve_fills_the_missing_token_and_renames_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    path, _row_payload = _seed(tmp_path)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True

    entry = load_history_entry("gallerydl:1")
    assert entry["creator"] == "Creator"
    assert entry["needs_resolve"] is False
    assert entry["filename_template"] == CURRENT_TEMPLATE
    renamed = path.with_name("Creator - Clip [abc123].mp4")
    assert renamed.is_file()
    assert not path.exists()
    assert entry["resolved_full_path"] == str(renamed)


def test_resolve_rechecks_at_probe_time_and_never_probes_a_satisfied_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    # A download or refresh fills the creator after the flag was written.
    entry = load_history_entry("gallerydl:1")
    entry["creator"] = "Creator"
    save_history_entry_row("gallerydl:1", entry)
    calls = _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Other"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is False
    assert calls == []
    assert history_resolve_flagged_count() == 0


def test_resolve_probes_anonymously_before_using_cookies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    url = "https://example.com/p/abc123"
    calls = _probe_recorder(monkeypatch, {f"cookies:{url}": {"uploader": "Creator"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True
    assert calls == [(url, False), (url, True)]


def test_resolve_bounds_the_probes_per_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    calls = _probe_recorder(monkeypatch, {})
    monkeypatch.setattr(
        resolve_module,
        "reconstruct_url_candidates",
        lambda *args, **kwargs: [f"https://example.com/alt/{index}" for index in range(5)],
    )

    with pytest.raises(LookupError):
        resolve_module.resolve_history_entry("gallerydl:1")

    # Two candidates, each probed anonymously then with cookies.
    assert len({url for url, _ in calls}) == resolve_module._MAX_PROBE_CANDIDATES


def test_a_link_that_never_answers_stops_being_probed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    calls = _probe_recorder(monkeypatch, {})

    resolve_module.start_resolve()
    for _ in range(enrichment_module._MAX_ATTEMPTS):
        job = claim_next_enrichment_job()
        assert job is not None
        enrichment_module._process_enrichment_job(job)
    assert [job["status"] for job in load_enrichment_jobs()] == ["failed"]

    probes_so_far = len(calls)
    # A later run must not queue the dead link again.
    assert resolve_module.start_resolve() == 0
    assert claim_next_enrichment_job() is None
    assert len(calls) == probes_so_far


def test_the_worker_routes_a_resolve_job_to_the_resolver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    resolve_module.start_resolve()
    job = claim_next_enrichment_job()
    assert job is not None and job["kind"] == RESOLVE_JOB_KIND
    enrichment_module._process_enrichment_job(job)

    assert load_history_entry("gallerydl:1")["creator"] == "Creator"
    assert load_enrichment_jobs() == []


def test_a_probed_token_with_no_column_survives_into_the_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch, "{{title}} [{{id}}] {{series}}")
    path, _row_payload = _seed(tmp_path)
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"series": "Season 1"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True

    entry = load_history_entry("gallerydl:1")
    assert entry["resolved_tokens"] == {"series": "Season 1"}
    assert path.with_name("Clip [abc123] Season 1.mp4").is_file()


def test_resolve_scope_counts_report_both_choices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    _refresh(load_history()["entries"])

    assert resolve_module.resolve_scope_counts() == {"flagged": 1, "total": 2}
