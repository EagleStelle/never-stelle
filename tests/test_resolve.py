from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.domains.downloads.rename as rename_module
import backend.app.domains.downloads.resolve as resolve_module
import backend.app.domains.downloads.scan as scan_module
import backend.app.domains.downloads.workers.enrichment as enrichment_module
from backend.app.domains.downloads.constants import RESOLVE_JOB_KIND
from backend.app.domains.downloads.serializers import history_to_api, library_activity
from backend.app.domains.downloads.store import (
    claim_next_enrichment_job,
    enqueue_enrichment_job,
    history_resolve_flagged_ids,
    load_enrichment_jobs,
    load_history,
    load_history_entry,
    remove_history_record,
    save_history_entry_row,
    sync_history_resolve_flags,
)
from tests.support import use_temp_db

@pytest.fixture(autouse=True)
def _fresh_pass_counts():
    resolve_module._passes.clear()


STORED_TEMPLATE = "{{title}} [{{id}}]"
# Needs a creator the row may not have, which is what makes it a resolve case.
CURRENT_TEMPLATE = "{{username}} - {{title}} [{{id}}]"


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

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", probe)
    monkeypatch.setattr(resolve_module, "load_learned_formats", dict)
    monkeypatch.setattr(resolve_module, "get_effective_fields", lambda source_url="": {"username": ["uploader"]})
    return calls


def test_refresh_flags_a_row_the_template_cannot_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)

    assert _refresh(load_history()["entries"]) == ["gallerydl:1"]
    assert len(history_resolve_flagged_ids()) == 1
    assert load_history_entry("gallerydl:1")["needs_resolve"] is True
    # The button is offered on every completed row; the flag only decides the default scope.
    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["can_resolve"] is True


def test_refresh_clears_the_flag_once_the_row_can_be_named(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    assert len(history_resolve_flagged_ids()) == 1

    # The creator arrives from somewhere else; the template is satisfiable now.
    entry = load_history_entry("gallerydl:1")
    entry["creator"] = "Creator"
    save_history_entry_row("gallerydl:1", entry)
    _refresh(load_history()["entries"])

    assert len(history_resolve_flagged_ids()) == 0
    # Still offered per row: a satisfied row is re-probed only if the user asks for it.
    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["can_resolve"] is True


def test_a_satisfiable_row_is_never_flagged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, creator="Creator")

    assert _refresh(load_history()["entries"]) == []
    assert len(history_resolve_flagged_ids()) == 0


def test_the_scan_persists_the_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    media_root = tmp_path / "media"
    _seed(tmp_path)
    monkeypatch.setattr(scan_module, "load_task_store", lambda: {"tasks": {}})
    monkeypatch.setattr(scan_module, "load_learned_formats", dict)

    result = scan_module.scan_media_library([media_root])

    assert result["needs_resolve"] == 1
    assert len(history_resolve_flagged_ids()) == 1


def test_resolve_queues_nothing_when_nothing_is_flagged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, creator="Creator")
    calls = _probe_recorder(monkeypatch, {})

    assert resolve_module.start_resolve()["queued"] == 0
    assert load_enrichment_jobs() == []
    assert calls == []


def test_resolve_queues_only_the_flagged_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])

    assert resolve_module.start_resolve()["queued"] == 1
    assert [job["id"] for job in load_enrichment_jobs()] == ["resolve:gallerydl:1"]


def test_resolve_everything_queues_every_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)

    assert resolve_module.start_resolve("all")["queued"] == 2
    jobs = load_enrichment_jobs()
    assert {job["id"] for job in jobs} == {"resolve:gallerydl:1", "resolve:gallerydl:2"}
    # Everything is the deliberate full re-probe, so satisfied rows are probed too.
    assert all(job["payload"]["force"] for job in jobs)


def test_resolve_one_row_queues_only_that_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)

    assert resolve_module.start_resolve(task_ids=["gallerydl:2"])["queued"] == 1
    jobs = load_enrichment_jobs()
    assert [job["id"] for job in jobs] == ["resolve:gallerydl:2"]
    # Clicking one row is as deliberate as asking for the library, so it forces.
    assert jobs[0]["payload"]["force"] is True


def test_resolve_fills_the_missing_token_and_renames_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
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
    use_temp_db(tmp_path, monkeypatch)
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
    assert len(history_resolve_flagged_ids()) == 0


def test_resolve_probes_anonymously_before_using_cookies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    url = "https://example.com/p/abc123"
    calls = _probe_recorder(monkeypatch, {f"cookies:{url}": {"uploader": "Creator"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True
    assert calls == [(url, False), (url, True)]


def test_resolve_bounds_the_probes_per_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
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
    use_temp_db(tmp_path, monkeypatch)
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
    assert resolve_module.start_resolve()["queued"] == 0
    assert claim_next_enrichment_job() is None
    assert len(calls) == probes_so_far


def _spend_the_retries(task_id: str = "gallerydl:1") -> None:
    """Run the flagged pass until its job has spent every attempt and is marked failed."""
    resolve_module.start_resolve()
    for _ in range(enrichment_module._MAX_ATTEMPTS):
        job = claim_next_enrichment_job()
        assert job is not None
        enrichment_module._process_enrichment_job(job)
    assert [(job["id"], job["status"]) for job in load_enrichment_jobs()] == [(f"resolve:{task_id}", "failed")]


def test_a_spent_row_stops_being_offered_by_the_flagged_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _spend_the_retries()

    # The flag stays; the count follows the pass, which would queue nothing.
    assert len(history_resolve_flagged_ids()) == 1
    assert resolve_module.resolve_scope_counts() == {"flagged": 0, "total": 1}


def test_forcing_one_row_hands_back_its_spent_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _spend_the_retries()

    assert resolve_module.start_resolve(task_ids=["gallerydl:1"])["queued"] == 1
    jobs = load_enrichment_jobs()
    # A fresh budget, not a job that fails again on its first claim.
    assert [(job["status"], job["attempts"]) for job in jobs] == [("pending", 0)]

    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})
    job = claim_next_enrichment_job()
    assert job is not None
    enrichment_module._process_enrichment_job(job)

    assert load_history_entry("gallerydl:1")["creator"] == "Creator"
    assert len(history_resolve_flagged_ids()) == 0
    assert load_enrichment_jobs() == []


def test_resolving_everything_hands_back_spent_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _spend_the_retries()

    assert resolve_module.start_resolve("all")["queued"] == 1
    assert [(job["status"], job["attempts"]) for job in load_enrichment_jobs()] == [("pending", 0)]


def test_requeueing_an_unspent_job_keeps_the_attempts_it_has_used(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})

    resolve_module.start_resolve()
    job = claim_next_enrichment_job()
    assert job is not None
    enrichment_module._retry_job(job, "boom")

    # Resetting an in-flight job would let repeated clicks retry a dead link forever.
    assert resolve_module.start_resolve("all")["queued"] == 1
    assert [(job["status"], job["attempts"]) for job in load_enrichment_jobs()] == [("pending", 1)]


def test_a_crashed_run_is_handed_back_to_the_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})

    resolve_module.start_resolve()
    assert claim_next_enrichment_job() is not None

    # The process that claimed it is gone; nothing else would ever claim it again.
    monkeypatch.setattr(enrichment_module, "_recovered", False)
    monkeypatch.setattr(enrichment_module, "pending_enrichment_job_count", lambda: 0)
    enrichment_module.ensure_enrichment_worker()

    assert [(job["status"], job["attempts"]) for job in load_enrichment_jobs()] == [("pending", 1)]


def test_the_worker_routes_a_resolve_job_to_the_resolver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
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
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch, "{{title}} [{{id}}] {{series}}")
    path, _row_payload = _seed(tmp_path)
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"series": "Season 1"}})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True

    entry = load_history_entry("gallerydl:1")
    assert entry["resolved_tokens"] == {"series": "Season 1"}
    assert path.with_name("Clip [abc123] Season 1.mp4").is_file()


def test_resolve_scope_counts_report_both_choices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2", creator="Creator")
    _refresh(load_history()["entries"])

    assert resolve_module.resolve_scope_counts() == {"flagged": 1, "total": 2}


def test_a_spent_row_says_so_on_its_own_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})

    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["resolve_failed"] is False
    _spend_the_retries()
    # It drops out of the flagged count, so the row itself must carry the reason.
    assert history_to_api("gallerydl:1", load_history_entry("gallerydl:1"))["resolve_failed"] is True


def test_requeueing_a_spent_completion_job_hands_back_its_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    enqueue_enrichment_job("completion:gallerydl:1", "completion", {"task_id": "gallerydl:1"})
    for _ in range(enrichment_module._MAX_ATTEMPTS):
        job = claim_next_enrichment_job()
        assert job is not None
        enrichment_module.retry_enrichment_job(str(job["id"]), "boom", max_attempts=enrichment_module._MAX_ATTEMPTS)
    assert [job["status"] for job in load_enrichment_jobs()] == ["failed"]

    enqueue_enrichment_job("completion:gallerydl:1", "completion", {"task_id": "gallerydl:1"})

    # Without the reset it came back at its ceiling and failed on first claim.
    assert [(job["status"], job["attempts"]) for job in load_enrichment_jobs()] == [("pending", 0)]


def test_deleting_a_history_row_takes_its_jobs_with_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _spend_the_retries()

    remove_history_record("gallerydl:1")

    # A spent job outliving its row made the id unqueueable if it came back.
    assert load_enrichment_jobs() == []


def _configured(monkeypatch: pytest.MonkeyPatch, *, slug: dict | None = None, scraped: dict | None = None):
    import backend.app.domains.downloads.enrich as enrich_module

    monkeypatch.setattr(enrich_module, "resolve_slug_tokens", lambda *a, **k: dict(slug or {}))
    monkeypatch.setattr(enrich_module, "resolve_scraped_tokens", lambda *a, **k: dict(scraped or {}))


def test_a_slug_token_resolves_a_row_the_probe_cannot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    path, _row_payload = _seed(tmp_path)
    _refresh(load_history()["entries"])
    # The source answers, but carries no username: the case that used to be a dead end.
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"title": "Clip"}})
    _configured(monkeypatch, slug={"username": "SlugCreator"})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True

    assert load_history_entry("gallerydl:1")["creator"] == "SlugCreator"
    assert path.with_name("SlugCreator - Clip [abc123].mp4").is_file()


def test_a_scraper_rule_outranks_the_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "FromProbe"}})
    _configured(monkeypatch, slug={"username": "FromSlug"}, scraped={"username": "FromScraper"})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True

    assert load_history_entry("gallerydl:1")["creator"] == "FromScraper"


def test_a_source_that_answers_nothing_still_resolves_from_its_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _configured(monkeypatch, scraped={"username": "FromScraper"})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True
    assert load_history_entry("gallerydl:1")["creator"] == "FromScraper"


def test_nothing_answering_and_no_rules_is_still_a_dead_link(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _configured(monkeypatch)

    with pytest.raises(LookupError):
        resolve_module.resolve_history_entry("gallerydl:1")


def test_activity_reports_queued_resolves_until_they_finish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    assert resolve_module.resolve_in_progress() == 0
    resolve_module.start_resolve()
    # Queued but not run: the client that reloads still has to see the pass.
    assert resolve_module.resolve_in_progress() == 1

    job = claim_next_enrichment_job()
    assert job is not None
    assert resolve_module.resolve_in_progress() == 1
    enrichment_module._process_enrichment_job(job)

    assert resolve_module.resolve_in_progress() == 0


def _report(pass_id: int) -> dict[str, int]:
    return resolve_module.resolve_pass_reports()[str(pass_id)]


def _drain(limit: int = 8) -> None:
    for _ in range(limit):
        job = claim_next_enrichment_job()
        if job is None:
            return
        enrichment_module._process_enrichment_job(job)


def test_the_pass_report_counts_a_filled_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    started = resolve_module.start_resolve()
    _drain(1)

    assert _report(started["pass_id"]) == {"queued": 1, "resolved": 1, "skipped": 0, "failed": 0}
    # The count lands before the job row clears, so a client seeing 0 running reads a settled pass.
    assert resolve_module.resolve_in_progress() == 0


def test_a_row_that_no_longer_needs_anything_counts_as_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    started = resolve_module.start_resolve()
    entry = load_history_entry("gallerydl:1")
    entry["creator"] = "Creator"
    save_history_entry_row("gallerydl:1", entry)
    _probe_recorder(monkeypatch, {})

    _drain(1)

    assert _report(started["pass_id"]) == {"queued": 1, "resolved": 0, "skipped": 1, "failed": 0}


def test_a_dead_link_counts_once_its_retries_are_spent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})

    started = resolve_module.start_resolve()
    _drain(1)
    # A retried attempt is not a result yet: the row is back on the queue.
    assert _report(started["pass_id"])["failed"] == 0

    _drain(enrichment_module._MAX_ATTEMPTS - 1)

    assert _report(started["pass_id"]) == {"queued": 1, "resolved": 0, "skipped": 0, "failed": 1}


def test_a_partly_filled_row_is_not_reported_as_resolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    # Wants a series the filename never carried, and only the creator ever comes back.
    _pin_template(monkeypatch, "{{username}} - {{title}} [{{id}}] {{series}}")
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    started = resolve_module.start_resolve()
    _drain(enrichment_module._MAX_ATTEMPTS)

    entry = load_history_entry("gallerydl:1")
    # The creator is kept, but the row still cannot be named, so it stays flagged.
    assert entry["creator"] == "Creator"
    assert entry["needs_resolve"] is True
    assert _report(started["pass_id"]) == {"queued": 1, "resolved": 0, "skipped": 0, "failed": 1}


def test_each_click_reports_its_own_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path, task_id="gallerydl:1")
    _seed(tmp_path, task_id="gallerydl:2")
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    first = resolve_module.start_resolve(task_ids=["gallerydl:1"])
    second = resolve_module.start_resolve(task_ids=["gallerydl:2"])
    _drain(2)

    # A click landing mid-pass used to be told the running total of both.
    assert _report(first["pass_id"]) == {"queued": 1, "resolved": 1, "skipped": 0, "failed": 0}
    assert _report(second["pass_id"]) == {"queued": 1, "resolved": 1, "skipped": 0, "failed": 0}


def test_a_row_already_running_is_not_queued_twice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    resolve_module.start_resolve(task_ids=["gallerydl:1"])
    job = claim_next_enrichment_job()
    assert job is not None

    # Clicking the row again while its probe is in flight must not re-arm the job: the
    # run finishing would delete the requeue, and the pass would wait on a row nobody runs.
    again = resolve_module.start_resolve(task_ids=["gallerydl:1"])
    assert again == {"queued": 0, "pass_id": 0}
    assert [row["status"] for row in load_enrichment_jobs()] == ["running"]

    enrichment_module._process_enrichment_job(job)
    assert load_enrichment_jobs() == []


def test_activity_carries_the_pass_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)

    started = resolve_module.start_resolve(task_ids=["gallerydl:1"])

    assert library_activity()["resolve_passes"][str(started["pass_id"])]["queued"] == 1


def test_a_spent_resolve_is_not_reported_as_still_running(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {})
    _spend_the_retries()

    # Failed rows stay in the table; counting them would spin the UI forever.
    assert resolve_module.resolve_in_progress() == 0


def test_resolve_holds_the_history_lock_while_it_rewrites_the_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    _probe_recorder(monkeypatch, {"https://example.com/p/abc123": {"uploader": "Creator"}})

    held: list[bool] = []
    original = resolve_module.save_history_entry_row

    def spy(task_id: str, entry: dict) -> None:
        held.append(scan_module.scan_in_progress())
        original(task_id, entry)

    monkeypatch.setattr(resolve_module, "save_history_entry_row", spy)

    assert resolve_module.resolve_history_entry("gallerydl:1") is True
    # A scan planning from a snapshot would otherwise revert this write.
    assert held == [True]
    assert scan_module.scan_in_progress() is False


def test_the_probe_runs_outside_the_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    _pin_template(monkeypatch)
    _seed(tmp_path)
    _refresh(load_history()["entries"])
    seen: list[bool] = []

    def probe(url: str, *, with_cookies: bool = False) -> dict[str, str]:
        seen.append(scan_module.scan_in_progress())
        return {"uploader": "Creator"}

    monkeypatch.setattr(scan_module, "_scan_probe_metadata", probe)
    monkeypatch.setattr(resolve_module, "load_learned_formats", dict)
    monkeypatch.setattr(resolve_module, "get_effective_fields", lambda source_url="": {"username": ["uploader"]})

    assert resolve_module.resolve_history_entry("gallerydl:1") is True
    # Holding the lock across a network round-trip would stall every refresh behind it.
    assert seen and not any(seen)


def test_the_enrichment_worker_stands_down_during_a_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setattr(enrichment_module, "active_download_task_count", lambda: 0)

    assert enrichment_module._library_busy() is False
    with scan_module.history_write_lock():
        assert enrichment_module._library_busy() is True
    assert enrichment_module._library_busy() is False
