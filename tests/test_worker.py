from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.app.domains.downloads.workers.completion.sidecars as sidecars_module
import backend.app.domains.downloads.workers.execution as worker_module
import backend.app.domains.downloads.workers.runner as runner_module
from backend.app.domains.downloads import volatile
from backend.app.domains.downloads.engines.engine import Engine
from backend.app.domains.downloads.engines.ytdlp import (
    YTDLP_NICKNAME_FIELD,
    build_ytdlp_command,
)
from backend.app.domains.downloads.engines.ytdlp import read_creator_sidecar as _read_creator_sidecar
from tests.support import engine_by_name


def test_read_creator_sidecar_returns_last_non_empty_line(tmp_path: Path):
    sidecar = tmp_path / "creator.txt"
    sidecar.write_text("First Creator\n\nSecond Creator\n", encoding="utf-8")
    assert _read_creator_sidecar(str(sidecar)) == "Second Creator"


def test_read_creator_sidecar_treats_unknown_as_empty(tmp_path: Path):
    sidecar = tmp_path / "creator.txt"
    sidecar.write_text("Unknown\n", encoding="utf-8")
    assert _read_creator_sidecar(str(sidecar)) == ""


def test_read_creator_sidecar_handles_empty_and_missing(tmp_path: Path):
    empty = tmp_path / "empty.txt"
    empty.write_text("", encoding="utf-8")
    assert _read_creator_sidecar(str(empty)) == ""
    assert _read_creator_sidecar(str(tmp_path / "missing.txt")) == ""


def test_build_ytdlp_command_adds_creator_sidecar_print():
    cmd = build_ytdlp_command(
        "https://youtu.be/abc",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        creator_sidecar="/tmp/creator.txt",
    )
    assert "--print-to-file" in cmd
    idx = cmd.index("--print-to-file")
    assert cmd[idx + 1] == f"after_move:{YTDLP_NICKNAME_FIELD}"
    assert cmd[idx + 2] == "/tmp/creator.txt"
    # Output template and source URL stay at the tail.
    assert cmd[-2:] == ["/media/out.%(ext)s", "https://youtu.be/abc"]


def test_build_ytdlp_command_creator_sidecar_uses_display_name_field_for_non_youtube():
    # Consolidated: the sidecar records the display name (nickname field) everywhere.
    cmd = build_ytdlp_command(
        "https://x.com/DemoVT/status/2000000000000000001",
        "/usr/bin/ffmpeg",
        "/media/out.%(ext)s",
        creator_sidecar="/tmp/creator.txt",
    )

    idx = cmd.index("--print-to-file")
    assert cmd[idx + 1] == f"after_move:{YTDLP_NICKNAME_FIELD}"
    assert cmd[idx + 2] == "/tmp/creator.txt"


def test_build_ytdlp_command_omits_print_without_sidecar():
    cmd = build_ytdlp_command("https://youtu.be/abc", "/usr/bin/ffmpeg", "/media/out.%(ext)s")
    assert "--print-to-file" not in cmd


def _stub_worker_cookie_rotation(monkeypatch, worker_module, paths=("/tmp/cookies-jar1.txt",), *, target=""):
    from backend.app.domains.access.pool import CookieLease

    leases = [
        CookieLease(cookie_id=f"jar-{index}", source_key="youtube", path=path, filename=f"jar{index}.txt")
        for index, path in enumerate(paths, start=1)
    ]

    def fake_rotation(source_key, **kwargs):
        yield from leases

    _stub_access(monkeypatch, fake_rotation, target=target)
    return leases


def _stub_access(monkeypatch, rotation, *, target=""):
    import backend.app.domains.access.rotation as access_module
    import backend.app.domains.downloads.workers.execution as worker_module

    monkeypatch.setattr(access_module, "has_cookies_for_source", lambda source_key: True)
    monkeypatch.setattr(access_module, "cookie_rotation", rotation)
    monkeypatch.setattr(access_module, "_impersonation_families", lambda: (target,) if target else ())
    monkeypatch.setattr(worker_module, "impersonation_target", lambda: target)


def _run_attempts(worker_module, **options):
    from backend.app.domains.downloads.engines.engine import YtdlpEngine

    return worker_module._run_engine_attempts(
        YtdlpEngine(),
        "task-123",
        "https://www.youtube.com/watch?v=abc123age",
        "/tmp",
        "/usr/bin/ffmpeg",
        "/tmp/%(title)s.%(ext)s",
        "youtube",
        "",
        "",
        0,
        **options,
    )


def test_run_engine_attempts_tries_anonymous_first_then_a_leased_cookie(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts_seen = []

    def fake_run_engine(engine, task_id, cmd, **options):
        with_cookies = "--cookies" in cmd
        attempts_seen.append(with_cookies)
        if not with_cookies:
            # Simulate anonymous attempt failure (e.g. age restricted)
            return 1, "", []
        assert cmd[cmd.index("--cookies") + 1] == "/tmp/cookies-jar1.txt"
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    (lease,) = _stub_worker_cookie_rotation(monkeypatch, worker_module)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "update_task", lambda task_id, **kwargs: {})

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert attempts_seen == [False, True]
    assert lease.banned is False


def test_run_engine_attempts_spends_no_cookie_when_anonymous_succeeds(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    leased = []

    def fake_run_engine(engine, task_id, cmd, **options):
        assert "--cookies" not in cmd
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    def fail_rotation(source_key, **kwargs):
        leased.append(source_key)
        raise AssertionError("no cookie should be leased after an anonymous success")

    _stub_access(monkeypatch, fail_rotation)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert leased == []


def test_run_engine_attempts_retries_a_site_extractor_miss_generically_before_any_cookie(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts: list[bool] = []

    def fake_run_engine(engine, task_id, cmd, **options):
        assert "--cookies" not in cmd
        generic = cmd[-2:] == ["--use-extractors", "generic"]
        attempts.append(generic)
        return (0, "/tmp/out.mp4", ["/tmp/out.mp4"]) if generic else (1, "", [])

    def fail_rotation(source_key, **kwargs):
        raise AssertionError("no cookie should be leased when the generic retry gets the media")

    _stub_access(monkeypatch, fail_rotation)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(
        worker_module, "_task_log_tail", lambda task_id: "ERROR: [Example] abc123: No video formats found!"
    )

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert attempts == [False, True]


def test_run_engine_attempts_retries_every_cookie_until_one_works(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    used: list[str] = []

    def fake_run_engine(engine, task_id, cmd, **options):
        if "--cookies" not in cmd:
            return 1, "", []
        cookies_file = cmd[cmd.index("--cookies") + 1]
        used.append(cookies_file)
        # Only the third jar in the list still has a working session.
        if cookies_file == "/tmp/jar3.txt":
            return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]
        return 1, "", []

    leases = _stub_worker_cookie_rotation(
        monkeypatch, worker_module, ["/tmp/jar1.txt", "/tmp/jar2.txt", "/tmp/jar3.txt"]
    )
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "update_task", lambda task_id, **kwargs: {})
    monkeypatch.setattr(
        worker_module, "_task_log_tail", lambda task_id: "ERROR: HTTP Error 429: Too Many Requests"
    )

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert used == ["/tmp/jar1.txt", "/tmp/jar2.txt", "/tmp/jar3.txt"]
    # The two that failed on a rate limit rest; the one that worked does not.
    assert [lease.banned for lease in leases] == [True, True, False]


def test_run_engine_attempts_retries_behind_a_fingerprint_after_a_wall(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts: list[tuple[str, bool, str]] = []
    tails: list[str] = []

    def fake_run_engine(engine, task_id, cmd, *, env=None, **options):
        impersonate = cmd[cmd.index("--impersonate") + 1] if "--impersonate" in cmd else ""
        attempts.append((impersonate, "--cookies" in cmd, (env or {}).get("PYTHONPATH", "")))
        if impersonate and "--cookies" in cmd:
            return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]
        tails.append("ERROR: Got HTTP Error 403 caused by Cloudflare anti-bot challenge")
        return 1, "", []

    leases = _stub_worker_cookie_rotation(monkeypatch, worker_module, ["/tmp/jar1.txt"], target="chrome")
    monkeypatch.setenv("NEVER_STELLE_IMPERSONATE_PATH", "/opt/impersonate")
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "_task_log_tail", lambda task_id: tails[-1])

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    # Only impersonated attempts load the fingerprint backend.
    assert attempts == [
        ("", False, ""),
        ("chrome", False, "/opt/impersonate"),
        ("chrome", True, "/opt/impersonate"),
    ]
    # A 403 wall blocks every jar alike, so it never rests the one that hit it.
    assert leases[0].banned is False


def test_run_engine_attempts_fingerprints_only_the_cookie_without_a_wall(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts: list[bool] = []

    def fake_run_engine(engine, task_id, cmd, **options):
        attempts.append("--impersonate" in cmd)
        return (0, "/tmp/out.mp4", ["/tmp/out.mp4"]) if "--cookies" in cmd else (1, "", [])

    _stub_worker_cookie_rotation(monkeypatch, worker_module, target="chrome")
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "_task_log_tail", lambda task_id: "ERROR: Unsupported URL")

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert attempts == [False, True]


def test_run_engine_attempts_rests_a_cookie_that_came_back_rate_limited(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    def fake_run_engine(engine, task_id, cmd, **options):
        return 1, "", []

    (lease,) = _stub_worker_cookie_rotation(monkeypatch, worker_module)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "update_task", lambda task_id, **kwargs: {})
    monkeypatch.setattr(
        worker_module, "_task_log_tail", lambda task_id: "ERROR: HTTP Error 429: Too Many Requests"
    )

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 1
    assert lease.banned is True


def _stub_busy_jars(monkeypatch, worker_module, ready_in, tail="ERROR: Unsupported URL"):
    first_waits = []

    def busy_rotation(source_key, **kwargs):
        first_waits.append(kwargs.get("first_wait"))
        yield from ()

    _stub_access(monkeypatch, busy_rotation)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", lambda *args, **kwargs: (1, "", []))
    monkeypatch.setattr(worker_module, "_task_log_tail", lambda task_id: tail)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: ready_in)
    return first_waits


@pytest.mark.parametrize(
    ("tail", "walled"),
    [("ERROR: Unsupported URL", False), ("ERROR: Got HTTP Error 403 caused by Cloudflare anti-bot challenge", True)],
)
def test_run_engine_attempts_defers_the_task_when_every_jar_is_busy(monkeypatch, tail, walled):
    import backend.app.domains.downloads.workers.execution as worker_module
    from backend.app.runtime.processes import TaskDeferred

    first_waits = _stub_busy_jars(monkeypatch, worker_module, ready_in=4.0, tail=tail)

    with pytest.raises(TaskDeferred) as deferred:
        _run_attempts(worker_module)

    assert deferred.value.source_key == "youtube"
    # The resumed cookie stage keeps the fingerprint a wall called for.
    assert deferred.value.walled is walled
    # The first jar is never waited for, so the worker is free at once.
    assert first_waits == [0]


def test_run_engine_attempts_resumes_at_the_cookie_stage(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts: list[tuple[bool, bool]] = []

    def fake_run_engine(engine, task_id, cmd, **options):
        attempts.append(("--impersonate" in cmd, "--cookies" in cmd))
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    _stub_worker_cookie_rotation(monkeypatch, worker_module, target="chrome")
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "append_task_log", lambda task_id, message: None)

    rc, _, _ = _run_attempts(worker_module, resume_walled=True)

    assert rc == 0
    # No anonymous or fingerprint-only rerun; the cookie connects as its browser.
    assert attempts == [(True, True)]


def test_run_engine_attempts_fails_instead_of_deferring_when_a_jar_is_free(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    _stub_busy_jars(monkeypatch, worker_module, ready_in=0.0)

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 1


class _FakeProcess:
    def __init__(self, lines):
        self.stdout = iter(lines)

    def wait(self):
        return 0

    def poll(self):
        return 0

    def kill(self):
        return None


def _stream_engine_progress(
    monkeypatch,
    lines,
    *,
    engine_name: str = "ytdlp",
    keep_audio: bool = False,
    task_id: str = "task",
):
    """Run the streaming loop over ``lines``, returning the row writes it caused."""
    import backend.app.domains.downloads.workers.runner as runner_module
    from tests.support import engine_by_name

    writes: list[dict] = []
    volatile.forget(task_id)
    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *a, **k: _FakeProcess(iter(lines)))
    monkeypatch.setattr(runner_module, "update_task", lambda task_id, **updates: writes.append(updates))
    monkeypatch.setattr(runner_module, "register_process", lambda task_id, process: None)
    monkeypatch.setattr(runner_module, "unregister_process", lambda task_id, process: None)

    rc, dest, paths = runner_module._run_engine_to_task(
        engine_by_name(engine_name),
        task_id,
        [engine_name],
        keep_audio=keep_audio,
    )
    return writes, rc, dest, paths


def _stream_progress(monkeypatch, lines, task_id: str = "task"):
    writes, rc, dest, _ = _stream_engine_progress(monkeypatch, lines, task_id=task_id)
    return writes, rc, dest


def test_a_transfer_moving_only_the_bar_writes_no_rows(monkeypatch):
    # A 10-minute transfer emitting a progress line every 100ms produces no durable
    # state at all: the bar and the log tail are memory until something real changes.
    lines = [f"[download]  {index / 60:5.1f}% of 4.20GiB at 12.00MiB/s\n" for index in range(6000)]

    writes, rc, _ = _stream_progress(monkeypatch, lines, task_id="long")

    assert rc == 0
    assert writes == []
    assert volatile.merge("long", {})["progress_pct"] > 0


def test_the_bar_moves_on_every_reported_percentage(monkeypatch):
    # Memory costs nothing to write, so no reading is skipped for being too close
    # to the last one.
    seen: list[float] = []
    monkeypatch.setattr(
        "backend.app.domains.downloads.workers.runner.record_task_progress",
        lambda task_id, progress_pct: seen.append(progress_pct),
    )
    lines = [f"[download]  {index / 10:5.1f}% of 4.20MiB at 1.00MiB/s\n" for index in range(200)]

    _stream_progress(monkeypatch, lines, task_id="fine")

    assert len(seen) == len(lines)
    assert seen == sorted(seen)


def test_a_new_output_path_writes_immediately(monkeypatch):
    lines = [
        "[download]  10.0% of 4.20GiB at 12.00MiB/s\n",
        "[download] Destination: /media/x/clip [abc].mp4\n",
    ]

    writes, _, dest = _stream_progress(monkeypatch, lines, task_id="path")

    assert dest.endswith("clip [abc].mp4")
    assert [update.get("resolved_filename") for update in writes] == ["clip [abc].mp4"]


def test_gallerydl_audio_sidecar_is_ignored_for_non_audio_tasks(monkeypatch):
    lines = ["/media/x/probe.m4a\n"]

    writes, rc, dest, paths = _stream_engine_progress(monkeypatch, lines, engine_name="gallerydl")

    assert rc == 0
    assert dest == ""
    assert paths == []
    assert not any(update.get("resolved_filename") == "probe.m4a" for update in writes)


def test_gallerydl_audio_output_is_recorded_for_audio_tasks(monkeypatch):
    lines = ["/media/x/probe.m4a\n"]

    writes, rc, dest, paths = _stream_engine_progress(
        monkeypatch,
        lines,
        engine_name="gallerydl",
        keep_audio=True,
    )

    assert rc == 0
    assert dest.endswith("probe.m4a")
    assert [path.replace("\\", "/") for path in paths] == ["/media/x/probe.m4a"]
    assert any(update.get("resolved_filename") == "probe.m4a" for update in writes)


def test_the_log_tail_is_kept_for_the_worker_to_read_back(monkeypatch):
    # Failure reports and path recovery read the tail; it is capped and never a write.
    lines = [f"[download]  {index:5.1f}% of 4.20GiB at 12.00MiB/s\n" for index in range(40)]

    writes, _, _ = _stream_progress(monkeypatch, lines, task_id="tail")

    assert writes == []
    tail = volatile.merge("tail", {})["last_log_lines"]
    assert len(tail) == volatile.LOG_TAIL
    assert tail[-1].startswith("[download]")


def test_ytdlp_command_does_not_request_verbose_output():
    cmd = build_ytdlp_command("https://youtu.be/abc", "/usr/bin", "/out/%(title)s.%(ext)s")

    assert "--verbose" not in cmd
    assert "--newline" in cmd


def _patch_worker_task_store(monkeypatch: pytest.MonkeyPatch, store: dict, update_task):
    def load_task(task_id: str):
        return (store.get("tasks") or {}).get(task_id, {})

    monkeypatch.setattr(worker_module, "load_task", load_task)
    monkeypatch.setattr(worker_module, "update_task", update_task)
    monkeypatch.setattr(runner_module, "update_task", update_task)


def test_gallerydl_multifile_run_uses_first_image_and_clean_display_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first = tmp_path / "Creator - TikTok photo #1234567890 [1234567890]_1.jpg"
    second = tmp_path / "Creator - TikTok photo #1234567890 [1234567890]_2.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    source_url = "https://www.tiktok.com/@Creator/photo/1234567890"
    task_id = "gallerydl:test"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "tiktok",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        stdout = iter([f"{second}\n", f"{first}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    first_clean = tmp_path / "Creator - Unknown [1234567890]_1.jpg"
    second_clean = tmp_path / "Creator - Unknown [1234567890]_2.jpg"
    assert completed["status"] == "completed"
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert not first.exists()
    assert not second.exists()
    assert completed["resolved_full_path"] == str(first_clean)
    assert completed["resolved_filename"] == "Creator - Unknown [1234567890].jpg"
    assert completed["title"] == ""
    assert saved[task_id]["resolved_full_path"] == str(first_clean)
    assert saved[task_id]["resolved_filename"] == "Creator - Unknown [1234567890].jpg"


@pytest.mark.parametrize(
    ("answer", "repair", "filename"),
    [
        ({"channel": "ChannelHandle", "title": "Nice clip"}, False, "ChannelHandle - Nice clip [abc123].mp4"),
        # No engine answered, so the lookup is tried once more later; an empty answer would be final.
        (None, True, "Unknown - Unknown [abc123].mp4"),
    ],
)
def test_gallerydl_sparse_single_output_probes_inline_and_repairs_only_an_unanswered_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, answer, repair, filename
):
    raw_video = tmp_path / "[abc123].mp4"
    raw_video.write_bytes(b"video")
    source_url = "https://www.example.test/watch/abc123"
    task_id = "gallerydl:sparse-template"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    queued: list[dict[str, object]] = []
    probed: list[str] = []

    class FakeProcess:
        stdout = iter([f"{raw_video}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "get_effective_fields", lambda url: {"username": ["channel"]})

    def probe(url: str, key: str) -> dict[str, str] | None:
        probed.append(url)
        return answer

    monkeypatch.setattr(sidecars_module, "probe_link_metadata", probe)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "learn_field_roles", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))
    monkeypatch.setattr(
        worker_module,
        "enqueue_completion_enrichment",
        lambda *args, **kwargs: queued.append({"args": args, "kwargs": kwargs}),
    )

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert completed["status"] == "completed"
    assert Path(saved[task_id]["resolved_full_path"]).is_file()
    assert saved[task_id]["folder_template"] == store["tasks"][task_id]["folder_template"]
    assert saved[task_id]["filename_template"] == store["tasks"][task_id]["filename_template"]
    assert probed == [source_url]
    assert saved[task_id]["resolved_filename"] == filename
    assert queued[0]["args"] == (task_id,)
    assert queued[0]["kwargs"]["needs_metadata_probe"] is repair
    assert queued[0]["kwargs"]["needs_field_probe"] is False


def test_gallerydl_same_source_assets_share_one_row_and_source_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    first = tmp_path / "Poster - Image [childA]_1.jpg"
    second = tmp_path / "Poster - Image [childB]_2.jpg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    source_url = "https://www.example.test/post/DDemoReel01"
    task_id = "ytdlp:gallery-post"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        if cmd[0] == "yt-dlp":
            return FakeProcess(["ERROR: [Example] DDemoReel01: No video formats found!\n"], 1)
        return FakeProcess([f"{first}\n", f"{second}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    first_clean = tmp_path / "Poster - Image [DDemoReel01]_1.jpg"
    second_clean = tmp_path / "Poster - Image [DDemoReel01]_2.jpg"
    completed = store["tasks"][task_id]
    assert set(saved) == {task_id}
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert not first.exists()
    assert not second.exists()
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert completed["media_id"] == "DDemoReel01"
    assert completed["source_url"] == source_url
    assert completed["resolved_full_path"] == str(first_clean)
    assert completed["resolved_filename"] == "Poster - Image [DDemoReel01].jpg"
    assert saved[task_id]["media_id"] == "DDemoReel01"
    assert saved[task_id]["resolved_filename"] == "Poster - Image [DDemoReel01].jpg"


def test_worker_falls_back_to_gallerydl_after_empty_ytdlp_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "ytdlp:fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        if cmd[0] == "yt-dlp":
            return FakeProcess(["ERROR: [Example] abc123: No video formats found!\n"], 1)
        return FakeProcess([f"{image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert saved[task_id]["engine"] == "gallerydl"


def test_worker_does_not_run_fallback_after_media_and_unsupported_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "@Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:no-duplicate-fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        stdout = iter(
            [
                f"{image}\n",
                "ERROR: [Example] child-video: No video formats found!\n",
            ]
        )

        def wait(self):
            return 1

        def poll(self):
            return 1

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        return FakeProcess()

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_image = tmp_path / "Creator - Image [abc123].jpg"
    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert clean_image.is_file()
    assert not image.exists()
    assert saved[task_id]["resolved_full_path"] == str(clean_image)


@pytest.mark.parametrize(
    ("gallerydl_line", "gallerydl_rc"),
    [
        ("ERROR: Unsupported URL: https://www.example.test/post/abc123", 1),
        # A clean exit that named no file found nothing either.
        ("[ytdl][info] No results for https://www.example.test/post/abc123", 0),
    ],
)
def test_worker_runs_ytdlp_fallback_after_empty_gallerydl_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gallerydl_line: str,
    gallerydl_rc: int,
):
    video = tmp_path / "Creator - Clip [abc123].mp4"
    video.write_bytes(b"video")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:ytdlp-fallback"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[str] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        if cmd[0] == "gallery-dl":
            return FakeProcess([f"{gallerydl_line}\n"], gallerydl_rc)
        return FakeProcess([f"[download] Destination: {video}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert commands == ["gallery-dl", "yt-dlp"]
    assert completed["status"] == "completed"
    assert completed["engine"] == "ytdlp"
    assert saved[task_id]["resolved_full_path"] == str(video)


def test_worker_leads_with_the_engine_that_gets_media_on_the_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    video = tmp_path / "Creator - Clip [abc123].mp4"
    video.write_bytes(b"video")
    source_url = "https://www.example.test/post/abc123"
    store: dict = {"tasks": {}}
    commands: list[str] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd[0])
        if cmd[0] == "gallery-dl":
            return FakeProcess([f"ERROR: Unsupported URL: {source_url}\n"], 1)
        return FakeProcess([f"[download] Destination: {video}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: None)

    for run in range(4):
        task_id = f"gallerydl:learned-{run}"
        store["tasks"][task_id] = {
            "engine": "gallerydl",
            "source_url": source_url,
            "source_key": "example",
            "status": "pending",
            "output_dir": str(tmp_path),
            "resolved_folder": str(tmp_path),
            "folder_template": "",
            "filename_template": "{{username}} - {{title}} [{{id}}]",
        }
        commands.clear()
        worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)
        assert store["tasks"][task_id]["status"] == "completed"

    # Three links taught the route; the fourth skips the engine that never got media there.
    assert commands == ["yt-dlp"]


def test_worker_resumes_a_deferred_task_at_the_cookie_stage_of_the_engine_that_found_the_jar_busy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import backend.app.domains.access.rotation as access_module
    from backend.app.domains.access.pool import CookieLease
    from backend.app.runtime.processes import TaskDeferred

    video = tmp_path / "Creator - Clip [abc123].mp4"
    video.write_bytes(b"video")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:resumed"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[tuple[str, bool]] = []
    jar = {"free": True}
    lease = CookieLease(cookie_id="jar", source_key="example", path=str(tmp_path / "jar.txt"), filename="jar.txt")

    def rotation(source_key, *, first_wait=None):
        # The only jar rests after each use.
        if jar["free"]:
            jar["free"] = False
            yield lease

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        with_cookies = "--cookies" in cmd
        commands.append((cmd[0], with_cookies))
        if cmd[0] == "yt-dlp" and with_cookies:
            return FakeProcess([f"[download] Destination: {video}\n"], 0)
        return FakeProcess(["ERROR: login required\n"], 1)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(access_module, "has_cookies_for_source", lambda source_key: True)
    monkeypatch.setattr(access_module, "cookie_rotation", rotation)
    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0 if jar["free"] else 5.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    with pytest.raises(TaskDeferred) as deferred:
        worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    assert commands == [("gallery-dl", False), ("gallery-dl", True), ("yt-dlp", False)]
    assert deferred.value.engine == "ytdlp"
    assert deferred.value.walled is False
    assert [failure.split()[0] for failure in deferred.value.failures] == ["gallerydl"]

    commands.clear()
    jar["free"] = True
    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False, resume=deferred.value)

    # Everything before yt-dlp's cookie stage already failed, so only that stage runs.
    assert commands == [("yt-dlp", True)]
    assert store["tasks"][task_id]["status"] == "completed"
    assert saved[task_id]["resolved_full_path"] == str(video)


def test_worker_runs_gallerydl_without_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    image = tmp_path / "Creator - Image [abc123]_1.jpg"
    image.write_bytes(b"image")
    source_url = "https://www.example.test/post/abc123"
    task_id = "gallerydl:no-preflight"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[list[str]] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd)
        return FakeProcess([f"{image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    completed = store["tasks"][task_id]
    assert [cmd[0] for cmd in commands] == ["gallery-dl"]
    # No stored template: the worker rebuilds a gallery-dl filename, not a yt-dlp one.
    assert commands[0][commands[0].index("--filename") + 1].endswith(".{extension}")
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert saved[task_id]["engine"] == "gallerydl"


def test_worker_merges_fallback_assets_without_duplicate_videos(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    ytdlp_video = tmp_path / "demo.reelzz - Video by Zed [DEM-oPost04].mp4"
    gallery_video = tmp_path / "demo.reelzz - Video by Zed [DEM-oPost04]_1.mp4"
    gallery_image = tmp_path / "demo.reelzz - None [DEM-oPost04]_2.jpg"
    stale_wrong_video = tmp_path / "Zed" / "Zed - [DEM-oPost04].mp4"
    stale_wrong_video.parent.mkdir()
    for path in (ytdlp_video, gallery_video, gallery_image):
        path.write_bytes(b"media")
    stale_wrong_video.write_bytes(b"duplicate")
    source_url = "https://www.example.test/post/DEM-oPost04"
    task_id = "ytdlp:mixed-post"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    commands: list[list[str]] = []

    class FakeProcess:
        def __init__(self, lines: list[str], rc: int):
            self.stdout = iter(lines)
            self._rc = rc

        def wait(self):
            return self._rc

        def poll(self):
            return self._rc

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd)
        if cmd[0] == "yt-dlp":
            return FakeProcess(
                [
                    f"[download] Destination: {ytdlp_video}\n",
                    "ERROR: [Example] child-image: No video formats found!\n",
                ],
                1,
            )
        return FakeProcess([f"{gallery_video}\n", f"{gallery_image}\n"], 0)

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)

    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_video = tmp_path / "demo.reelzz - Unknown [DEM-oPost04]_1.mp4"
    clean_image = tmp_path / "demo.reelzz - Unknown [DEM-oPost04]_2.jpg"
    completed = store["tasks"][task_id]
    assert [cmd[0] for cmd in commands] == ["gallery-dl"]
    assert "--filter" not in commands[0]
    assert set(saved) == {task_id}
    assert clean_video.is_file()
    assert clean_image.is_file()
    assert not ytdlp_video.exists()
    assert not gallery_video.exists()
    assert not gallery_image.exists()
    assert stale_wrong_video.exists()
    assert completed["status"] == "completed"
    assert completed["engine"] == "gallerydl"
    assert completed["creator"] == "demo.reelzz"
    assert completed["source_url"] == source_url
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_filename"] == "demo.reelzz - Unknown [DEM-oPost04].mp4"


def test_worker_renames_display_creator_to_handle_and_template_folder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    raw_video = tmp_path / "Zed" / "Zed - [DEM-oPost04].mp4"
    raw_video.parent.mkdir()
    raw_video.write_bytes(b"video")
    source_url = "https://www.example.test/post/DEM-oPost04"
    task_id = "ytdlp:display-name"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "instagram",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}

    class FakeProcess:
        stdout = iter([f"[download] Destination: {raw_video}\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_popen(cmd, *args, **kwargs):
        for index, arg in enumerate(cmd):
            if arg != "--print-to-file":
                continue
            template = cmd[index + 1]
            sidecar = Path(cmd[index + 2])
            if "filepath" in template:
                sidecar.write_text(
                    json.dumps(
                        {"filepath": str(raw_video), "id": "DEM-oPost04", "channel": "demo.reelzz", "uploader": "Zed"}
                    )
                    + "\n",
                    encoding="utf-8",
                )
            else:
                sidecar.write_text("Zed\n", encoding="utf-8")
        return FakeProcess()

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "engine_order", lambda url: (engine_by_name("ytdlp"),))
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_formats", lambda samples: False)
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    clean_video = tmp_path / "demo.reelzz" / "demo.reelzz - Unknown [DEM-oPost04].mp4"
    completed = store["tasks"][task_id]
    assert clean_video.is_file()
    assert not raw_video.exists()
    assert completed["status"] == "completed"
    assert completed["creator"] == "demo.reelzz"
    assert completed["resolved_folder"] == str(clean_video.parent)
    assert completed["resolved_full_path"] == str(clean_video)
    assert completed["resolved_filename"] == "demo.reelzz - Unknown [DEM-oPost04].mp4"
    assert saved[task_id]["resolved_full_path"] == str(clean_video)


def test_worker_splits_distinct_media_outputs_and_cleans_each_real_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    first = tmp_path / "demo.reelzz - Video by demo.reelzz [DDemoStry3_].mp4"
    second = tmp_path / "demo.reelzz - [DDemoStry02].mp4"
    third = tmp_path / "demo.reelzz - Video by demo.reelzz [DDemoStry05].mp4"
    for path in (first, second, third):
        path.write_bytes(b"video")
    source_url = "https://www.instagram.com/stories/demo.reelzz/3900000000000000001/"
    task_id = "ytdlp:story"
    store = {
        "tasks": {
            task_id: {
                "engine": "ytdlp",
                "source_url": source_url,
                "source_key": "instagram",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            }
        }
    }
    saved: dict[str, dict] = {}
    dropped_cache_paths: list[Path] = []

    class FakeProcess:
        stdout = iter(
            [
                f"[download] Destination: {first}\n",
                f"[download] Destination: {second}\n",
                f"[download] Destination: {third}\n",
            ]
        )

        def wait(self):
            return 0

        def poll(self):
            return 0

        def kill(self):
            return None

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    monkeypatch.setattr(worker_module, "engine_order", lambda url: (engine_by_name("ytdlp"),))
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    learned: list[tuple[list, set[str]]] = []
    monkeypatch.setattr(
        worker_module, "learn_formats", lambda samples: bool(learned.append((list(samples), set(saved))))
    )
    monkeypatch.setattr(worker_module, "drop_file_cache", lambda paths: dropped_cache_paths.extend(paths))
    monkeypatch.setattr(worker_module, "save_history_entry", lambda task_id, task: saved.update({task_id: dict(task)}))

    worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)

    # Every output teaches the format in one write, only once all of them were saved.
    assert len(learned) == 1
    assert len(learned[0][0]) == 3
    assert learned[0][1] == {task_id, f"{task_id}:DDemoStry02", f"{task_id}:DDemoStry05"}

    first_clean = tmp_path / "demo.reelzz - Unknown [DDemoStry3_].mp4"
    second_clean = tmp_path / "demo.reelzz - Unknown [DDemoStry02].mp4"
    third_clean = tmp_path / "demo.reelzz - Unknown [DDemoStry05].mp4"
    assert first_clean.is_file()
    assert second_clean.is_file()
    assert third_clean.is_file()
    assert not first.exists()
    assert not third.exists()
    assert set(saved) == {task_id, f"{task_id}:DDemoStry02", f"{task_id}:DDemoStry05"}
    assert saved[task_id]["resolved_filename"] == first_clean.name
    assert saved[f"{task_id}:DDemoStry02"]["resolved_filename"] == second_clean.name
    assert saved[f"{task_id}:DDemoStry05"]["resolved_filename"] == third_clean.name
    assert {Path(path).name for path in dropped_cache_paths} == {
        first_clean.name,
        second_clean.name,
        third_clean.name,
    }
    assert saved[task_id]["source_url"] == "https://www.instagram.com/stories/demo.reelzz/DDemoStry3_"
    assert saved[f"{task_id}:DDemoStry02"]["source_url"] == (
        "https://www.instagram.com/stories/demo.reelzz/DDemoStry02"
    )


def test_worker_hands_an_engine_the_item_in_a_learned_format_it_takes(monkeypatch: pytest.MonkeyPatch):
    learned = {
        "example": {
            "templates": ["https://example.test/@{creator}/photo/{id}", "https://example.test/@{creator}/video/{id}"]
        }
    }
    monkeypatch.setattr(worker_module, "load_learned_formats", lambda: learned)
    photo = "https://example.test/@alice/photo/12345678"
    video = "https://example.test/@alice/video/12345678"

    class VideoEngine(Engine):
        def reads(self, url: str) -> bool:
            return "/video/" in url

    assert worker_module._engine_link(VideoEngine(), photo) == video
    assert worker_module._engine_link(VideoEngine(), video) == video
    # A link naming no item, or in no learned format, keeps the link it was given.
    assert worker_module._engine_link(VideoEngine(), "https://example.test/@alice") == "https://example.test/@alice"
    place = "https://example.test/@alice/places/12345678"
    assert worker_module._engine_link(VideoEngine(), place) == place
    assert worker_module._engine_link(Engine(), photo) == photo


def test_run_engine_attempts_spends_no_cookie_on_a_limit_skip(monkeypatch):
    attempts: list[bool] = []

    def fake_run_engine(engine, task_id, cmd, **options):
        attempts.append("--cookies" in cmd)
        assert cmd[cmd.index("--match-filters") + 1] == "duration<=?60"
        return 1, "", []

    _stub_worker_cookie_rotation(monkeypatch, worker_module)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(
        worker_module,
        "_task_log_tail",
        lambda task_id: "[download] File is larger than max-filesize (2000 bytes > 1000 bytes). Aborting.",
    )

    _run_attempts(worker_module, limits={"max_minutes": 1})

    # Skipping an item over its limits is no failure another identity could fix.
    assert attempts == [False]


def test_a_download_over_its_limits_leaves_the_tracker_entry_only_seen(tmp_path: Path, monkeypatch):
    task_id = "gallerydl:over-limit"
    store = {
        "tasks": {
            task_id: {
                "engine": "gallerydl",
                "source_url": "https://www.example.test/post/abc123",
                "source_key": "example",
                "status": "pending",
                "output_dir": str(tmp_path),
                "resolved_folder": str(tmp_path),
                "folder_template": "",
                "filename_template": "{{title}}",
                "limits": {"max_minutes": 1},
            }
        }
    }
    commands: list[list[str]] = []
    removed: list[str] = []
    unlinked: list[str] = []
    learned: list[tuple] = []

    class FakeProcess:
        stdout = iter(["[download] Clip does not pass filter (duration<=?60), skipping ..\n"])

        def wait(self):
            return 0

        def poll(self):
            return 0

    def fake_popen(cmd, *args, **kwargs):
        commands.append(cmd)
        return FakeProcess()

    def fake_update_task(task_id: str, **updates):
        store["tasks"].setdefault(task_id, {}).update(updates)
        return store["tasks"][task_id]

    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    _patch_worker_task_store(monkeypatch, store, fake_update_task)
    monkeypatch.setattr(
        worker_module, "load_task", lambda task_id: volatile.merge(task_id, dict(store["tasks"].get(task_id, {})))
    )
    monkeypatch.setattr(worker_module, "cookie_ready_in", lambda source_key: 0.0)
    monkeypatch.setattr(worker_module, "learn_route", lambda *args, **kwargs: learned.append(args))
    monkeypatch.setattr(worker_module, "remove_task_record", removed.append)
    monkeypatch.setattr(worker_module, "unlink_tracker_downloads", unlinked.extend)

    try:
        worker_module.run_task(task_id, store["tasks"][task_id], mark_running=False)
    finally:
        volatile.forget(task_id)

    # The other engine never runs, the route learns nothing and the task is no failure.
    assert len(commands) == 1
    assert any("duration<=?60" in arg for arg in commands[0] if arg.startswith("downloader.ytdl.cmdline-args="))
    assert (removed, unlinked, learned) == ([task_id], [task_id], [])
    assert store["tasks"][task_id].get("status") != "failed"
