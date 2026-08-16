from __future__ import annotations

from pathlib import Path

from backend.app.domains.downloads import volatile
from backend.app.domains.downloads.ytdlp import YTDLP_NICKNAME_FIELD, build_ytdlp_command
from backend.app.domains.downloads.ytdlp import read_creator_sidecar as _read_creator_sidecar


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
        "https://x.com/DohaVT/status/2073635724684054528",
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


def _stub_worker_cookie_rotation(monkeypatch, worker_module, paths=("/tmp/cookies-jar1.txt",)):
    from backend.app.domains.settings import CookieLease

    leases = [
        CookieLease(cookie_id=f"jar-{index}", source_key="youtube", path=path, filename=f"jar{index}.txt")
        for index, path in enumerate(paths, start=1)
    ]

    def fake_rotation(source_key, **kwargs):
        yield from leases

    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: True)
    monkeypatch.setattr(worker_module, "cookie_rotation", fake_rotation)
    return leases


def _run_attempts(worker_module):
    from backend.app.domains.downloads.engine import YtdlpEngine

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
    )


def test_run_engine_attempts_tries_anonymous_first_then_a_leased_cookie(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    attempts_seen = []

    def fake_run_engine(engine, task_id, cmd, total_items=0, progress=None):
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

    def fake_run_engine(engine, task_id, cmd, total_items=0, progress=None):
        assert "--cookies" not in cmd
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    def fail_rotation(source_key, **kwargs):
        leased.append(source_key)
        raise AssertionError("no cookie should be leased after an anonymous success")

    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: True)
    monkeypatch.setattr(worker_module, "cookie_rotation", fail_rotation)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert leased == []


def test_run_engine_attempts_retries_every_cookie_until_one_works(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    used: list[str] = []

    def fake_run_engine(engine, task_id, cmd, total_items=0, progress=None):
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


def test_run_engine_attempts_rests_a_cookie_that_came_back_rate_limited(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    def fake_run_engine(engine, task_id, cmd, total_items=0, progress=None):
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
    keep_gallerydl_audio: bool = False,
    task_id: str = "task",
):
    """Run the streaming loop over ``lines``, returning the row writes it caused."""
    import backend.app.domains.downloads.workers.runner as runner_module
    from backend.app.domains.downloads.engine import engine_by_name

    writes: list[dict] = []
    volatile.forget(task_id)
    monkeypatch.setattr(runner_module.subprocess, "Popen", lambda *a, **k: _FakeProcess(iter(lines)))
    monkeypatch.setattr(runner_module, "update_task", lambda task_id, **updates: writes.append(updates))
    monkeypatch.setattr(runner_module, "_register_process", lambda task_id, process: None)
    monkeypatch.setattr(runner_module, "_unregister_process", lambda task_id: None)

    rc, dest, paths = runner_module._run_engine_to_task(
        engine_by_name(engine_name),
        task_id,
        [engine_name],
        keep_gallerydl_audio=keep_gallerydl_audio,
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
        keep_gallerydl_audio=True,
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
