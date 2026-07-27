from __future__ import annotations

from pathlib import Path

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

    def fake_run_engine(engine, task_id, cmd, total_items=0):
        with_cookies = "--cookies" in cmd
        attempts_seen.append(with_cookies)
        if not with_cookies:
            # Simulate anonymous attempt failure (e.g. age restricted)
            return 1, "", []
        assert cmd[cmd.index("--cookies") + 1] == "/tmp/cookies-jar1.txt"
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    (lease,) = _stub_worker_cookie_rotation(monkeypatch, worker_module)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "_append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "update_task", lambda task_id, **kwargs: {})

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 0
    assert attempts_seen == [False, True]
    assert lease.banned is False


def test_run_engine_attempts_spends_no_cookie_when_anonymous_succeeds(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module

    leased = []

    def fake_run_engine(engine, task_id, cmd, total_items=0):
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

    def fake_run_engine(engine, task_id, cmd, total_items=0):
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
    monkeypatch.setattr(worker_module, "_append_task_log", lambda task_id, message: None)
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

    def fake_run_engine(engine, task_id, cmd, total_items=0):
        return 1, "", []

    (lease,) = _stub_worker_cookie_rotation(monkeypatch, worker_module)
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)
    monkeypatch.setattr(worker_module, "_append_task_log", lambda task_id, message: None)
    monkeypatch.setattr(worker_module, "update_task", lambda task_id, **kwargs: {})
    monkeypatch.setattr(
        worker_module, "_task_log_tail", lambda task_id: "ERROR: HTTP Error 429: Too Many Requests"
    )

    rc, _, _ = _run_attempts(worker_module)

    assert rc == 1
    assert lease.banned is True
