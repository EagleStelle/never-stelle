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


def test_run_engine_attempts_tries_anonymous_first_then_cookies(monkeypatch):
    import backend.app.domains.downloads.workers.execution as worker_module
    import backend.app.domains.downloads.ytdlp as ytdlp_module
    from backend.app.domains.downloads.engine import YtdlpEngine

    attempts_seen = []

    def fake_run_engine(engine, task_id, cmd, total_items=0):
        with_cookies = "--cookies" in cmd
        attempts_seen.append(with_cookies)
        if not with_cookies:
            # Simulate anonymous attempt failure (e.g. age restricted)
            return 1, "", []
        return 0, "/tmp/out.mp4", ["/tmp/out.mp4"]

    monkeypatch.setattr(worker_module, "has_cookies_for_source", lambda source_key: True)
    monkeypatch.setattr(ytdlp_module, "find_cookies_file_for_source", lambda source_key: "/tmp/cookies.txt")
    monkeypatch.setattr(worker_module, "_run_engine_to_task", fake_run_engine)

    rc, _, _ = worker_module._run_engine_attempts(
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

    assert rc == 0
    assert attempts_seen == [False, True]
