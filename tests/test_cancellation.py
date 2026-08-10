from __future__ import annotations

import errno
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.app.domains.downloads.enrich as enrich_module
import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.postprocessing as postprocessing_module
import backend.app.domains.downloads.workers.execution as execution_module
import backend.app.domains.downloads.workers.processes as processes_module
import backend.app.runtime.scratch as scratch_module


def test_cancel_interrupts_finalizer_process_reaps_it_and_clears_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    task_id = "ytdlp:finalizing"
    cancelled = threading.Event()

    def finalize() -> None:
        workspace = scratch_module.scratch_temp_dir(prefix="nvs-download-task-")
        (workspace / "partial.tmp").write_bytes(b"partial")
        try:
            with processes_module.task_execution(task_id):
                processes_module.run_task_subprocess(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    capture_output=True,
                    text=True,
                )
        except processes_module.TaskCancelled:
            cancelled.set()
        finally:
            scratch_module.remove_scratch_path(workspace)

    thread = threading.Thread(target=finalize, name="test-finalizer")
    thread.start()
    deadline = time.monotonic() + 5
    while not processes_module.has_active_process(task_id) and time.monotonic() < deadline:
        time.sleep(0.01)

    assert processes_module.has_active_task(task_id)
    assert processes_module.has_active_process(task_id)
    processes_module.request_cancel(task_id)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert cancelled.is_set()
    assert not processes_module.has_active_task(task_id)
    assert not processes_module.has_active_process(task_id)
    assert list(scratch_root.iterdir()) == []

    # Cancellation state belongs to one execution only; reusing an id cannot
    # poison this worker or any future queue entry.
    with processes_module.task_execution(task_id):
        processes_module.raise_if_cancelled(task_id)


def test_cancelled_thumbnail_conversion_removes_its_temporary_output(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    thumbnail = tmp_path / "cover.webp"
    thumbnail.write_bytes(b"image")
    task_id = "ytdlp:thumbnail"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    def cancel_ffmpeg(*args, **kwargs):
        processes_module.request_cancel(task_id)
        processes_module.raise_if_cancelled(task_id)

    monkeypatch.setattr(postprocessing_module, "run_task_subprocess", cancel_ffmpeg)

    with pytest.raises(processes_module.TaskCancelled):
        with processes_module.task_execution(task_id):
            postprocessing_module._convert_thumbnail_for_embedding("ffmpeg", thumbnail)

    assert list(scratch_root.iterdir()) == []


def test_cancelled_cross_filesystem_publish_removes_staging_file(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    source = scratch_module.scratch_temp_path(prefix="nvs-publish-", suffix=".mp4")
    source.write_bytes(b"x" * (2 * 1024 * 1024))
    target = tmp_path / "media" / "clip.mp4"
    target.parent.mkdir()
    original_replace = Path.replace

    def replace_across_mount(path: Path, destination: Path):
        if path == source.resolve():
            raise OSError(errno.EXDEV, "cross-device", str(path), str(destination))
        return original_replace(path, destination)

    def cancel_copy() -> None:
        raise processes_module.TaskCancelled()

    monkeypatch.setattr(Path, "replace", replace_across_mount)

    with pytest.raises(processes_module.TaskCancelled):
        scratch_module.publish_scratch_file(source, target, cancel_check=cancel_copy)

    assert source.exists()
    assert not target.exists()
    assert list(target.parent.iterdir()) == []


def test_cancel_running_task_signals_python_finalization_without_a_subprocess(monkeypatch):
    task_id = "ytdlp:finalizing"
    requested: list[str] = []
    removed: list[str] = []
    monkeypatch.setattr(
        operations_module,
        "load_task_store",
        lambda: {"tasks": {task_id: {"status": "running"}}},
    )
    monkeypatch.setattr(operations_module, "has_active_task", lambda value: value == task_id)
    monkeypatch.setattr(operations_module, "request_cancel", requested.append)
    monkeypatch.setattr(operations_module, "remove_task_record", removed.append)

    operations_module.cancel_task(task_id)

    assert requested == [task_id]
    assert removed == []


def test_pending_cancel_handles_a_simultaneous_scheduler_claim(monkeypatch):
    task_id = "ytdlp:claim-race"
    reads = iter(
        [
            {"tasks": {task_id: {"status": "pending"}}},
            {"tasks": {task_id: {"status": "running"}}},
        ]
    )
    requested: list[str] = []
    monkeypatch.setattr(operations_module, "load_task_store", lambda: next(reads))
    monkeypatch.setattr(operations_module, "remove_task_record_if_status", lambda *args: False)
    monkeypatch.setattr(operations_module, "has_active_task", lambda value: value == task_id)
    monkeypatch.setattr(operations_module, "request_cancel", requested.append)

    operations_module.cancel_task(task_id)

    assert requested == [task_id]


def test_run_task_cancellation_during_post_processing_removes_task_and_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    scratch_root = tmp_path / "scratch"
    output = tmp_path / "clip.mp4"
    output.write_bytes(b"media")
    task_id = "ytdlp:post-process"
    task = {
        "engine": "fake",
        "source_url": "https://example.test/watch/1",
        "source_key": "example",
        "status": "running",
        "output_dir": str(tmp_path),
        "resolved_folder": str(tmp_path),
        "post_processing": {"metadata": True, "save_as": "sidecar"},
    }
    store = {task_id: task}
    workspaces: list[Path] = []

    class FakeEngine:
        name = "fake"
        needs_ffmpeg = False
        emits_progress = True

        def build_output_template(self, *args, **kwargs):
            return str(output)

        def count_items(self, *args, **kwargs):
            return 0

    def make_workspace(*, prefix: str) -> Path:
        workspace = scratch_module.scratch_temp_dir(prefix=prefix)
        workspaces.append(workspace)
        return workspace

    finalized = SimpleNamespace(
        keep_paths=[output],
        final_path=output,
        media_id="1",
        source_url=task["source_url"],
        source_key="example",
        creator="",
        display_filename=output.name,
        title="",
    )

    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    monkeypatch.setattr(execution_module, "scratch_temp_dir", make_workspace)
    monkeypatch.setattr(execution_module, "_engine_run_order", lambda value: [FakeEngine()])
    monkeypatch.setattr(execution_module, "load_token_roles", lambda: {})
    monkeypatch.setattr(execution_module, "get_effective_fields", lambda value: {})
    monkeypatch.setattr(execution_module, "load_slug_tokens", lambda: {})
    monkeypatch.setattr(execution_module, "load_scrape_rules", lambda: {})
    monkeypatch.setattr(execution_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(enrich_module, "resolve_slug_tokens", lambda *args, **kwargs: {})
    monkeypatch.setattr(enrich_module, "resolve_scraped_tokens", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        execution_module,
        "_run_engine_attempts",
        lambda *args, **kwargs: (0, str(output), [str(output)]),
    )
    monkeypatch.setattr(execution_module, "load_task", lambda value: store.get(value, {}))
    monkeypatch.setattr(execution_module, "update_task", lambda value, **updates: store[value].update(updates))
    monkeypatch.setattr(execution_module, "remove_task_record", lambda value: store.pop(value, None))
    monkeypatch.setattr(execution_module, "_read_metadata_sidecar", lambda value: {})
    monkeypatch.setattr(execution_module, "_probe_single_output_metadata_inline", lambda *args: None)
    monkeypatch.setattr(execution_module, "_single_output_metadata_enrichment_needed", lambda *args: False)
    monkeypatch.setattr(execution_module, "_dedupe_output_records", lambda records, *args: records)
    monkeypatch.setattr(
        execution_module,
        "_download_groups",
        lambda *args: [{"path": output, "paths": [output], "media_id": "1", "metadata": {}}],
    )
    monkeypatch.setattr(execution_module, "_finalize_completed_output", lambda **kwargs: finalized)

    def cancel_in_post_processing(*args, **kwargs):
        (workspaces[0] / "post-process.tmp").write_bytes(b"partial")
        processes_module.request_cancel(task_id)
        processes_module.raise_if_cancelled(task_id)

    monkeypatch.setattr(execution_module, "apply_finalized_post_processing", cancel_in_post_processing)

    execution_module.run_task(task_id, task, mark_running=False)

    assert task_id not in store
    assert workspaces and not workspaces[0].exists()
    assert list(scratch_root.iterdir()) == []
    assert not processes_module.has_active_task(task_id)
