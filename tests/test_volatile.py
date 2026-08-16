from __future__ import annotations

import threading
from pathlib import Path

import pytest

from backend.app.domains.downloads import store, volatile
from tests.support import use_temp_db


@pytest.fixture(autouse=True)
def clean_state():
    volatile.forget_all()
    yield
    volatile.forget_all()


def _row_writes(monkeypatch) -> list[tuple[str, dict]]:
    writes: list[tuple[str, dict]] = []
    real = store.merge_task_payload

    def counting(task_id: str, updates: dict):
        writes.append((task_id, updates))
        return real(task_id, updates)

    monkeypatch.setattr(store, "merge_task_payload", counting)
    return writes


def test_moving_the_bar_writes_no_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    writes = _row_writes(monkeypatch)

    store.record_task_progress("task", 42.5)

    assert writes == []
    assert store.load_task("task")["progress_pct"] == 42.5


def test_a_volatile_only_update_writes_no_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    writes = _row_writes(monkeypatch)

    payload = store.update_task("task", progress_pct=17.0, last_log_lines=["one"])

    assert writes == []
    assert payload["progress_pct"] == 17.0
    assert payload["last_log_lines"] == ["one"]


def test_a_durable_update_still_returns_the_live_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    store.record_task_progress("task", 61.0)
    store.append_task_log("task", "[download] 61%")

    payload = store.update_task("task", resolved_filename="clip.mp4")

    assert payload["resolved_filename"] == "clip.mp4"
    assert payload["progress_pct"] == 61.0
    assert payload["last_log_lines"] == ["[download] 61%"]


def test_the_store_listing_carries_live_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    store.record_task_progress("task", 73.5)

    listed = (store.load_task_store().get("tasks") or {})["task"]

    assert listed["progress_pct"] == 73.5


def test_finishing_a_task_drops_its_live_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    store.record_task_progress("task", 99.0)

    payload = store.update_task("task", status="completed")

    # The caller still sees the final value; nothing is left behind for the next id.
    assert payload["progress_pct"] == 99.0
    assert volatile.merge("task", {}) == {}


def test_removing_a_task_drops_its_live_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    use_temp_db(tmp_path, monkeypatch)
    store.update_task("task", status="running", source_url="https://x.test/a")
    store.record_task_progress("task", 50.0)

    store.remove_task_record("task")

    assert volatile.merge("task", {}) == {}


def test_the_log_tail_is_capped():
    for index in range(volatile.LOG_TAIL * 3):
        volatile.append_log("task", f"line {index}")

    tail = volatile.merge("task", {})["last_log_lines"]

    assert len(tail) == volatile.LOG_TAIL
    assert tail[-1] == f"line {volatile.LOG_TAIL * 3 - 1}"


def test_parallel_workers_and_readers_keep_their_own_state():
    # One thread per task writing flat out, plus readers merging the whole store,
    # which is what a worker pool and a polling API do to this at the same time.
    workers = 8
    per_worker = 500
    failures: list[BaseException] = []
    done = threading.Event()

    def write(index: int) -> None:
        task_id = f"task-{index}"
        try:
            for step in range(per_worker):
                volatile.record_progress(task_id, step / per_worker * 100)
                volatile.append_log(task_id, f"{task_id} line {step}")
        except BaseException as error:  # noqa: BLE001 - re-raised on the main thread
            failures.append(error)

    def read() -> None:
        try:
            while not done.is_set():
                volatile.merge_store({f"task-{index}": {} for index in range(workers)})
        except BaseException as error:  # noqa: BLE001 - re-raised on the main thread
            failures.append(error)

    threads = [threading.Thread(target=write, args=(index,)) for index in range(workers)]
    reader = threading.Thread(target=read)
    reader.start()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    done.set()
    reader.join()

    assert failures == []
    for index in range(workers):
        live = volatile.merge(f"task-{index}", {})
        assert live["progress_pct"] == (per_worker - 1) / per_worker * 100
        assert live["last_log_lines"][-1] == f"task-{index} line {per_worker - 1}"
        assert len(live["last_log_lines"]) == volatile.LOG_TAIL
