from __future__ import annotations

import pytest

import backend.app.db.database as database_module
import backend.app.db.volume as volume


def _database_with_lock(tmp_path):
    database_path = tmp_path / "never-stelle.sqlite3"
    database_path.write_bytes(b"")
    lock = volume.dot_lock(database_path)
    lock.mkdir()
    return database_path, lock


def test_a_lock_no_process_stands_behind_is_released(tmp_path, monkeypatch):
    database_path, lock = _database_with_lock(tmp_path)
    monkeypatch.setattr(volume, "_opened_elsewhere", lambda path: False)

    assert volume.release_stale_lock(database_path, "9p") == lock
    assert not lock.exists()


def test_a_lock_a_live_process_holds_is_left_alone(tmp_path, monkeypatch):
    database_path, lock = _database_with_lock(tmp_path)
    monkeypatch.setattr(volume, "_opened_elsewhere", lambda path: True)

    assert volume.release_stale_lock(database_path, "9p") is None
    assert lock.is_dir()


def test_a_local_disk_has_no_dot_file_lock(tmp_path, monkeypatch):
    database_path, lock = _database_with_lock(tmp_path)
    monkeypatch.setattr(volume, "_opened_elsewhere", lambda path: False)

    # Only a network mount is opened through the dot-file VFS, so only there is the
    # directory beside the database a lock at all.
    assert volume.release_stale_lock(database_path, "") is None
    assert lock.is_dir()


def test_a_process_namespace_that_cannot_be_read_counts_as_held(tmp_path, monkeypatch):
    database_path, lock = _database_with_lock(tmp_path)
    monkeypatch.setattr(volume, "_PROCESSES", tmp_path / "absent")

    assert volume.release_stale_lock(database_path, "9p") is None
    assert lock.is_dir()


def test_a_native_open_holds_the_dot_file_lock_until_closed():
    database_module.initialize_database()
    lock = volume.dot_lock(database_module.DATABASE_PATH)
    assert lock.is_file()

    database_module.close_database()
    assert not lock.exists()


def test_a_native_open_refuses_a_database_locked_across_a_mount():
    lock = volume.dot_lock(database_module.DATABASE_PATH)
    lock.mkdir()

    with pytest.raises(database_module.DatabaseVolumeError, match="open in another process"):
        database_module.initialize_database()
    assert lock.is_dir()


def test_a_native_lock_left_by_a_dead_process_is_taken_over():
    lock = volume.dot_lock(database_module.DATABASE_PATH)
    lock.write_bytes(b"")

    database_module.initialize_database()
    assert lock.is_file()
