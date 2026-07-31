from __future__ import annotations

from backend.app.domains.downloads import cache as cache_module


def test_drop_file_cache_advises_unique_existing_files_without_sync_by_default(tmp_path, monkeypatch):
    first = tmp_path / "one.mp4"
    second = tmp_path / "two.mp4"
    missing = tmp_path / "missing.mp4"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    synced: list[int] = []
    advised: list[tuple[int, int, int]] = []

    def fake_fdatasync(fd: int) -> None:
        synced.append(fd)

    def fake_posix_fadvise(fd: int, offset: int, length: int, advice: int) -> None:
        advised.append((offset, length, advice))

    monkeypatch.setattr(cache_module.os, "fdatasync", fake_fdatasync, raising=False)
    monkeypatch.setattr(cache_module.os, "posix_fadvise", fake_posix_fadvise, raising=False)
    monkeypatch.setattr(cache_module.os, "POSIX_FADV_DONTNEED", 4, raising=False)
    monkeypatch.delenv("NEVER_STELLE_DROP_CACHE_SYNC", raising=False)

    cache_module.drop_file_cache([first, first, missing, second])

    assert synced == []
    assert advised == [(0, 0, 4), (0, 0, 4)]


def test_drop_file_cache_syncs_when_enabled(tmp_path, monkeypatch):
    media = tmp_path / "one.mp4"
    media.write_bytes(b"one")
    synced: list[int] = []
    advised: list[tuple[int, int, int]] = []

    monkeypatch.setattr(cache_module.os, "fdatasync", lambda fd: synced.append(fd), raising=False)
    monkeypatch.setattr(
        cache_module.os,
        "posix_fadvise",
        lambda fd, offset, length, advice: advised.append((offset, length, advice)),
        raising=False,
    )
    monkeypatch.setattr(cache_module.os, "POSIX_FADV_DONTNEED", 4, raising=False)
    monkeypatch.setenv("NEVER_STELLE_DROP_CACHE_SYNC", "true")

    cache_module.drop_file_cache([media])

    assert len(synced) == 1
    assert advised == [(0, 0, 4)]


def test_drop_file_cache_noops_without_kernel_support(tmp_path, monkeypatch):
    media = tmp_path / "one.mp4"
    media.write_bytes(b"one")
    monkeypatch.delattr(cache_module.os, "posix_fadvise", raising=False)
    monkeypatch.delattr(cache_module.os, "POSIX_FADV_DONTNEED", raising=False)

    cache_module.drop_file_cache([media])
