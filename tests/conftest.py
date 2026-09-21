from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import backend.app.db.database as database_module
from backend.app.integrations.swaratelle import breaker as swaratelle_breaker
from tests.support import use_temp_db


@pytest.fixture(autouse=True)
def isolated_swaratelle_breaker() -> Iterator[None]:
    """Clears the process-wide breaker state between tests."""
    swaratelle_breaker.reset()
    yield
    swaratelle_breaker.reset()


@pytest.fixture(autouse=True)
def isolated_database(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Every test gets its own database file.

    Tests that only stub the load side of a store still reach the real write path, and
    the real path is the developer's own library: a scan test used to write its fixture
    URLs into the running install as learned formats.
    """
    use_temp_db(Path(tmp_path_factory.mktemp("db")), monkeypatch)
    yield
    # Released before the temp directory is swept, and so the next test opens its own file.
    database_module.close_database()
