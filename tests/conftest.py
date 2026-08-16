from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import backend.app.db.database as database_module
from tests.support import use_temp_db


@pytest.fixture(autouse=True)
def isolated_database(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Every test gets its own database file.

    Tests that only stub the load side of a store still reach the real write path, and
    the real path is the developer's own library: a scan test used to seed its fixture
    URLs into the running install and drop the seeded-download ledger with them.
    """
    use_temp_db(Path(tmp_path_factory.mktemp("db")), monkeypatch)
    yield
    # Released before the temp directory is swept, and so the next test opens its own file.
    database_module.close_database()
