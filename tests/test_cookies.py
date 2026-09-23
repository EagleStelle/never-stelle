from __future__ import annotations

import asyncio
import io
from pathlib import Path

import pytest
from fastapi import UploadFile

import backend.app.db.database as database_module
import backend.app.runtime.scratch as scratch_module
from backend.app.db import repositories
from backend.app.domains.settings import cookies as cookies_module
from tests.support import use_temp_db


def test_materialized_cookie_is_one_use_temp_file(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch)

    try:
        repositories.add_source_cookie("jar1", "instagram", "jar.txt", b"cookies")

        path = Path(cookies_module.materialize_cookie("jar1"))

        assert path.parent == scratch
        assert path.name.startswith("nvs-cookie-")
        assert path.name.endswith(".txt")
        assert path.read_bytes() == b"cookies"

        cookies_module.drop_materialized_cookie(str(path))

        assert not path.exists()
    finally:
        database_module.close_database()


_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
_FIREFOX = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"


@pytest.mark.parametrize(
    ("uploader", "stored", "browser"),
    [(_CHROME, _CHROME, "Chrome 140 Windows"), (_FIREFOX, "", "Default Chrome")],
)
def test_an_upload_keeps_the_browser_that_sent_it(monkeypatch, uploader, stored, browser):
    monkeypatch.setattr(cookies_module, "require_settings_managed_source", lambda key: None)
    upload = UploadFile(io.BytesIO(b"# Netscape HTTP Cookie File\n"), filename="cookies.txt")

    entry = asyncio.run(cookies_module.save_ytdlp_cookies_upload(upload, "instagram", uploader))
    (listed,) = cookies_module.list_cookies_for_source("instagram")

    assert (entry["user_agent"], entry["browser"]) == (stored, browser)
    assert (listed["user_agent"], listed["browser"]) == (stored, browser)
