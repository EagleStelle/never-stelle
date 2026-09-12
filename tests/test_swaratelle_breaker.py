from __future__ import annotations

import time

import httpx
import pytest

from backend.app.integrations.swaratelle import breaker
from backend.app.integrations.swaratelle import client as swaratelle


def _wait_for_probe(timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not breaker._probing:
            return
        time.sleep(0.01)
    raise AssertionError("Probe never finished.")


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle.invalid")


def test_poll_calls_do_not_wait_on_an_unreachable_remote(
    configured: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unreachable(*args, **kwargs):
        raise httpx.ConnectError("no route")

    monkeypatch.setattr(swaratelle, "_shared_client", lambda: _Stub(unreachable))

    started = time.monotonic()
    assert swaratelle.fetch_counts() == {}
    assert swaratelle.fetch_active_tasks() == []
    assert swaratelle.fetch_history_page() == {"entries": []}
    assert swaratelle.scan_media_library() == {"checked": 0, "missing": 0, "added": 0}

    # The retry goes to a background probe, not to this caller.
    assert time.monotonic() - started < 1.0


def test_a_transport_failure_opens_the_breaker(configured: None, monkeypatch: pytest.MonkeyPatch) -> None:
    breaker.record_success()
    calls = {"count": 0}

    def unreachable(*args, **kwargs):
        calls["count"] += 1
        raise httpx.ConnectTimeout("timed out")

    monkeypatch.setattr(swaratelle, "_shared_client", lambda: _Stub(unreachable))

    assert swaratelle.fetch_counts() == {}
    assert calls["count"] == 1
    assert swaratelle.fetch_counts() == {}
    # The second call never reached the network.
    assert calls["count"] == 1


def test_an_http_error_leaves_the_breaker_closed(configured: None, monkeypatch: pytest.MonkeyPatch) -> None:
    breaker.record_success()
    calls = {"count": 0}

    def refuse(*args, **kwargs):
        calls["count"] += 1
        request = httpx.Request("GET", "http://swaratelle.invalid/api/counts")
        return httpx.Response(500, request=request, json={"error": "boom"})

    monkeypatch.setattr(swaratelle, "_shared_client", lambda: _Stub(refuse))

    assert swaratelle.fetch_counts() == {}
    assert swaratelle.fetch_counts() == {}
    # Both calls went out, the remote was reachable.
    assert calls["count"] == 2


def test_the_probe_closes_the_breaker_once_the_remote_answers(
    configured: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def answer(*args, **kwargs):
        request = httpx.Request("GET", "http://swaratelle.invalid/api/counts")
        return httpx.Response(200, request=request, json={"queued": 1, "running": 0, "completed": 2, "failed": 0})

    monkeypatch.setattr(swaratelle, "_shared_client", lambda: _Stub(answer))

    # Cold start refuses and probes in the background.
    assert swaratelle.fetch_counts() == {}
    _wait_for_probe()

    assert swaratelle.fetch_counts() == {"queued": 1, "running": 0, "completed": 2, "failed": 0}


def test_an_unconfigured_remote_never_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARATELLE_URL", raising=False)

    def fail(*args, **kwargs):
        raise AssertionError("An unconfigured remote must not be contacted.")

    monkeypatch.setattr(swaratelle, "_shared_client", lambda: _Stub(fail))

    assert swaratelle.fetch_counts() == {}
    _wait_for_probe()


class _Stub:
    def __init__(self, handler) -> None:
        self._handler = handler

    def request(self, *args, **kwargs):
        response = self._handler(*args, **kwargs)
        response.raise_for_status()
        return response
