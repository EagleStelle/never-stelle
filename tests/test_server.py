from __future__ import annotations

from backend.app.server import MAX_IDLE_TICK_SECONDS, MIN_IDLE_TICK_SECONDS, idle_tick_seconds


def test_idle_tick_seconds_defaults(monkeypatch):
    monkeypatch.delenv("NEVER_STELLE_IDLE_TICK_SECONDS", raising=False)

    assert idle_tick_seconds() == 5.0


def test_idle_tick_seconds_clamps_low_values(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_IDLE_TICK_SECONDS", "0.001")

    assert idle_tick_seconds() == MIN_IDLE_TICK_SECONDS


def test_idle_tick_seconds_clamps_high_values(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_IDLE_TICK_SECONDS", "99")

    assert idle_tick_seconds() == MAX_IDLE_TICK_SECONDS


def test_idle_tick_seconds_ignores_invalid_values(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_IDLE_TICK_SECONDS", "slow")

    assert idle_tick_seconds() == 5.0
