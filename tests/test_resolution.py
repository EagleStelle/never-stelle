from __future__ import annotations

import backend.app.core.config as config_module
import backend.app.domains.settings.storage as storage_module
from backend.app.core.pacing import CpuPacer, available_cores, background_cpu_budget
from backend.app.core.resolution import invalidate, resolution_scope, resolved, scope_active


def _counter():
    calls = {"n": 0}

    def factory():
        calls["n"] += 1
        return calls["n"]

    return calls, factory


def test_resolution_is_pass_through_without_a_scope():
    calls, factory = _counter()

    resolved("k", factory)
    resolved("k", factory)

    assert calls["n"] == 2
    assert scope_active() is False


def test_scope_resolves_each_key_once():
    calls, factory = _counter()

    with resolution_scope():
        first = resolved("k", factory)
        second = resolved("k", factory)

    assert (first, second) == (1, 1)
    assert calls["n"] == 1


def test_distinct_keys_resolve_independently():
    calls, factory = _counter()

    with resolution_scope():
        resolved("a", factory)
        resolved("b", factory)

    assert calls["n"] == 2


def test_nested_scopes_share_the_outer_resolutions():
    # A scan started inside a request must not open a second cache of the same data.
    calls, factory = _counter()

    with resolution_scope():
        resolved("k", factory)
        with resolution_scope():
            resolved("k", factory)
        resolved("k", factory)

    assert calls["n"] == 1


def test_scope_does_not_outlive_the_operation():
    calls, factory = _counter()

    with resolution_scope():
        resolved("k", factory)
    with resolution_scope():
        resolved("k", factory)

    assert calls["n"] == 2


def test_invalidate_drops_matching_keys_by_prefix():
    calls, factory = _counter()

    with resolution_scope():
        resolved("settings.fields", factory)
        resolved("core.config", factory)
        invalidate("settings.")
        resolved("settings.fields", factory)
        resolved("core.config", factory)

    assert calls["n"] == 3


def test_saving_settings_drops_the_scoped_snapshot(tmp_path, monkeypatch):
    import backend.app.db.database as database_module

    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)

    with resolution_scope():
        assert storage_module.load_saved_settings_file() == {}
        storage_module.save_saved_settings_file({"default_naming": {"strip_hashtags": True}})
        # Reading back inside the same scope must see the write, not the snapshot.
        assert storage_module.load_saved_settings_file() == {"default_naming": {"strip_hashtags": True}}


def test_app_config_is_built_once_per_scope(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(config_module, "discover_volume_locations", lambda: (calls.update(n=calls["n"] + 1), [])[1])

    with resolution_scope():
        config_module.load_app_config()
        config_module.load_app_config()
        config_module.load_app_config()

    assert calls["n"] == 1


def test_cpu_budget_is_derived_and_leaves_headroom():
    cores = available_cores()
    budget = background_cpu_budget()

    assert cores >= 1.0
    assert 0.0 < budget <= cores


def test_pacer_splits_the_budget_across_concurrent_loops():
    with CpuPacer() as first:
        alone = first._current_share()
        with CpuPacer() as second:
            shared = second._current_share()

    assert shared < alone
    assert abs(shared * 2 - alone) < 1e-9


def test_pacer_tick_is_harmless_on_a_trivial_loop():
    with CpuPacer(check_every=1) as pacer:
        for _ in range(10):
            pacer.tick()

    assert pacer.share > 0


def test_a_caller_supplied_scoped_config_still_hits_the_memo(monkeypatch):
    # Almost every caller passes the config it loaded a line earlier rather than
    # omitting it. Keying the memo on the no-argument form only meant queueing a
    # batch of URLs rebuilt the whole settings snapshot twice per URL.
    import backend.app.domains.settings.service as service_module

    calls = {"n": 0}
    real = service_module._effective_saved_settings
    monkeypatch.setattr(
        service_module,
        "_effective_saved_settings",
        lambda cfg=None: (calls.update(n=calls["n"] + 1), real(cfg))[1],
    )

    with resolution_scope():
        cfg = config_module.load_app_config()
        for _ in range(5):
            service_module.get_effective_saved_settings(cfg)
        service_module.get_effective_saved_settings()

    assert calls["n"] == 1


def test_a_foreign_config_is_not_served_from_the_memo(monkeypatch):
    import backend.app.domains.settings.service as service_module

    calls = {"n": 0}
    real = service_module._effective_saved_settings
    monkeypatch.setattr(
        service_module,
        "_effective_saved_settings",
        lambda cfg=None: (calls.update(n=calls["n"] + 1), real(cfg))[1],
    )

    with resolution_scope():
        service_module.get_effective_saved_settings()
        service_module.get_effective_saved_settings({"downloadLocations": ["/somewhere/else"]})

    assert calls["n"] == 2


def test_scoped_config_also_shortcuts_source_profiles(monkeypatch):
    import backend.app.domains.settings.profiles as profiles_module

    calls = {"n": 0}
    real = profiles_module._effective_source_profiles
    monkeypatch.setattr(
        profiles_module,
        "_effective_source_profiles",
        lambda cfg=None, payload=None, extra=None: (calls.update(n=calls["n"] + 1), real(cfg, payload, extra))[1],
    )

    with resolution_scope():
        cfg = config_module.load_app_config()
        payload = storage_module.load_saved_settings_file()
        profiles_module.get_effective_source_profiles(cfg, payload)
        profiles_module.get_effective_source_profiles(cfg)
        profiles_module.get_effective_source_profiles()
        # extra_keys widens the answer, so it must not be served from the memo.
        profiles_module.get_effective_source_profiles(cfg, payload, ["tiktok"])

    assert calls["n"] == 2


def test_is_scoped_only_matches_the_resolved_object():
    from backend.app.core.resolution import is_scoped

    with resolution_scope():
        assert is_scoped("k", None) is True
        assert is_scoped("k", {"a": 1}) is False
        held = resolved("k", lambda: {"a": 1})
        assert is_scoped("k", held) is True
        assert is_scoped("k", {"a": 1}) is False
