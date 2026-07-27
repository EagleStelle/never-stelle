import backend.app.db.database as database_module
import backend.app.domains.settings.cookie_policy as policy_module
from backend.app.domains.settings import (
    DEFAULT_COOKIE_POLICY,
    cookie_policy_for_source,
    normalize_source_cookie_policies,
    persist_settings,
)


def use_temp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
    policy_module.invalidate_cookie_policies()


def test_normalize_keeps_only_the_fields_the_user_set():
    normalized = normalize_source_cookie_policies(
        {"Instagram": {"limit": "5", "delay": 2.5, "window": "", "cooldown": None, "junk": 9}}
    )

    # Blank/absent fields are dropped so the defaults keep applying to them.
    assert normalized == {"instagram": {"limit": 5, "delay": 2.5}}


def test_normalize_clamps_values_into_their_range():
    normalized = normalize_source_cookie_policies(
        {"tiktok": {"limit": 0, "window": 10_000_000, "delay": -4, "wait": "abc"}}
    )

    assert normalized["tiktok"] == {"limit": 1, "window": 86_400.0, "delay": 0.0}


def test_normalize_drops_sources_with_nothing_configured():
    assert normalize_source_cookie_policies({"twitter": {}, "": {"limit": 3}, "x": "nope"}) == {}


def test_unconfigured_source_uses_the_built_in_defaults(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)

    assert cookie_policy_for_source("instagram") == DEFAULT_COOKIE_POLICY


def test_saving_settings_takes_effect_without_a_restart(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    cfg = {"downloadLocations": []}
    profiles = [{"key": "instagram", "label": "Instagram", "hosts": ["instagram.com"]}]

    assert cookie_policy_for_source("instagram").limit == DEFAULT_COOKIE_POLICY.limit

    persist_settings(
        cfg,
        {},
        None,
        profiles,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        {"instagram": {"limit": 4, "cooldown": 60}},
    )

    # The write path drops the cache, so the next lookup sees the new numbers.
    policy = cookie_policy_for_source("instagram")
    assert policy.limit == 4
    assert policy.cooldown == 60.0
    # Untouched fields keep their defaults.
    assert policy.delay == DEFAULT_COOKIE_POLICY.delay
    assert cookie_policy_for_source("tiktok") == DEFAULT_COOKIE_POLICY


def test_configured_defaults_apply_to_every_source(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    cfg = {"downloadLocations": []}
    profiles = [{"key": "instagram", "label": "Instagram", "hosts": ["instagram.com"]}]

    persist_settings(
        cfg,
        {},
        None,
        profiles,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        {"instagram": {"limit": 4}},
        raw_default_cookie_policy={"limit": 9, "delay": 1.5},
    )

    # A source with nothing of its own follows the configured defaults.
    tiktok = cookie_policy_for_source("tiktok")
    assert tiktok.limit == 9
    assert tiktok.delay == 1.5
    assert tiktok.cooldown == DEFAULT_COOKIE_POLICY.cooldown
    # A source override wins per field; the rest still come from the defaults.
    instagram = cookie_policy_for_source("instagram")
    assert instagram.limit == 4
    assert instagram.delay == 1.5


def test_resolved_policies_are_cached_between_lookups(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    reads = []

    def counted_load():
        reads.append(1)
        return {"source_cookie_policies": {"instagram": {"limit": 7}}}

    monkeypatch.setattr(policy_module, "load_saved_settings_file", counted_load)

    for _ in range(50):
        assert cookie_policy_for_source("instagram").limit == 7

    # One settings read total; every later lookup is a dict hit.
    assert len(reads) == 1
