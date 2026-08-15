from contextlib import closing

import backend.app.domains.settings.cookie_pool as pool


def _stub_pool(monkeypatch, jars, *, limit=100, window=3600.0, delay=0.0, cooldown=900.0, policies=None):
    entries = {
        "instagram": [{"id": jar, "filename": f"{jar}.txt", "uploaded_at": ""} for jar in jars]
    }
    default = pool.CookiePolicy(limit=limit, window=window, delay=delay, cooldown=cooldown, wait=0.0)
    resolved = policies or {}
    monkeypatch.setattr(pool, "list_cookies_for_source", lambda key: entries.get(key, []))
    monkeypatch.setattr(pool, "materialize_cookie", lambda cookie_id: f"/tmp/{cookie_id}.txt")
    monkeypatch.setattr(pool, "cookie_policy_for_source", lambda key: resolved.get(key, default))
    pool.reset_cookie_pool()
    return entries


def test_lease_returns_nothing_when_the_source_has_no_cookies(monkeypatch):
    _stub_pool(monkeypatch, [])

    assert pool.lease_cookie("instagram") is None


def test_rotation_spreads_requests_evenly_across_every_jar(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b", "c"])

    used = []
    for _ in range(6):
        lease = pool.lease_cookie("instagram")
        assert lease is not None
        used.append(lease.cookie_id)
        pool.release_cookie(lease)

    assert sorted(used) == ["a", "a", "b", "b", "c", "c"]


def test_a_leased_jar_is_not_handed_to_a_second_caller(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b"])

    first = pool.lease_cookie("instagram")
    second = pool.lease_cookie("instagram")

    assert first is not None and second is not None
    assert {first.cookie_id, second.cookie_id} == {"a", "b"}
    # Pool exhausted: a third caller waits rather than doubling up on a jar.
    assert pool.lease_cookie("instagram", wait_seconds=0) is None

    pool.release_cookie(first)
    assert pool.lease_cookie("instagram", wait_seconds=0) is not None


def test_releasing_a_lease_drops_the_materialized_cookie(monkeypatch):
    dropped = []
    _stub_pool(monkeypatch, ["a"])
    monkeypatch.setattr(pool, "drop_materialized_cookie", lambda path: dropped.append(path))

    lease = pool.lease_cookie("instagram")
    assert lease is not None

    pool.release_cookie(lease)

    assert dropped == ["/tmp/a.txt"]


def test_more_jars_means_more_requests_before_the_pool_runs_dry(monkeypatch):
    def drain(jars):
        _stub_pool(monkeypatch, jars, limit=2)
        served = 0
        while True:
            lease = pool.lease_cookie("instagram", wait_seconds=0)
            if lease is None:
                return served
            served += 1
            pool.release_cookie(lease)

    assert drain(["a"]) == 2
    assert drain(["a", "b"]) == 4
    assert drain(["a", "b", "c"]) == 6


def test_per_jar_delay_parks_a_jar_between_uses(monkeypatch):
    _stub_pool(monkeypatch, ["a"], delay=60.0)

    lease = pool.lease_cookie("instagram")
    assert lease is not None
    pool.release_cookie(lease)

    assert pool.lease_cookie("instagram", wait_seconds=0) is None


def test_per_jar_limit_parks_a_jar_for_the_rest_of_the_window(monkeypatch):
    _stub_pool(monkeypatch, ["a"], limit=1)

    lease = pool.lease_cookie("instagram")
    assert lease is not None
    pool.release_cookie(lease)

    assert pool.lease_cookie("instagram", wait_seconds=0) is None


def test_a_blocked_jar_rests_while_its_sibling_keeps_working(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b"], cooldown=600.0)

    first = pool.lease_cookie("instagram")
    assert first is not None
    first.banned = True
    pool.release_cookie(first)

    for _ in range(3):
        lease = pool.lease_cookie("instagram", wait_seconds=0)
        assert lease is not None
        assert lease.cookie_id != first.cookie_id
        pool.release_cookie(lease)


def test_looks_rate_limited_reads_engine_output():
    assert pool.looks_rate_limited("ERROR: HTTP Error 429: Too Many Requests")
    assert pool.looks_rate_limited("Sign in to confirm you are not a bot")
    assert not pool.looks_rate_limited("ERROR: Unsupported URL")


def test_invalidate_forgets_jars_that_were_deleted(monkeypatch):
    entries = _stub_pool(monkeypatch, ["a"], limit=1)

    lease = pool.lease_cookie("instagram")
    assert lease is not None
    pool.release_cookie(lease)
    assert pool.lease_cookie("instagram", wait_seconds=0) is None

    # Replacing the jar clears its spent quota; the fresh upload starts at zero.
    entries["instagram"] = [{"id": "b", "filename": "b.txt", "uploaded_at": ""}]
    pool.invalidate_cookie_pool("instagram")

    replacement = pool.lease_cookie("instagram", wait_seconds=0)
    assert replacement is not None and replacement.cookie_id == "b"


def test_rotation_walks_the_list_in_order_until_one_works(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b", "c"])

    walked = []
    with closing(pool.cookie_rotation("instagram")) as rotation:
        for lease in rotation:
            walked.append(lease.cookie_id)

    assert walked == ["a", "b", "c"]


def test_rotation_stops_at_the_jar_that_works_and_frees_the_rest(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b", "c"])

    walked = []
    with closing(pool.cookie_rotation("instagram")) as rotation:
        for lease in rotation:
            walked.append(lease.cookie_id)
            if lease.cookie_id == "b":
                break

    assert walked == ["a", "b"]
    # Both jars went back to the pool, so the next task can use them right away.
    assert pool.lease_cookie("instagram", wait_seconds=0) is not None


def test_parallel_rotations_never_share_a_jar(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b"])

    first = pool.cookie_rotation("instagram")
    second = pool.cookie_rotation("instagram")
    try:
        assert next(first).cookie_id != next(second).cookie_id
    finally:
        first.close()
        second.close()


def test_rotation_hands_later_tasks_a_different_starting_jar(monkeypatch):
    _stub_pool(monkeypatch, ["a", "b", "c"])

    def first_jar():
        with closing(pool.cookie_rotation("instagram")) as rotation:
            return next(rotation).cookie_id

    # The list decides the cold-start order; used jars then fall behind their siblings.
    assert [first_jar() for _ in range(3)] == ["a", "b", "c"]


def test_each_source_runs_on_its_own_configured_limits(monkeypatch):
    # instagram allows one request per window; the default pool allows plenty.
    _stub_pool(
        monkeypatch,
        ["a", "b"],
        policies={"instagram": pool.CookiePolicy(limit=1, window=3600.0, delay=0.0, wait=0.0)},
    )

    served = 0
    while True:
        lease = pool.lease_cookie("instagram", wait_seconds=0)
        if lease is None:
            break
        served += 1
        pool.release_cookie(lease)

    # Two jars, one request each: the source is spent after two leases.
    assert served == 2


def test_a_jar_that_cannot_be_written_out_is_dropped_instead_of_retried(monkeypatch):
    _stub_pool(monkeypatch, ["dead"])
    tried = []

    def never_materializes(cookie_id):
        tried.append(cookie_id)
        return ""

    monkeypatch.setattr(pool, "materialize_cookie", never_materializes)

    assert pool.lease_cookie("instagram", wait_seconds=0.2) is None
    assert tried == ["dead"]


def test_a_broken_jar_does_not_stop_its_siblings_from_being_leased(monkeypatch):
    _stub_pool(monkeypatch, ["dead", "good"])
    monkeypatch.setattr(
        pool, "materialize_cookie", lambda cookie_id: "" if cookie_id == "dead" else f"/tmp/{cookie_id}.txt"
    )

    lease = pool.lease_cookie("instagram", wait_seconds=0)

    assert lease is not None and lease.cookie_id == "good"


def test_a_lease_keeps_the_limits_it_was_taken_under(monkeypatch):
    _stub_pool(
        monkeypatch,
        ["a"],
        policies={"instagram": pool.CookiePolicy(limit=10, window=3600.0, delay=0.0, cooldown=600.0, wait=0.0)},
    )

    lease = pool.lease_cookie("instagram")
    assert lease is not None and lease.policy.cooldown == 600.0

    # The release path reads the lease's own snapshot, never the settings again.
    lease.banned = True
    pool.release_cookie(lease)
    assert pool.lease_cookie("instagram", wait_seconds=0) is None
