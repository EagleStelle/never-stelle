from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from .cookie_policy import DEFAULT_COOKIE_POLICY, CookiePolicy, cookie_policy_for_source
from .cookies import list_cookies_for_source, materialize_cookie, normalize_cookie_source

# Site responses that mean "this jar is burnt for now" rather than "this link is bad".
RATE_LIMIT_MARKERS = (
    "429",
    "too many requests",
    "rate limit",
    "rate-limit",
    "ratelimit",
    "temporarily blocked",
    "try again later",
    "http error 403",
    "sign in to confirm",
    "login required",
    "account has been",
    "checkpoint_required",
    "challenge_required",
    "please wait a few minutes",
)

# Never spin: a wake-up is only useful once some jar's timer has actually elapsed.
_WAKE_EPSILON_SECONDS = 0.05


@dataclass
class _CookieState:
    cookie_id: str
    source_key: str
    in_use: bool = False
    used_at: float = 0.0
    ready_at: float = 0.0
    window_start: float = field(default_factory=time.monotonic)
    window_count: int = 0
    uses: int = 0


@dataclass
class CookieLease:
    cookie_id: str
    source_key: str
    path: str
    filename: str
    # Caller sets this when the run came back blocked; the jar then rests longer.
    banned: bool = False
    # Limits this source was configured with, resolved once when the jar was taken
    # so the release path re-reads nothing.
    policy: CookiePolicy = DEFAULT_COOKIE_POLICY


_CONDITION = threading.Condition()
_STATES: dict[str, _CookieState] = {}


def looks_rate_limited(text: Any) -> bool:
    value = str(text or "").lower()
    return any(marker in value for marker in RATE_LIMIT_MARKERS)


def reset_cookie_pool() -> None:
    with _CONDITION:
        _STATES.clear()
        _CONDITION.notify_all()


def invalidate_cookie_pool(source_key: str = "") -> None:
    """Forget runtime counters for jars that no longer exist (upload/delete)."""
    key = normalize_cookie_source(source_key)
    if not key:
        reset_cookie_pool()
        return
    live = {entry["id"] for entry in list_cookies_for_source(key)}
    with _CONDITION:
        for cookie_id, state in list(_STATES.items()):
            if state.source_key == key and cookie_id not in live:
                del _STATES[cookie_id]
        _CONDITION.notify_all()


def _sync_states(source_key: str, entries: dict[str, dict[str, Any]]) -> None:
    now = time.monotonic()
    for cookie_id in entries:
        if cookie_id not in _STATES:
            _STATES[cookie_id] = _CookieState(cookie_id=cookie_id, source_key=source_key, window_start=now)


def _pick_state(
    entries: dict[str, dict[str, Any]], now: float, policy: CookiePolicy
) -> _CookieState | None:
    """Next jar to use: coldest first, list order breaking ties.

    Ranking is (uses this window, least recently used, list position). The list is
    the user's order, so a fresh pool is walked top-down; once a jar has served a
    request it drops behind its siblings, which spreads traffic over the whole list
    instead of hammering the first entry.
    """
    limit = policy.limit
    window = policy.window
    best: tuple[tuple[int, float, int], _CookieState] | None = None
    for position, cookie_id in enumerate(entries):
        state = _STATES[cookie_id]
        if state.in_use or now < state.ready_at:
            continue
        if now - state.window_start >= window:
            state.window_start = now
            state.window_count = 0
        if state.window_count >= limit:
            continue
        ranking = (state.window_count, state.used_at, position)
        if best is None or ranking < best[0]:
            best = (ranking, state)
    return best[1] if best else None


def _next_wake_seconds(
    entries: dict[str, dict[str, Any]], now: float, policy: CookiePolicy
) -> float:
    limit = policy.limit
    window = policy.window
    waits: list[float] = []
    for cookie_id in entries:
        state = _STATES[cookie_id]
        if state.in_use:
            continue
        wait = max(0.0, state.ready_at - now)
        if state.window_count >= limit:
            wait = max(wait, state.window_start + window - now)
        waits.append(wait)
    if not waits:
        # Every jar is leased out; a release notifies us before this expires.
        return 1.0
    return max(min(waits), _WAKE_EPSILON_SECONDS)


def lease_cookie(
    source_key: str,
    *,
    wait_seconds: float | None = None,
    exclude: set[str] | None = None,
) -> CookieLease | None:
    """Reserve one jar for this source, waiting out per-jar limits and delays.

    ``exclude`` skips jars this caller already tried, so a task can walk the whole
    list. Returns ``None`` when nothing is left to try, or when every remaining jar
    stayed rate limited for the whole wait. The caller then stays anonymous.
    """
    source_key = normalize_cookie_source(source_key)
    if not source_key:
        return None
    skip = exclude or set()
    # One settings read per attempt; every limit below comes from this snapshot.
    policy = cookie_policy_for_source(source_key)
    budget = policy.wait if wait_seconds is None else max(0.0, float(wait_seconds))
    deadline = time.monotonic() + budget
    with _CONDITION:
        while True:
            stored = [entry for entry in list_cookies_for_source(source_key) if entry["id"]]
            _sync_states(source_key, {entry["id"]: entry for entry in stored})
            entries = {entry["id"]: entry for entry in stored if entry["id"] not in skip}
            if not entries:
                return None
            now = time.monotonic()
            state = _pick_state(entries, now, policy)
            if state is not None:
                path = materialize_cookie(state.cookie_id)
                if not path:
                    # Row disappeared under us; drop it and re-read the source.
                    _STATES.pop(state.cookie_id, None)
                    continue
                state.in_use = True
                state.window_count += 1
                state.uses += 1
                return CookieLease(
                    cookie_id=state.cookie_id,
                    source_key=source_key,
                    path=path,
                    filename=str(entries[state.cookie_id].get("filename") or "cookies.txt"),
                    policy=policy,
                )
            remaining = deadline - now
            if remaining <= 0:
                return None
            _CONDITION.wait(min(remaining, _next_wake_seconds(entries, now, policy)))


def release_cookie(lease: CookieLease | None, *, banned: bool | None = None) -> None:
    """Return a jar to the pool, resting it before its next turn."""
    if lease is None:
        return
    banned = lease.banned if banned is None else banned
    policy = lease.policy
    with _CONDITION:
        state = _STATES.get(lease.cookie_id)
        if state is not None:
            now = time.monotonic()
            state.in_use = False
            state.used_at = now
            state.ready_at = now + (policy.cooldown if banned else policy.delay)
            if banned:
                # Burn the window too, so a blocked jar is skipped, not just delayed.
                state.window_count = max(state.window_count, policy.limit)
        _CONDITION.notify_all()


def cookie_rotation(source_key: str, *, wait_seconds: float | None = None) -> Iterator[CookieLease]:
    """Yield this source's jars one at a time until the caller finds one that works.

    Each jar is leased for the body of the loop and released on the way out, so a
    failed attempt moves to the next jar and other tasks keep rotating in parallel.
    Set ``lease.banned`` before continuing when the site answered with a block.
    Callers should close the iterator (``contextlib.closing``) when they break early.
    """
    tried: set[str] = set()
    while True:
        lease = lease_cookie(source_key, wait_seconds=wait_seconds, exclude=tried)
        if lease is None:
            return
        tried.add(lease.cookie_id)
        try:
            yield lease
        finally:
            release_cookie(lease)
