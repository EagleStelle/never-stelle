from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from backend.app.core.sources import normalize_source_key

from .storage import load_saved_settings_file


@dataclass(frozen=True)
class CookiePolicy:
    """Per-source rate limits the cookie pool enforces on one jar."""

    # Requests one jar may serve per window before it is parked.
    limit: int = 20
    # Length of that quota window, in seconds.
    window: float = 300.0
    # Seconds a jar rests between two of its own requests.
    delay: float = 5.0
    # Seconds a jar rests after the site answered with a block or rate limit.
    cooldown: float = 900.0
    # Seconds a task waits for a free jar before continuing without one.
    wait: float = 300.0


DEFAULT_COOKIE_POLICY = CookiePolicy()

# field -> (minimum, maximum, whole numbers only)
_POLICY_FIELDS: dict[str, tuple[float, float, bool]] = {
    "limit": (1, 10_000, True),
    "window": (1.0, 86_400.0, False),
    "delay": (0.0, 3_600.0, False),
    "cooldown": (0.0, 86_400.0, False),
    "wait": (0.0, 3_600.0, False),
}

_cache_lock = threading.RLock()
_policies_cache: dict[str, CookiePolicy] | None = None
_default_policy_cache: CookiePolicy | None = None


def _clamped(raw: Any, minimum: float, maximum: float, whole: bool) -> float | int | None:
    """A usable number inside its range, or ``None`` when the field is unset."""
    if raw is None or isinstance(raw, bool):
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    value = max(minimum, min(value, maximum))
    return int(value) if whole else value


def normalize_cookie_policy(raw: Any) -> dict[str, Any]:
    """One source's overrides. Unset fields are dropped so defaults still apply."""
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, Any] = {}
    for field, (minimum, maximum, whole) in _POLICY_FIELDS.items():
        value = _clamped(source.get(field), minimum, maximum, whole)
        if value is not None:
            out[field] = value
    return out


def normalize_source_cookie_policies(raw: Any) -> dict[str, dict[str, Any]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for raw_key, raw_policy in source.items():
        key = normalize_source_key(raw_key)
        policy = normalize_cookie_policy(raw_policy)
        if key and policy:
            out[key] = policy
    return out


def get_effective_cookie_policies(payload: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_source_cookie_policies(payload.get("source_cookie_policies"))


def normalize_default_cookie_policy(raw: Any) -> dict[str, Any]:
    """The global overrides. Unset fields are dropped so the built-ins still apply."""
    return normalize_cookie_policy(raw)


def builtin_cookie_policy_defaults() -> dict[str, Any]:
    return {
        "limit": DEFAULT_COOKIE_POLICY.limit,
        "window": DEFAULT_COOKIE_POLICY.window,
        "delay": DEFAULT_COOKIE_POLICY.delay,
        "cooldown": DEFAULT_COOKIE_POLICY.cooldown,
        "wait": DEFAULT_COOKIE_POLICY.wait,
    }


def cookie_policy_defaults(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """What a source with no overrides of its own gets."""
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return {
        **builtin_cookie_policy_defaults(),
        **normalize_default_cookie_policy(payload.get("default_cookie_policy")),
    }


def invalidate_cookie_policies() -> None:
    """Drop the resolved cache. Called from the one settings write path."""
    global _policies_cache, _default_policy_cache
    with _cache_lock:
        _policies_cache = None
        _default_policy_cache = None


def _resolved_policies() -> tuple[CookiePolicy, dict[str, CookiePolicy]]:
    # Settings have a single writer, which invalidates this, so a hit is always
    # current. The pool resolves a policy once per attempt and reads no further.
    global _policies_cache, _default_policy_cache
    with _cache_lock:
        if _policies_cache is None or _default_policy_cache is None:
            payload = load_saved_settings_file()
            defaults = cookie_policy_defaults(payload)
            _default_policy_cache = CookiePolicy(**defaults)
            # A source overrides single fields; the rest come from the global defaults.
            _policies_cache = {
                key: CookiePolicy(**{**defaults, **policy})
                for key, policy in get_effective_cookie_policies(payload).items()
            }
        return _default_policy_cache, _policies_cache


def cookie_policy_for_source(source_key: Any) -> CookiePolicy:
    default_policy, policies = _resolved_policies()
    return policies.get(normalize_source_key(source_key), default_policy)
