from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, TypeVar

T = TypeVar("T")

# One operation (an API request, a download task, a library scan) resolves the
# same derived settings over and over: the app config, the saved settings row,
# the effective source profiles. Each resolution is pure for the duration of that
# operation, so it is computed once and reused instead of rebuilt per call site.
_scope: ContextVar[dict[str, Any] | None] = ContextVar("resolution_scope", default=None)


@contextmanager
def resolution_scope() -> Iterator[dict[str, Any]]:
    """Memoize derived settings for the length of one operation.

    Nesting reuses the outer scope, so a scan started inside a request shares the
    request's resolutions instead of opening a second cache. Outside any scope the
    helpers below stay pass-through, which keeps direct calls (and tests that stub
    the loaders) on the uncached path.
    """
    existing = _scope.get()
    if existing is not None:
        yield existing
        return
    store: dict[str, Any] = {}
    token = _scope.set(store)
    try:
        yield store
    finally:
        _scope.reset(token)


def resolved(key: str, factory: Callable[[], T]) -> T:
    """Return ``factory()``, computed once per key inside the active scope."""
    store = _scope.get()
    if store is None:
        return factory()
    if key not in store:
        store[key] = factory()
    return store[key]


def is_scoped(key: str, value: Any) -> bool:
    """True when ``value`` is ``None`` or is exactly what this scope resolved for ``key``.

    Callers routinely pass a value they loaded a line earlier — the app config, the
    saved settings row. Inside a scope that is the very object the memo holds, so
    anything derived from it is identical to the no-argument form and the memo
    applies. Checking identity never triggers a load: an unresolved key simply
    falls through to a rebuild.
    """
    if value is None:
        return True
    store = _scope.get()
    return store is not None and store.get(key) is value


def invalidate(*keys: str) -> None:
    """Drop resolutions whose key starts with any of ``keys`` from the active scope."""
    store = _scope.get()
    if not store:
        return
    for cached in [cached for cached in store if any(cached.startswith(key) for key in keys)]:
        store.pop(cached, None)


def scope_active() -> bool:
    return _scope.get() is not None
