from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse

from backend.app.domains.downloads.engines.probe import gallerydl_reads, url_exact_values, ytdlp_single_video
from backend.app.domains.settings.trackers import merge_tracker_tabs, page_words, row_matches, same_label
from backend.app.domains.trackers.browser import BrowserSession
from backend.app.domains.trackers.listing.entries import _on_site, _Resolver, _visit_key
from backend.app.domains.trackers.listing.models import ListingStats


def _parents(link: str) -> list[str]:
    # The links one query field or one path segment shorter, which a tab of that shape extends.
    parsed = urlparse(link)
    if fields := parse_qsl(parsed.query):
        return [parsed._replace(query=urlencode(fields[:i] + fields[i + 1 :])).geturl() for i in range(len(fields))]
    segments = [part for part in parsed.path.split("/") if part]
    return [parsed._replace(path="/" + "/".join(segments[:-1])).geturl()] if len(segments) > 1 else []


def _tab_owner(resolver: _Resolver, links: list[str]) -> _Resolver:
    """The link the given links are tabs of: the tracked link, else one of them naming the same creator that the
    most others extend, as when the tracked link redirects to a page whose menu names it another way."""
    if any(map(resolver.is_tab, links)):
        return resolver
    candidates = dict.fromkeys([*links, *(parent for link in links for parent in _parents(link))])
    owners = [
        _Resolver(link, resolver.source_key)
        for link in candidates
        if _on_site(link, resolver) and url_exact_values(link) & resolver.tracker_tokens
    ]
    counts = [sum(map(owner.is_tab, links)) for owner in owners]
    return owners[counts.index(max(counts))] if counts and max(counts) else resolver


def _tabs(resolver: _Resolver, browser: BrowserSession, *, markup: bool) -> tuple[_Resolver, list[tuple[str, str]]]:
    # The tabs the rendered pages' menus offer with their text, else with ``markup`` every tab the tracked link's
    # markup links, with the link they are tabs of.
    def offers() -> Iterator[list[tuple[str, str]]]:
        menus = [entry for navigation in list(browser.navigation.values()) for entry in navigation]
        yield [(link, text) for link, text, _ in menus]
        # A menu of script tabs is the link's own even when none was followed.
        if markup and all(link for link, _, _ in menus):
            yield [(link, "") for link in resolver.markup_links()]

    for offered in offers():
        owner = _tab_owner(resolver, [link for link, _ in offered])
        if tabs := [(link, text) for link, text in offered if owner.is_tab(link)]:
            return owner, tabs
    return resolver, []


def page_variant(tracker_url: str, link: str) -> tuple[str, str]:
    """The name a page goes by on the tracked link, with the query field holding it: ("", "") for the link
    itself, the value of the one query field it adds, else its last path segment."""
    if _visit_key(link) == _visit_key(tracker_url):
        return "", ""
    tracked = {key for key, _ in parse_qsl(urlparse(tracker_url).query)}
    extra = [(key, value) for key, value in parse_qsl(urlparse(link).query) if key not in tracked]
    if len(extra) == 1:
        return extra[0][1], extra[0][0]
    segments = [part for part in urlparse(link).path.split("/") if part]
    return (unquote(segments[-1]), "") if segments else ("", "")


def _page_url(tracker_url: str, name: str, query_field: str) -> str:
    # A link naming its page by path takes the name as one more segment, one naming it by query as one more
    # field; "" when that field is unknown.
    parsed = urlparse(tracker_url)
    if not name:
        return tracker_url
    if not parsed.query:
        return parsed._replace(path=f"{parsed.path.rstrip('/')}/{quote(name)}").geturl()
    if not query_field:
        return ""
    return parsed._replace(query=urlencode([*parse_qsl(parsed.query), (query_field, name)])).geturl()


def _engine_reads(url: str) -> bool:
    # An engine lists the page itself; a dispatcher matching it only hands out other pages.
    return gallerydl_reads(url) is True or ytdlp_single_video(url) is False


def _read_navigation(browser: BrowserSession, url: str, follow: Callable[[str], bool]) -> None:
    # A page showing no item ends after its first scroll, which reads its navigation.
    list(browser.scroll_links(url, lambda link: False, lambda link: True, follow=follow))


def _follow_tabs(rows: list[dict[str, Any]], browser: BrowserSession, stats: ListingStats) -> Callable[[str], bool]:
    """Which tabs switching a page in script a render clicks for their address: one no rendered menu linked yet,
    that no row goes by or that a ticked row still has to find on this link."""

    def wanted(text: str) -> bool:
        menus = list(browser.navigation.values())
        if any(link and same_label(shown, text) for navigation in menus for link, shown, _ in navigation):
            return False
        named = [row for row in rows if same_label(row["label"], text)]
        return not named or any(row["enabled"] and row["tab"] not in stats.tab_pages for row in named)

    return wanted


def _page_rows(
    url: str,
    rows: list[dict[str, Any]],
    resolver: _Resolver,
    browser: BrowserSession,
    stats: ListingStats,
    markup: bool,
) -> list[dict[str, Any]]:
    """The pages the rendered menus offer, as page rows: the link itself, then its tabs, each marked ``engine``
    when the engines list it. With ``markup``, the link's markup stands in for menus offering no tab. A page a
    row already is keeps that row's mark, so engines are asked only about new ones."""

    def engine(name: str, link: str, text: str) -> bool:
        row = next((row for row in rows if row_matches(row, name, text)), None)
        return name in stats.engine_tabs or (row["engine"] if row else _engine_reads(link))

    owner, tabs = _tabs(resolver, browser, markup=markup)
    own = {_visit_key(page) for page in (url, owner.tracker_url, browser.landed.get(url) or url)}
    menus = [entry for navigation in list(browser.navigation.values()) for entry in navigation]
    label = next((text for link, text, _ in menus if _visit_key(link) in own), "")
    found = {"": {"tab": "", "label": label, "variants": [{"name": "", "field": ""}], "engine": engine("", url, label)}}
    for link, text in tabs:
        name, query_field = page_variant(owner.tracker_url, link)
        row = found.setdefault(
            name,
            {
                "tab": name,
                "label": text,
                "variants": [{"name": name, "field": query_field}],
                "engine": engine(name, link, text),
            },
        )
        row["label"] = row["label"] or text
    return list(found.values())


def _learn_pages(
    url: str,
    rows: list[dict[str, Any]],
    resolver: _Resolver,
    browser: BrowserSession,
    stats: ListingStats,
    markup: bool,
) -> list[dict[str, Any]]:
    """Joins the pages the rendered menus offer to ``rows`` and reports them in ``stats.found_tabs``; returns the
    ones that joined ticked, for the walk to scroll. Nothing is learned before a page rendered, unless ``markup``
    stands in."""
    if not (browser.navigation or markup):
        return []
    stats.found_tabs = _page_rows(url, rows, resolver, browser, stats, markup)
    known = {row["tab"] for row in rows}
    joined = [row for row in merge_tracker_tabs(rows, stats.found_tabs) if row["tab"] not in known]
    rows.extend(joined)
    return [row for row in joined if row["enabled"]]


def _offered_pages(url: str, row: dict[str, Any], resolver: _Resolver, browser: BrowserSession) -> list[str]:
    """Tabs of the tracked link that may be the row's, from any rendered page's navigation and the link's markup,
    closest names first."""
    offered = [(link, text) for navigation in list(browser.navigation.values()) for link, text, _ in navigation]
    offered += [(link, "") for link in resolver.markup_links()]
    owner = _tab_owner(resolver, [link for link, _ in offered])
    matching: dict[str, tuple[int, str]] = {}
    for link, text in offered:
        name = page_variant(owner.tracker_url, link)[0]
        if owner.is_tab(link) and row_matches(row, name, text):
            distance = min(len(page_words(name) ^ page_words(variant["name"])) for variant in row["variants"])
            matching.setdefault(_visit_key(link), (distance, link))
    return [link for _, link in sorted(matching.values())]


def _row_pages(
    url: str,
    row: dict[str, Any],
    resolver: _Resolver,
    browser: BrowserSession | None,
    stats: ListingStats,
    learned: dict[str, list[tuple[str, str]]],
) -> Iterator[str]:
    """Pages of the tracked link that may be the row's, likeliest first: the one it was last, the tabs the
    link offers that resemble it, every name it went by built on the link, then the tabs the menus of the
    pages tried offer. Without a ``browser`` only the pages known without rendering any."""
    if not row["tab"]:
        yield url
        return
    if remembered := stats.tab_pages.get(row["tab"]):
        yield remembered
    if browser:
        yield from _offered_pages(url, row, resolver, browser)
    variants = [*((variant["name"], variant["field"]) for variant in row["variants"]), *learned.get(row["tab"], [])]
    query_fields = [query_field for _, query_field in variants if query_field]
    for name, query_field in variants:
        for candidate_field in dict.fromkeys([query_field, *query_fields]):
            if page := _page_url(url, name, candidate_field):
                yield page
    # The menus of the pages tried may offer it.
    if browser:
        yield from _offered_pages(url, row, resolver, browser)


def _shown_page(url: str, row: dict[str, Any], resolver: _Resolver, browser: BrowserSession, *, shown: bool) -> str:
    """The row's page a candidate turned out to be: the tab its navigation marks as showing, else where it landed
    when it showed items; "" when it was another page. A page no browser rendered is taken as asked."""
    if url not in browser.landed:
        return url
    navigation = browser.navigation.get(url, [])
    owner = _tab_owner(resolver, [link for link, _, _ in navigation]).tracker_url
    marked = [(link, text) for link, text, showing in navigation if showing]
    if marked:
        return next((link for link, text in marked if row_matches(row, page_variant(owner, link)[0], text)), "")
    landed = browser.landed[url] or url
    return url if shown and row_matches(row, page_variant(resolver.tracker_url, landed)[0]) else ""
