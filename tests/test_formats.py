from __future__ import annotations

import pytest

import backend.app.db.repositories as repositories
import backend.app.domains.downloads.workers.completion.samples as learning_module
from backend.app.domains.formats.analysis import (
    creator_from_url,
    extract_url_part,
    media_id_from_url,
    url_dedup_key,
)
from backend.app.domains.formats.learning import (
    conflicts_with_source,
    describe_learned_segments,
    guess_sources,
    learn_download,
    learn_formats,
    learn_media_id,
    reconstruct_url,
    reconstruct_url_candidates,
)
from backend.app.domains.formats.matching import (
    format_covers,
    learned_templates_for,
    match_template,
    select_for_format,
)
from tests.support import learned_youtube_twitter


def _learn_posts(urls: list[str], metadata: dict | None) -> list[str]:
    learned: dict = {}
    for url in urls:
        learned = learn_download(learned, url, media_id_from_url(url), metadata)
    return learned["example"]["templates"]


def test_learn_download_derives_url_template():
    assert learned_youtube_twitter()["youtube"]["templates"][0] == "https://www.youtube.com/watch?v={id}"


def test_learn_download_generalizes_repeated_format_handle_to_var():
    learned = learn_download({}, "https://twitter.com/DemoVT/status/2000000000000000001", "2000000000000000001")
    learned = learn_download(learned, "https://twitter.com/Other/status/1111111111111111111", "1111111111111111111")
    assert learned["twitter"]["templates"][0] == "https://twitter.com/{var}/status/{id}"


def test_learn_download_marks_metadata_proven_creator_segment():
    learned = learn_download(
        {},
        "https://twitter.com/DemoVT/status/2000000000000000001",
        "2000000000000000001",
        {"uploader": "DemoVT"},
    )

    assert learned["twitter"]["templates"][0] == "https://twitter.com/{creator}/status/{id}"


def test_learn_download_marks_exact_username_or_nickname_segment():
    username = learn_download(
        {},
        "https://www.facebook.com/DemoPerson/posts/pfbid02MockPostMockPostMockPostMockPostMockPostMockPostMockPostMockPostM",
        "pfbid02MockPostMockPostMockPostMockPostMockPostMockPostMockPostMockPostM",
        {"uploader_id": "DemoPerson"},
    )
    nickname = learn_download(
        {},
        "https://www.facebook.com/DemoPerson/posts/pfbid02MockPostMockPostMockPostMockPostMockPostMockPostMockPostMockPostM",
        "pfbid02MockPostMockPostMockPostMockPostMockPostMockPostMockPostMockPostM",
        {"display_name": "DemoPerson"},
    )

    assert username["facebook"]["templates"][0] == "https://www.facebook.com/{username}/posts/{id}"
    assert nickname["facebook"]["templates"][0] == "https://www.facebook.com/{nickname}/posts/{id}"


def test_learn_download_trims_seo_query_and_keeps_handle_literal_without_metadata():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001?lang=en&q=fakeacc&t=1781279478413",
        "7100000000000000001",
    )

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@fakeacc.com/video/{id}"
    assert (
        reconstruct_url(learned, "tiktok", "7100000000000000001")
        == "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001"
    )


def test_learn_download_keeps_multiple_templates_per_source():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001",
        "7100000000000000001",
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002",
        "7100000000000000002",
    )

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@fakeacc.com/video/{id}"
    assert set(learned["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@fakeacc.com/video/{id}",
        "https://www.tiktok.com/@fakeacc.com/photo/{id}",
    }


def test_describe_learned_segments_keeps_shared_url_creator_generic():
    learned = learn_download(
        {},
        "https://www.tiktok.com/@demo0n/video/7100000000000000005",
        "7100000000000000005",
        {"uploader": "demo0n"},
    )

    described = describe_learned_segments(learned["tiktok"])

    assert learned["tiktok"]["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert described["templates"][0] == "https://www.tiktok.com/@{creator}/video/{id}"
    assert described["segments"][0]["label"] == "{creator}"


def test_reconstruct_url_candidates_returns_every_learned_route():
    # Both routes come out as concrete candidates; a probe (not a heuristic) picks the real one.
    learned = learn_download(
        {},
        "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001",
        "7100000000000000001",
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002",
        "7100000000000000002",
    )

    candidates = reconstruct_url_candidates(learned, "tiktok", "7100000000000000002", creator="fakeacc.com")
    assert set(candidates) == {
        "https://www.tiktok.com/@fakeacc.com/video/7100000000000000002",
        "https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002",
    }


def test_format_sample_reads_the_id_from_the_filename():
    url = "https://example.test/@alice/photo/7100000000000000002"

    assert learning_module._format_sample(url, "alice - [7100000000000000002].jpg") == (
        url,
        "7100000000000000002",
        None,
    )


def test_learn_formats_reports_only_a_new_template():
    # Only a new format is worth a deferred field probe.
    assert learn_formats([("https://example.test/@alice/video/7100000000000000002", "7100000000000000002", None, {})])
    assert not learn_formats(
        [("https://example.test/@alice/video/7420705673542978834", "7420705673542978834", None, {})]
    )
    assert repositories.load_learned_formats_payload()["example"]["samples"] == 2


def test_learn_download_keeps_descriptive_segment_literal_for_single_sample():
    # URL parts are not promoted to a built-in {{slug}} token: a lone descriptive
    # segment stays literal until a configured URL-part value overrides it.
    learned = learn_download(
        {},
        "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/",
        "4483553",
    )

    assert learned["rule34video"]["templates"][0] == "https://rule34video.com/video/{id}/daiwa-scarlet-suokanawer"
    assert (
        reconstruct_url(learned, "rule34video", "3238394")
        == "https://rule34video.com/video/3238394/daiwa-scarlet-suokanawer"
    )
    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds-minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_reconstruct_url_replaces_literal_segment_with_configured_url_part_value():
    learned = {
        "rule34video": {
            "templates": ["https://rule34video.com/video/{id}/cocolia-rand-sutekimeppou"],
        }
    }

    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds - minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_reconstruct_url_candidates_needs_url_part_value_for_generalized_var():
    learned = learn_download({}, "https://www.tiktok.com/@fakeacc.com/video/7100000000000000001", "7100000000000000001")
    learned = learn_download(learned, "https://www.tiktok.com/@other/video/7100000000000000002", "7100000000000000002")
    assert reconstruct_url_candidates(learned, "tiktok", "123") == []
    assert reconstruct_url_candidates(learned, "tiktok", "") == []
    assert reconstruct_url_candidates(
        learned,
        "tiktok",
        "123",
        slug_values={"path:0": "fakeacc.com"},
    ) == ["https://www.tiktok.com/@fakeacc.com/video/123"]


def test_creator_from_url_uses_handle_segment_without_at_sign():
    assert creator_from_url("https://www.tiktok.com/@fakeacc.com/video/7100000000000000001") == "fakeacc.com"
    assert (
        creator_from_url("https://www.tiktok.com/@fakeacc.com/video/7100000000000000001", strip_at=False)
        == "@fakeacc.com"
    )
    assert creator_from_url("https://x.com/DEMOinARTIST/status/2000000000000000002") == "DEMOinARTIST"
    assert creator_from_url("https://www.facebook.com/share/p/1bDemoBb2c/") == ""


def test_extract_url_part_reads_configured_path_segment():
    # A configured URL part reads its value straight from the canonical URL.
    url = "https://rule34video.com/video/3056158/84-minus8/"
    assert media_id_from_url(url) == "3056158"
    assert extract_url_part(url, "path:2") == "84-minus8"
    assert extract_url_part(url, "path:0") == "video"
    assert extract_url_part(url, "path:9") == ""


def test_extract_url_part_reads_query_value():
    url = "https://example.com/watch?v=YtDemoVid04&list=PL123"
    assert extract_url_part(url, "query:list") == "PL123"
    assert extract_url_part(url, "query:missing") == ""


def test_describe_learned_segments_marks_id_reserved_and_url_part_selectable():
    learned = learn_download(
        {},
        "https://rule34video.com/video/4483553/daiwa-scarlet-suokanawer/",
        "4483553",
    )
    described = describe_learned_segments(learned["rule34video"])
    parts = {seg["part"]: seg for seg in described["segments"]}
    assert parts["path:1"]["kind"] == "id" and parts["path:1"]["reserved"] is True
    # The descriptive segment is selectable so the user can name a URL-part token for it.
    assert parts["path:2"]["reserved"] is False
    assert parts["path:2"]["label"] == "daiwa-scarlet-suokanawer"
    # A constant route word is not a useful token, so it stays reserved.
    assert parts["path:0"]["label"] == "video" and parts["path:0"]["reserved"] is True


def test_learn_download_merges_same_pattern_url_part_to_var():
    # Two downloads of the same route differing only in the descriptive URL part generalize
    # to a single {var} template instead of piling up near-duplicate literals.
    learned = learn_download(
        {},
        "https://rule34video.com/video/4497669/cleaning-the-base/",
        "4497669",
    )
    learned = learn_download(
        learned,
        "https://rule34video.com/video/4499077/charlie-s-late-christmas-special-chaosarts/",
        "4499077",
    )
    assert learned["rule34video"]["templates"] == ["https://rule34video.com/video/{id}/{var}"]


def test_learn_download_third_same_pattern_is_idempotent():
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    learned = learn_download(learned, "https://rule34video.com/video/4500123/another-title-here/", "4500123")
    assert learned["rule34video"]["templates"] == ["https://rule34video.com/video/{id}/{var}"]
    assert learned["rule34video"]["samples"] == 3


def test_describe_marks_var_selectable_after_merge():
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    parts = {seg["part"]: seg for seg in describe_learned_segments(learned["rule34video"])["segments"]}
    assert parts["path:0"]["reserved"] is True
    assert parts["path:1"]["kind"] == "id" and parts["path:1"]["reserved"] is True
    assert parts["path:2"]["kind"] == "var" and parts["path:2"]["reserved"] is False


def test_reconstruct_after_merge_needs_url_part_value():
    # Once a URL part generalizes to {var} the link can't be rebuilt from the id alone;
    # a configured URL-part value fills the position, otherwise there is no candidate.
    learned = learn_download({}, "https://rule34video.com/video/4497669/cleaning-the-base/", "4497669")
    learned = learn_download(learned, "https://rule34video.com/video/4499077/charlie-special/", "4499077")
    assert reconstruct_url(learned, "rule34video", "3238394") == ""
    assert (
        reconstruct_url(learned, "rule34video", "3238394", slug_values={"path:2": "wsds-minus8"})
        == "https://rule34video.com/video/3238394/wsds-minus8"
    )


def test_learn_download_keeps_distinct_route_words_unmerged():
    # Differing route words (video vs photo) are different routes, not a slug, so both
    # templates survive for reconstruction instead of collapsing to {var}.
    learned = learn_download(
        {},
        "https://www.tiktok.com/@a/video/7100000000000000001",
        "7100000000000000001",
        {"uploader": "a"},
    )
    learned = learn_download(
        learned,
        "https://www.tiktok.com/@a/photo/7100000000000000002",
        "7100000000000000002",
        {"uploader": "a"},
    )
    assert set(learned["tiktok"]["templates"]) == {
        "https://www.tiktok.com/@{creator}/video/{id}",
        "https://www.tiktok.com/@{creator}/photo/{id}",
    }


def test_learn_download_generalizes_one_creators_handle_from_metadata():
    # Two posts of one creator never differ at the handle; the metadata still proves it varies.
    posts = ["https://example.test/alice/post/22222222", "https://example.test/alice/post/33333333"]

    assert _learn_posts(posts, {"author[uniqueId]": "alice"}) == ["https://example.test/{username}/post/{id}"]
    # Without metadata the handle stays literal until another creator's post differs there.
    assert _learn_posts(posts, None) == ["https://example.test/alice/post/{id}"]


def test_learn_download_marks_a_loosely_matching_name_variable():
    # The handle spells the display name, but not exactly enough to fill it back in.
    templates = _learn_posts(["https://example.test/alice-chan/post/22222222"], {"author[nickname]": "Alice Chan"})

    assert templates == ["https://example.test/{var}/post/{id}"]


def test_learn_download_marks_an_album_from_one_post_variable():
    templates = _learn_posts(
        ["https://example.test/alice/albums/summer-2024/22222222"],
        {"author[uniqueId]": "alice", "album[name]": "Summer 2024"},
    )

    assert templates == ["https://example.test/{username}/albums/{var}/{id}"]


def test_learn_download_keeps_route_words_and_link_echoes_literal():
    templates = _learn_posts(
        ["https://example.test/gallery/22222222/clip_of_the_day"],
        {"subcategory": "gallery", "webpage_url_basename": "clip_of_the_day"},
    )

    assert templates == ["https://example.test/gallery/{id}/clip_of_the_day"]


def test_learn_download_heals_a_literal_handle_once_metadata_proves_it():
    learned = learn_download({}, "https://example.test/alice/post/22222222", "22222222")
    learned = learn_download(
        learned, "https://example.test/alice/post/33333333", "33333333", {"author[uniqueId]": "alice"}
    )

    assert learned["example"]["templates"] == ["https://example.test/{creator}/post/{id}"]


def test_learn_download_binds_the_fields_the_caller_names():
    # A tracker names the field it found holding the creator, outside the default chains.
    learned = learn_download(
        {},
        "https://example.test/alice/post/22222222",
        "22222222",
        {"author[handle]": "alice"},
        {"username": ["author[handle]"]},
    )

    assert learned["example"]["templates"] == ["https://example.test/{username}/post/{id}"]


def test_learn_download_joins_query_values_by_key_and_drops_optional_ones():
    # A playlist or share parameter only some links carry is not part of the item's format.
    templates = _learn_posts(
        [
            "https://example.test/watch?v=AAAAAAAAAAA&list=PLxxxxxxxxxxxxxxxxxxxx",
            "https://example.test/watch?v=BBBBBBBBBBB&feed=FDyyyyyyyyyyyyyyyyyyyy",
        ],
        None,
    )

    assert templates == ["https://example.test/watch?v={id}"]


def test_learn_download_marks_an_identifier_beside_the_id_variable():
    # A second identifier names the owner or parent, so one creator's posts do not freeze it.
    templates = _learn_posts(["https://example.test/permalink.php?story_fbid=22222222&id=11111111111111"], None)

    assert templates == ["https://example.test/permalink.php?story_fbid={id}&id={var}"]


def test_stored_formats_are_repaired_when_read():
    learned = {
        "example": {
            "templates": [
                "https://example.test/photo?fbid={id}",
                "https://example.test/{var}/posts/{id}",
                "https://example.test/permalink.php?story_fbid=pfbid0AAAAAAAAAAAAAAAAAAAAAAAA1"
                "&id=11111111111111&comment_id={id}",
                "https://example.test/photo?fbid={var}&set={id}",
                "https://example.test/permalink.php?story_fbid={id}&id=22222222222222",
                "https://example.test/watch?v={id}",
                "https://example.test/watch?v={id}&{var}={var}",
                "https://example.test/video/{id}/{id}",
            ]
        }
    }

    assert learned_templates_for(learned, "example") == [
        "https://example.test/photo?fbid={id}",
        "https://example.test/{var}/posts/{id}",
        "https://example.test/permalink.php?story_fbid={id}&id={var}",
        "https://example.test/watch?v={id}",
    ]


def test_a_link_with_extra_parameters_matches_its_format():
    learned = {"example": {"templates": ["https://example.test/watch?v={id}"]}}
    url = "https://example.test/watch?v=AAAAAAAAAAA&list=PLxxxxxxxxxxxxxxxxxxxx"

    assert match_template(learned, "example", url) == "https://example.test/watch?v={id}"


def test_a_setting_saved_on_a_format_follows_it_once_generalized():
    saved = {"https://example.test/alice/post/{id}": "alice-folder"}

    assert select_for_format(saved, "https://example.test/{creator}/post/{id}") == "alice-folder"
    assert select_for_format(saved, "https://example.test/{creator}/photo/{id}") is None
    # A key saved on a broken format follows the one it was repaired into.
    broken = {"https://example.test/photo?fbid={var}&set={id}": "photos"}
    assert select_for_format(broken, "https://example.test/photo?fbid={id}") == "photos"
    # A route word is never absorbed by a token.
    assert not format_covers("https://example.test/{creator}/{id}", "https://example.test/video/{id}")


def test_learn_download_ignores_unknown_host():
    assert learn_download({}, "not a url", "abc123") == {}


def test_url_dedup_key_ignores_route_so_reposts_dedup():
    # The reported bug: a video reconsolidated to /photo must still dedup its /video link.
    photo = url_dedup_key("https://www.tiktok.com/@fakeacc.com/photo/7100000000000000004")
    video = url_dedup_key("https://www.tiktok.com/@fakeacc.com/video/7100000000000000004")
    assert photo == video == "tiktok#7100000000000000004"


def test_url_dedup_key_separates_different_posts():
    a = url_dedup_key("https://www.tiktok.com/@fakeacc.com/photo/7100000000000000004")
    b = url_dedup_key("https://www.tiktok.com/@fakeacc.com/photo/7100000000000000002")
    assert a != b


def test_url_dedup_key_reads_the_item_not_the_set_it_was_opened_in():
    album = "set=pb.61111111111111.-2222222222"
    first = url_dedup_key(f"https://example.test/photo/?{album}&fbid=111111111111111111")
    second = url_dedup_key(f"https://example.test/photo/?fbid=222222222222222222&{album}")
    assert first != second
    assert first == url_dedup_key("https://example.test/photo?fbid=111111111111111111")


def test_url_dedup_key_reads_the_first_named_id_not_its_owner_or_a_comment():
    owner = "id=61111111111111"
    first = url_dedup_key(f"https://example.test/permalink.php?story_fbid=pfbid0abc123XYZ&{owner}")
    second = url_dedup_key(f"https://example.test/permalink.php?story_fbid=pfbid0def456UVW&{owner}")
    assert (first, second) == ("example#pfbid0abc123XYZ", "example#pfbid0def456UVW")
    commented = url_dedup_key("https://example.test/photo/?fbid=111111111111111&set=a.2222&comment_id=3333333333333333")
    assert commented == "example#111111111111111"
    assert url_dedup_key("https://example.test/watch?list=PL0123456789abcdefghijklmnop&v=YtDemoVid04") == (
        "example#YtDemoVid04"
    )


def test_media_id_from_url_reads_id_without_prior_knowledge():
    assert media_id_from_url("https://www.tiktok.com/@a/video/7100000000000000004") == "7100000000000000004"
    assert media_id_from_url("https://www.youtube.com/watch?v=YtDemoVid04") == "YtDemoVid04"


def test_guess_sources_uses_learned_signatures():
    learned = learned_youtube_twitter()
    assert guess_sources(learned, "kZ0vN9pLm-Q") == ["youtube"]
    assert guess_sources(learned, "2000000000000000001") == ["twitter"]


@pytest.mark.parametrize(
    "media_id,source_key,expected",
    [
        ("2000000000000000001", "youtube", True),
        ("2000000000000000001", "twitter", False),
        ("kZ0vN9pLm-Q", "youtube", False),
        ("kZ0vN9pLm-Q", "twitter", True),
        ("anything", "unlearnedsite", False),
    ],
)
def test_conflicts_with_source(media_id, source_key, expected):
    assert conflicts_with_source(learned_youtube_twitter(), source_key, media_id) is expected


@pytest.mark.parametrize(
    "source_key,media_id,expected",
    [
        ("youtube", "newid1234567", "https://www.youtube.com/watch?v=newid1234567"),
        ("tiktok", "123", ""),
        ("youtube", "", ""),
    ],
)
def test_reconstruct_url_from_learned(source_key, media_id, expected):
    assert reconstruct_url(learned_youtube_twitter(), source_key, media_id) == expected


def test_guess_sources_tolerates_base64url_separators():
    # Learned only from ids without "-"/"_"; new youtube ids carrying them must still match.
    learned = learn_download({}, "https://www.youtube.com/watch?v=YtDemoVid04", "YtDemoVid04")
    assert guess_sources(learned, "K1-BVtsHrOY") == ["youtube"]
    assert guess_sources(learned, "_F5vcIlr9bs") == ["youtube"]


def test_learn_media_id_seeds_shape_for_confirmed_source():
    learned = learn_media_id({}, "youtube", "YtDemoVid04")
    assert learned["youtube"]["id_min"] == 11
    assert learned["youtube"]["id_max"] == 11
    assert guess_sources(learned, "_F5vcIlr9bs") == ["youtube"]


def test_learn_media_id_ignores_removed_and_empty_source():
    assert learn_media_id({}, "others", "YtDemoVid04") == {}
    assert learn_media_id({}, "youtube", "") == {}
