"""Translation correctness: popfeed records -> NeoDB-compatible ActivityPub."""

from __future__ import annotations

import pytest
from skybridge.db import session_scope
from skybridge.models import Work
from skybridge.translate import neodb, works
from sqlalchemy import func, select

POST = {
    "$type": "social.popfeed.feed.post",
    "text": "Superman - Official Trailer\nhttps://www.ign.com/videos/superman",
    "title": "Superman - Official Trailer - IGN",
    "facets": [
        {
            "index": {"byteStart": 28, "byteEnd": 51},
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": "https://www.ign.com/videos/superman",
                }
            ],
        }
    ],
    "createdAt": {},
    "identifiers": {"imdbId": "tt5950044", "tmdbId": "1061474"},
    "creativeWorkType": "movie",
}

LIST = {
    "$type": "social.popfeed.feed.list",
    "name": "2025 The Game Awards Nominees",
    "description": "Games nominated in 2025",
    "tags": ["Gaming"],
    "listType": "default",
    "createdAt": "2025-11-17T18:09:54.291Z",
}

LIST_ITEM = {
    "$type": "social.popfeed.feed.listItem",
    "title": "Elden Ring",
    "listType": "complete",
    "posterUrl": "https://images.igdb.com/x.jpg",
    "identifiers": {"igdbId": "119133"},
    "addedAt": "2025-11-17T23:08:47.376Z",
    "creativeWorkType": "video_game",
}

REVIEW = {
    "$type": "social.popfeed.feed.review",
    "title": "Everything Everywhere All at Once",
    "text": "Mind-bending and heartfelt.",
    "facets": [],
    "tags": ["a24"],
    "rating": 9,
    "isRevisit": False,
    "containsSpoilers": True,
    "createdAt": "2026-07-03T17:16:24.038Z",
    "posterUrl": "https://cdn.example/poster.jpg",
    "identifiers": {"imdbId": "tt6710474", "tmdbId": "545611"},
    "creativeWorkType": "movie",
}


def test_legacy_post_collection_has_no_mapping(settings):
    # feed.post is legacy (superseded by feed.review) and no longer bridged.
    ref = works.work_ref(POST)
    with pytest.raises(ValueError, match="no AP mapping"):
        neodb.translate(
            did="did:plc:abc",
            handle="alice.test",
            collection="social.popfeed.feed.post",
            rkey="r1",
            record=POST,
            operation="create",
            time_us=None,
            ref=ref,
        )


def test_review_facets_render_as_links(settings):
    record = {
        **REVIEW,
        "text": "watch this https://www.ign.com/videos/superman now",
        "facets": [
            {
                "index": {"byteStart": 11, "byteEnd": 46},
                "features": [
                    {
                        "$type": "app.bsky.richtext.facet#link",
                        "uri": "https://www.ign.com/videos/superman",
                    }
                ],
            }
        ],
    }
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.review",
        rkey="rv9",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert '<a href="https://www.ign.com/videos/superman"' in note["content"]


def test_list_becomes_shelf(settings):
    note, _activity = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.list",
        rkey="l1",
        record=LIST,
        operation="create",
        time_us=None,
        ref=None,
    )
    assert note is not None
    shelves = [r for r in note["relatedWith"] if r["type"] == "Shelf"]
    assert shelves and shelves[0]["name"] == LIST["name"]
    assert any(t["type"] == "Hashtag" for t in note.get("tag", []))


def test_list_item_status_mark(settings):
    ref = works.work_ref(LIST_ITEM)
    assert ref is not None
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i1",
        record=LIST_ITEM,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert statuses and statuses[0]["status"] == "complete"
    assert statuses[0]["withRegardTo"] == ref.url
    # poster surfaced as an attachment
    assert note["attachment"][0]["url"] == LIST_ITEM["posterUrl"]


def test_review_becomes_rating_and_comment(settings):
    ref = works.work_ref(REVIEW)
    assert ref is not None
    note, activity = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.review",
        rkey="rv1",
        record=REVIEW,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert note["published"] == REVIEW["createdAt"]
    ratings = [r for r in note["relatedWith"] if r["type"] == "Rating"]
    assert ratings == [
        {"type": "Rating", "withRegardTo": ref.url, "value": 9, "best": 10, "worst": 1}
    ]
    # review text is an untitled Comment on the mark — never a Review/Article
    comments = [r for r in note["relatedWith"] if r["type"] == "Comment"]
    assert comments and comments[0]["withRegardTo"] == ref.url
    assert comments[0]["content"] == note["content"]
    assert not any(r["type"] == "Review" for r in note["relatedWith"])
    # spoilers become a content warning
    assert note["sensitive"] is True
    assert "Spoilers" in note["summary"]
    # record tags + work link + category hashtag
    assert {"type": "Hashtag", "name": "#a24"} in note["tag"]
    assert {"type": "Hashtag", "name": "#movie"} in note["tag"]
    assert activity["type"] == "Create"


def test_rating_only_review_has_no_review_object(settings):
    record = {**REVIEW, "text": "", "containsSpoilers": False}
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.review",
        rkey="rv2",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert "Rated" in note["content"] and "9/10" in note["content"]
    kinds = {r["type"] for r in note["relatedWith"]}
    assert kinds == {"Rating"}
    assert "sensitive" not in note


def test_compound_list_type_maps_to_status(settings):
    record = {**LIST_ITEM, "listType": "watched_movies"}
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i2",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert statuses and statuses[0]["status"] == "complete"


def test_update_and_delete_activities(settings):
    _, upd = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.review",
        rkey="r1",
        record=REVIEW,
        operation="update",
        time_us=None,
        ref=works.work_ref(REVIEW),
    )
    assert upd["type"] == "Update"

    note, dele = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.review",
        rkey="r1",
        record=None,
        operation="delete",
        time_us=None,
        prior_object_id=settings.post_id("alice.test", "r1"),
    )
    assert note is None
    assert dele["type"] == "Delete"
    assert dele["object"]["type"] == "Tombstone"
    assert dele["object"]["id"] == settings.post_id("alice.test", "r1")


def test_work_ref_namespaces_identifier(settings):
    ref = works.work_ref(POST)
    assert ref is not None
    assert ref.work_type == "movie"
    assert ref.work_id == "imdbId-tt5950044"  # priority picks imdb first
    assert ref.url == settings.catalog_id("movie", "imdbId-tt5950044")


@pytest.mark.parametrize(
    ("list_type", "status"),
    [
        # observed popfeed system lists
        ("watched_movies", "complete"),
        ("watched_tv_shows", "complete"),
        ("played_video_games", "complete"),
        ("read_books", "complete"),
        ("listened_albums_and_eps", "complete"),
        ("movie_watchlist", "wishlist"),
        # do / doing / done / dropped verb forms
        ("currently_reading", "progress"),
        ("listening", "progress"),
        ("game_backlog", "wishlist"),
        ("books_to_read", "wishlist"),  # "to_<verb>" is a want-list
        ("to_read", "wishlist"),
        ("dropped_shows", "dropped"),
        ("abandoned", "dropped"),
        # no status -> plain membership
        ("default", None),
        ("favorites", None),
        ("", None),
    ],
)
def test_shelf_status_matrix(list_type, status):
    assert neodb.shelf_status(list_type) == status


def test_album_maps_to_music_category():
    assert works.category_for("album") == "music"
    assert works.category_for("music") == "music"
    assert works.category_for("ep") == "music"
    assert works.category_for("tv_season") == "tv"
    assert works.category_for("unknown_type") == "item"


def test_identifier_priority_prefers_canonical_forms(settings):
    book = {"identifiers": {"isbn10": "0441013597", "isbn13": "9780441013593"}}
    ref = works.work_ref({**book, "creativeWorkType": "book"})
    assert ref is not None and ref.work_id == "isbn13-9780441013593"

    album = {
        "identifiers": {
            "mbReleaseId": "009d8137-38e1-42a3-ab4c-5ef5e942aea8",
            "mbId": "7d5a684e-73a3-325c-a59c-34c9a941d8d6",
        }
    }
    ref = works.work_ref({**album, "creativeWorkType": "album"})
    assert ref is not None and ref.work_id == "mbId-7d5a684e-73a3-325c-a59c-34c9a941d8d6"

    game = {
        "identifiers": {
            "slug": "disco-elysium-the-final-cut",
            "atUri": "at://did:web:x/games.x.game/1",
            "igdbId": "141540",
            "steamId": "632470",
        }
    }
    ref = works.work_ref({**game, "creativeWorkType": "video_game"})
    assert ref is not None and ref.work_id == "igdbId-141540"


def test_mint_merges_works_sharing_identifiers(settings):
    # A review carrying imdb+tmdb ids, then a listItem with only the tmdb id
    # (as seen in real popfeed data) must resolve to one catalog work.
    review = {
        "title": "Fleabag",
        "identifiers": {"imdbId": "tt5687612", "tmdbId": "67070"},
        "creativeWorkType": "tv_show",
    }
    item = {"title": "Fleabag", "identifiers": {"tmdbId": "67070"}, "creativeWorkType": "tv_show"}
    ref1 = works.mint(review)
    ref2 = works.mint(item)
    assert ref1 is not None and ref2 is not None
    assert ref1.work_key == ref2.work_key == "tv_show:imdbId-tt5687612"
    assert ref2.url == ref1.url

    # Reverse arrival order merges too: the poorer record mints first, the
    # richer one folds into it and registers the extra identifier as an alias.
    works.mint({"identifiers": {"tmdbId": "545611"}, "creativeWorkType": "movie"})
    ref3 = works.mint(
        {"identifiers": {"imdbId": "tt6710474", "tmdbId": "545611"}, "creativeWorkType": "movie"}
    )
    assert ref3 is not None and ref3.work_key == "movie:tmdbId-545611"

    # Same tmdb id under a different media type stays a distinct work.
    ref4 = works.mint({"identifiers": {"tmdbId": "67070"}, "creativeWorkType": "movie"})
    assert ref4 is not None and ref4.work_key == "movie:tmdbId-67070"

    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(Work)) == 3


def test_render_facets_plain_text():
    html = neodb.render_facets("just text & <b>", None)
    assert html == "<p>just text &amp; &lt;b&gt;</p>"
