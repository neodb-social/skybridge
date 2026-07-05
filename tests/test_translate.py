"""Translation correctness: popfeed records -> NeoDB-compatible ActivityPub."""

from __future__ import annotations

import json
from collections.abc import Mapping

import pytest
from skybridge.atproto import identity
from skybridge.db import session_scope
from skybridge.models import Record, Work
from skybridge.translate import neodb, works
from sqlalchemy import func, select


@pytest.fixture(autouse=True)
def _clear_list_fetch_failed():
    # _LIST_FETCH_FAILED is a module-level (process-lifetime) negative cache,
    # not DB state, so it must be reset per test even though the DB itself is
    # reinitialised by the `_db` autouse fixture in conftest.py.
    neodb._LIST_FETCH_FAILED.clear()
    yield
    neodb._LIST_FETCH_FAILED.clear()


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
    # the content links the work's catalog page; the poster rides on the
    # catalog-item tag, never as a direct media attachment
    assert f'<a href="{ref.url}">Elden Ring</a>' in note["content"]
    assert "attachment" not in note
    # listItem notes never carry a name (only a "to <list>" content line)
    assert "name" not in note


def test_list_item_content_uses_archived_list_name(settings):
    list_uri = "at://did:plc:abc/social.popfeed.feed.list/l1"
    with session_scope() as session:
        session.add(
            Record(
                at_uri=list_uri,
                did="did:plc:abc",
                collection="social.popfeed.feed.list",
                rkey="l1",
                source_json=json.dumps({"name": "Best games of 2024", "description": "..."}),
            )
        )
    record = {**LIST_ITEM, "listUri": list_uri}
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i1",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert "Best games of 2024" in note["content"]
    assert "name" not in note


def test_list_item_content_uses_description_when_list_unnamed(settings):
    list_uri = "at://did:plc:abc/social.popfeed.feed.list/l2"
    with session_scope() as session:
        session.add(
            Record(
                at_uri=list_uri,
                did="did:plc:abc",
                collection="social.popfeed.feed.list",
                rkey="l2",
                source_json=json.dumps({"description": "Games nominated in 2025"}),
            )
        )
    record = {**LIST_ITEM, "listUri": list_uri}
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i1",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert "Games nominated in 2025" in note["content"]
    assert "name" not in note


def test_list_item_falls_back_when_list_not_archived(settings, monkeypatch):
    # Keep this test offline: on an archive miss, _list_label now tries a
    # live fetch (see _fetch_and_archive_list). Force that fetch to fail so
    # we stay on the generic fallback text without touching the network.
    monkeypatch.setattr(neodb, "_fetch_and_archive_list", lambda list_uri: None)
    record = {**LIST_ITEM, "listUri": "at://did:plc:abc/social.popfeed.feed.list/missing"}
    ref = works.work_ref(record)
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i1",
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    assert "to a list" in note["content"]
    assert "name" not in note


def _fake_http_json(responses: Mapping[str, dict | None], calls: list[str]):
    """A stand-in for ``identity._http_json`` keyed by URL substring.

    Records every requested URL in ``calls`` so tests can assert a second
    translation of the same list doesn't hit the network again.
    """

    def fake(url: str, timeout: float = 8.0) -> dict | None:
        calls.append(url)
        for substring, value in responses.items():
            if substring in url:
                return value
        raise AssertionError(f"unexpected URL requested in test: {url}")

    return fake


def _translate_list_item(record, *, rkey="i1"):
    ref = works.work_ref(record)
    return neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey=rkey,
        record=record,
        operation="create",
        time_us=None,
        ref=ref,
    )


def test_list_item_fetches_and_archives_unarchived_list(settings, monkeypatch):
    list_did = "did:plc:zzz"
    list_uri = f"at://{list_did}/social.popfeed.feed.list/lst9"
    calls: list[str] = []
    responses = {
        "plc.directory": {
            "alsoKnownAs": ["at://lister.test"],
            "service": [{"id": "#atproto_pds", "serviceEndpoint": "https://pds.example"}],
        },
        "xrpc/com.atproto.repo.getRecord": {
            "cid": "bafyfakecid",
            "value": {"$type": "social.popfeed.feed.list", "name": "My Shelf"},
        },
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses, calls))

    record = {**LIST_ITEM, "listUri": list_uri}
    note, _ = _translate_list_item(record)
    assert note is not None
    assert "My Shelf" in note["content"]

    with session_scope() as session:
        row = session.get(Record, list_uri)
    assert row is not None
    assert row.did == list_did
    assert json.loads(row.source_json)["name"] == "My Shelf"

    calls_after_first_fetch = len(calls)
    assert calls_after_first_fetch > 0

    # A second listItem pointing at the same (now-archived) list must hit the
    # Record table, not the network again.
    note2, _ = _translate_list_item(record, rkey="i2")
    assert note2 is not None
    assert "My Shelf" in note2["content"]
    assert len(calls) == calls_after_first_fetch


def test_list_item_fetch_failure_is_cached_and_not_retried(settings, monkeypatch):
    list_did = "did:plc:zzz"
    list_uri = f"at://{list_did}/social.popfeed.feed.list/deadlst"
    calls: list[str] = []
    responses = {
        "plc.directory": {
            "service": [{"id": "#atproto_pds", "serviceEndpoint": "https://pds.example"}],
        },
        # Simulates a 404/unreachable record: _http_json swallows the error
        # and returns None.
        "xrpc/com.atproto.repo.getRecord": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses, calls))

    record = {**LIST_ITEM, "listUri": list_uri}
    note, _ = _translate_list_item(record)
    assert note is not None
    assert "to a list" in note["content"]
    assert list_uri in neodb._LIST_FETCH_FAILED

    with session_scope() as session:
        assert session.get(Record, list_uri) is None

    calls_after_first_fetch = len(calls)
    assert calls_after_first_fetch > 0

    # A second listItem pointing at the same dead list must not retry the
    # network at all — the negative cache short-circuits before any fetch.
    note2, _ = _translate_list_item(record, rkey="i2")
    assert note2 is not None
    assert "to a list" in note2["content"]
    assert len(calls) == calls_after_first_fetch


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
    assert len(ratings) == 1
    rating = ratings[0]
    assert rating["withRegardTo"] == ref.url
    assert (rating["value"], rating["best"], rating["worst"]) == (9, 10, 1)
    # NeoDB ingest requires id + published on every relatedWith entry
    assert rating["id"] and rating["href"] == rating["id"]
    assert rating["published"] == REVIEW["createdAt"]
    assert rating["attributedTo"] == note["attributedTo"]
    # review text is an untitled Comment on the mark — never a Review/Article
    comments = [r for r in note["relatedWith"] if r["type"] == "Comment"]
    assert comments and comments[0]["withRegardTo"] == ref.url
    # the Comment carries just the review text; the Note content leads with a
    # linked "Rated <work> n/10" line so plain Mastodon viewers see the work
    assert comments[0]["content"] == "<p>Mind-bending and heartfelt.</p>"
    assert note["content"].startswith(f'<p>Rated <a href="{ref.url}">')
    assert note["content"].endswith("<p>Mind-bending and heartfelt.</p>")
    assert comments[0]["id"] != rating["id"]  # facet ids are unique
    assert not any(r["type"] == "Review" for r in note["relatedWith"])
    # a titled Note would render as an Article; the work title stays in
    # content/tag only, and the poster is never a direct attachment
    assert "name" not in note
    assert "attachment" not in note
    # spoilers become a content warning
    assert note["sensitive"] is True
    assert "Spoilers" in note["summary"]
    # the work rides in tag as a typed NeoDB catalog ref + category hashtag
    work_tags = [t for t in note["tag"] if t["type"] == "Movie"]
    assert len(work_tags) == 1
    assert work_tags[0]["href"] == ref.url
    assert work_tags[0]["name"] == REVIEW["title"]
    assert work_tags[0]["image"] == REVIEW["posterUrl"]
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


def test_identifier_priority_prefers_canonical_forms(settings):
    book = {"identifiers": {"isbn10": "0441013597", "isbn13": "9780441013593"}}
    ref = works.work_ref({**book, "creativeWorkType": "book"})
    assert ref is not None and ref.work_id == "isbn13-9780441013593"
    assert ref.url == settings.catalog_id("book", "isbn13-9780441013593")

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
