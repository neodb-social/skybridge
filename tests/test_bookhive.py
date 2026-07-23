"""BookHive (``buzz.bookhive.book``) -> NeoDB-compatible ActivityPub."""

from __future__ import annotations

import asyncio
import json

import pytest
from skybridge.activitypub import objects
from skybridge.db import session_scope
from skybridge.models import Record, Work
from skybridge.pipeline import process_event
from skybridge.translate import bookhive, neodb, works
from sqlalchemy import func, select

BOOK = {
    "$type": "buzz.bookhive.book",
    "title": "The Left Hand of Darkness",
    "authors": "Ursula K. Le Guin",
    "hiveId": "hive:abc123",
    "status": "buzz.bookhive.defs#finished",
    "stars": 9,
    "review": "A landmark of the genre.\n\nUtterly humane.",
    "identifiers": {"isbn13": "9780441478125", "isbn10": "0441478123", "goodreadsId": "18423"},
    "createdAt": "2026-07-03T17:16:24.038Z",
}

WANT_TO_READ = {
    "$type": "buzz.bookhive.book",
    "title": "Dune",
    "authors": "Frank Herbert",
    "hiveId": "hive:dune",
    "status": "buzz.bookhive.defs#wantToRead",
    "identifiers": {"isbn13": "9780441013593"},
    "createdAt": "2026-07-04T10:00:00.000Z",
}


def _translate(record, *, operation="create", rkey="bk1"):
    ref = works.mint(record)
    return neodb.translate(
        did="did:plc:reader",
        handle="reader.test",
        collection=bookhive.BOOK_COLLECTION,
        rkey=rkey,
        record=record,
        operation=operation,
        time_us=None,
        ref=ref,
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("buzz.bookhive.defs#finished", "complete"),
        ("buzz.bookhive.defs#reading", "progress"),
        ("buzz.bookhive.defs#wantToRead", "wishlist"),
        ("buzz.bookhive.defs#abandoned", "dropped"),
        ("finished", "complete"),  # bare token
        ("FINISHED", "complete"),  # case-insensitive
        ("buzz.bookhive.defs#unknown", None),
        (None, None),
    ],
)
def test_shelf_status_mapping(status, expected):
    assert bookhive.shelf_status({"status": status}) == expected


def test_is_book_detection():
    assert bookhive.is_book(BOOK)
    assert bookhive.is_book({"hiveId": "x"})  # no $type, but has hiveId
    assert not bookhive.is_book({"$type": "social.popfeed.feed.review"})
    assert not bookhive.is_book({"title": "x"})


def test_book_mints_book_edition_work(settings):
    ref = works.mint(BOOK)
    assert ref is not None
    assert ref.work_type == "book"
    assert works.ap_type_for(ref.work_type) == "Edition"
    assert works.category_for(ref.work_type) == "book"
    # ISBN13 wins the identity (ranks above goodreadsId/hiveId).
    assert ref.work_id == "isbn13-9780441478125"
    assert ref.title == "The Left Hand of Darkness"


def test_book_becomes_rating_comment_and_status(settings):
    note, activity = _translate(BOOK)
    assert note is not None
    ref = works.work_ref(BOOK)
    assert ref is not None

    ratings = [r for r in note["relatedWith"] if r["type"] == "Rating"]
    assert len(ratings) == 1
    assert (ratings[0]["value"], ratings[0]["best"], ratings[0]["worst"]) == (9, 10, 1)
    assert ratings[0]["withRegardTo"] == ref.url

    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert len(statuses) == 1
    assert statuses[0]["status"] == "complete"

    comments = [r for r in note["relatedWith"] if r["type"] == "Comment"]
    assert len(comments) == 1
    # plain text (no facets): paragraphs preserved, single newlines -> <br/>
    assert comments[0]["content"] == "<p>A landmark of the genre.</p><p>Utterly humane.</p>"

    # lead line links the work with the ~neodb~ marker so peers localize it
    assert note["content"].startswith(f'<p>Rated <a href="{neodb._marker_url(ref.url)}">')
    # never a titled Note (would render Article-like) and no Review facet
    assert "name" not in note
    assert not any(r["type"] == "Review" for r in note["relatedWith"])
    # the work rides in tag as an Edition + #book hashtag
    editions = [t for t in note["tag"] if t.get("type") == "Edition"]
    assert len(editions) == 1 and editions[0]["href"] == ref.url
    assert {"type": "Hashtag", "name": "#book"} in note["tag"]
    assert activity["type"] == "Create"
    assert note["published"] == BOOK["createdAt"]


def test_status_only_book_has_no_rating_or_comment(settings):
    note, _ = _translate(WANT_TO_READ)
    assert note is not None
    kinds = {r["type"] for r in note["relatedWith"]}
    assert kinds == {"Status"}
    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert statuses[0]["status"] == "wishlist"
    # natural reading verb leads the Note for an unrated shelf-add
    ref = works.work_ref(WANT_TO_READ)
    assert ref is not None
    assert (
        note["content"] == f'<p>Wants to read <a href="{neodb._marker_url(ref.url)}">Dune</a></p>'
    )


def test_catalog_object_exposes_isbn_and_goodreads(settings):
    works.mint(BOOK)
    ref = works.work_ref(BOOK)
    assert ref is not None
    doc = objects.get_work_object(ref.work_type, ref.work_id)
    assert doc is not None
    assert doc["type"] == "Edition"
    assert doc["isbn"] == "9780441478125"
    urls = [e["url"] for e in doc["external_resources"]]
    assert "https://www.goodreads.com/book/show/18423" in urls


def test_book_and_popfeed_book_dedup_by_isbn(settings):
    # A popfeed book review and a BookHive book sharing an ISBN13 must resolve
    # to ONE catalog work rather than minting duplicates.
    popfeed_book = {
        "$type": "social.popfeed.feed.review",
        "title": "The Left Hand of Darkness",
        "creativeWorkType": "book",
        "identifiers": {"isbn13": "9780441478125"},
        "rating": 8,
        "createdAt": "2026-07-01T00:00:00.000Z",
    }
    ref_popfeed = works.mint(popfeed_book)
    ref_book = works.mint(BOOK)
    assert ref_popfeed is not None and ref_book is not None
    assert ref_popfeed.work_key == ref_book.work_key
    with session_scope() as session:
        assert (session.scalar(select(func.count()).select_from(Work)) or 0) == 1


def _commit(did, rkey, record, operation="create"):
    return {
        "did": did,
        "kind": "commit",
        "commit": {
            "operation": operation,
            "collection": bookhive.BOOK_COLLECTION,
            "rkey": rkey,
            "record": record,
        },
    }


def test_pipeline_create_update_delete(settings):
    did = "did:plc:reader"

    created = asyncio.run(process_event(_commit(did, "bk1", BOOK), allow_network=False))
    assert created is not None
    at_uri = created.at_uri
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.op == "create" and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        assert any(r["type"] == "Status" for r in note["relatedWith"])
        # raw BookHive source is archived verbatim (not the normalized form)
        assert json.loads(row.source_json)["$type"] == bookhive.BOOK_COLLECTION

    updated_record = {**BOOK, "stars": 10, "status": "buzz.bookhive.defs#finished"}
    updated = asyncio.run(
        process_event(_commit(did, "bk1", updated_record, "update"), allow_network=False)
    )
    assert updated is not None and updated.activity["type"] == "Update"
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        rating = next(r for r in note["relatedWith"] if r["type"] == "Rating")
        assert rating["value"] == 10

    deleted = asyncio.run(process_event(_commit(did, "bk1", {}, "delete"), allow_network=False))
    assert deleted is not None and deleted.activity["type"] == "Delete"
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.deleted_at is not None and row.ap_activity_json is not None
        assert json.loads(row.ap_activity_json)["object"]["type"] == "Tombstone"
