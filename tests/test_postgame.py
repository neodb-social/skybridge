"""Postgame (``at.postgame.game``) -> NeoDB-compatible ActivityPub."""

from __future__ import annotations

import asyncio
import json

import pytest
from skybridge.activitypub import objects
from skybridge.db import session_scope
from skybridge.models import Record, Work
from skybridge.pipeline import process_event
from skybridge.translate import neodb, postgame, works
from sqlalchemy import func, select

# Shaped after a real record (at.postgame.game, 2026-10-03).
GAME = {
    "coverUrl": "https://images.igdb.com/igdb/image/upload/t_cover_big/coc64f.jpg",
    "genres": ["Role-playing (RPG)", "Adventure"],
    "igdbId": 225582,
    "igdbUrl": "https://www.igdb.com/games/control-resonant--1",
    "releaseYear": 2026,
    "title": "Control Resonant",
}

PLAYED = {
    "$type": "at.postgame.game",
    "createdAt": "2026-09-24T21:48:00.485Z",
    "edition": "Standard",
    "finishedAt": "2026-10-03T04:29:38.223Z",
    "format": "digital",
    "game": GAME,
    "librarySource": "Steam",
    "owned": True,
    "platform": "PC",
    "playedStatus": "completed",
    "rating": 8,
    "notes": "A great change of pace.\n\nThe story kept me hooked.",
    "startedAt": "2026-09-24T21:47:55.800Z",
    "status": "played",
    "updatedAt": "2026-10-03T04:29:51.539Z",
}

WISHLISTED = {
    "$type": "at.postgame.game",
    "createdAt": "2026-09-29T04:09:44.161Z",
    "game": {
        "coverUrl": "https://images.igdb.com/igdb/image/upload/t_cover_big/cob1t2.jpg",
        "igdbId": 12517,
        "igdbUrl": "https://www.igdb.com/games/undertale",
        "title": "Undertale",
    },
    "status": "wishlisted",
}


def _translate(record, *, operation="create", rkey="pg1"):
    ref = works.mint(record)
    return neodb.translate(
        did="did:plc:player",
        handle="player.test",
        collection=postgame.GAME_COLLECTION,
        rkey=rkey,
        record=record,
        operation=operation,
        event_time=None,
        ref=ref,
    )


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"status": "playing"}, "progress"),
        ({"status": "wishlisted"}, "wishlist"),
        ({"status": "backlogged"}, "wishlist"),
        ({"status": "backlogged", "backloggedStatus": "shelved"}, "dropped"),
        ({"status": "played"}, "complete"),
        ({"status": "played", "playedStatus": "completed"}, "complete"),
        ({"status": "played", "playedStatus": "mastered"}, "complete"),
        ({"status": "played", "playedStatus": "retired"}, "complete"),
        ({"status": "played", "playedStatus": "abandoned"}, "dropped"),
        ({"status": "PLAYED", "playedStatus": "Abandoned"}, "dropped"),  # case-insensitive
        # legacy values the lexicon says older records still carry
        ({"status": "started"}, "progress"),
        ({"status": "wishlist"}, "wishlist"),
        ({"status": "shelved"}, "dropped"),
        ({"status": "finished"}, "complete"),
        ({"status": "abandoned"}, "dropped"),
        ({"status": "played", "playedStatus": "shelved"}, "dropped"),
        ({"status": "unknown"}, None),
        ({}, None),
    ],
)
def test_shelf_status_mapping(fields, expected):
    assert postgame.shelf_status(fields) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(8, 8), (1, 1), (10, 10), (0, None), (11, None), (7.5, None), (True, None), (None, None)],
)
def test_rating_is_stars_times_two(value, expected):
    assert postgame.rating({"rating": value}) == expected


def test_is_game_detection():
    assert postgame.is_game(PLAYED)
    assert not postgame.is_game({"$type": "buzz.bookhive.book"})
    assert not postgame.is_game({"game": {"igdbId": 1}})


def test_game_mints_game_work_with_igdb_slug(settings):
    ref = works.mint(PLAYED)
    assert ref is not None
    assert ref.work_type == "video_game"
    assert works.ap_type_for(ref.work_type) == "Game"
    assert ref.work_id == "igdbId-225582"
    assert ref.title == "Control Resonant"
    assert ref.poster_url == GAME["coverUrl"]
    doc = objects.get_work_object(ref.work_type, ref.work_id)
    assert doc is not None
    urls = [e["url"] for e in doc["external_resources"]]
    assert "https://www.igdb.com/games/control-resonant--1" in urls


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/games/x",
        "https://www.igdb.com/companies/x",
        "https://www.igdb.com/games/x/extra",
        None,
    ],
)
def test_non_game_igdb_url_gives_no_slug(url):
    record = {**PLAYED, "game": {**GAME, "igdbUrl": url}}
    ids = postgame.as_work_record(record)["identifiers"]
    assert ids == {"igdbId": "225582"}


def test_played_game_becomes_rating_comment_and_status(settings):
    note, activity = _translate(PLAYED)
    assert note is not None
    ref = works.work_ref(PLAYED)
    assert ref is not None

    ratings = [r for r in note["relatedWith"] if r["type"] == "Rating"]
    assert len(ratings) == 1
    assert (ratings[0]["value"], ratings[0]["best"], ratings[0]["worst"]) == (8, 10, 1)
    assert ratings[0]["withRegardTo"] == ref.url

    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert [s["status"] for s in statuses] == ["complete"]

    comments = [r for r in note["relatedWith"] if r["type"] == "Comment"]
    assert len(comments) == 1
    assert (
        comments[0]["content"] == "<p>A great change of pace.</p><p>The story kept me hooked.</p>"
    )

    assert note["content"].startswith(f'<p>Rated <a href="{neodb._marker_url(ref.url)}">')
    assert "name" not in note
    assert not any(r["type"] == "Review" for r in note["relatedWith"])
    games = [t for t in note["tag"] if t.get("type") == "Game"]
    assert len(games) == 1 and games[0]["href"] == ref.url
    assert {"type": "Hashtag", "name": "#game"} in note["tag"]
    assert activity["type"] == "Create"
    assert note["published"] == PLAYED["createdAt"]


def test_status_only_game_leads_with_playing_verb(settings):
    note, _ = _translate(WISHLISTED)
    assert note is not None
    assert {r["type"] for r in note["relatedWith"]} == {"Status"}
    ref = works.work_ref(WISHLISTED)
    assert ref is not None
    assert note["content"] == (
        f'<p>Wants to play <a href="{neodb._marker_url(ref.url)}">Undertale</a></p>'
    )


def test_note_on_an_unplayed_game_is_still_a_comment(settings):
    # A library note ("This is the PAL version") rides as a Comment too.
    record = {**WISHLISTED, "status": "backlogged", "notes": "This is the PAL version"}
    note, _ = _translate(record, rkey="pg-note")
    assert note is not None
    assert {r["type"] for r in note["relatedWith"]} == {"Status", "Comment"}
    assert note["content"].startswith("<p>Wants to play ")
    assert note["content"].endswith("<p>This is the PAL version</p>")


def test_game_and_popfeed_game_share_one_catalog_work(settings):
    # Postgame can import a Popfeed game into its own record; both carry the
    # IGDB id, so they must resolve to ONE catalog work.
    popfeed_game = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Control Resonant",
        "creativeWorkType": "video_game",
        "identifiers": {"igdbId": "225582", "slug": "control-resonant--1"},
        "listType": "played_video_games",
        "createdAt": "2026-09-01T00:00:00.000Z",
    }
    ref_popfeed = works.mint(popfeed_game)
    ref_game = works.mint(PLAYED)
    assert ref_popfeed is not None and ref_game is not None
    assert ref_popfeed.work_key == ref_game.work_key
    with session_scope() as session:
        assert (session.scalar(select(func.count()).select_from(Work)) or 0) == 1


def _commit(did, rkey, record, operation="create"):
    return {
        "did": did,
        "kind": "commit",
        "commit": {
            "operation": operation,
            "collection": postgame.GAME_COLLECTION,
            "rkey": rkey,
            "record": record,
        },
    }


def test_pipeline_create_update_delete(settings):
    did = "did:plc:player"
    playing = {**PLAYED, "status": "playing", "rating": None, "notes": ""}
    del playing["playedStatus"]

    created = asyncio.run(process_event(_commit(did, "pg1", playing), allow_network=False))
    assert created is not None and created.activity["type"] == "Create"
    at_uri = created.at_uri
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        first_id = note["id"]
        assert [r["status"] for r in note["relatedWith"] if r["type"] == "Status"] == ["progress"]
        # raw Postgame source is archived verbatim (not the normalized form)
        assert json.loads(row.source_json)["$type"] == postgame.GAME_COLLECTION

    # Postgame edits the record in place when the game is finished.
    updated = asyncio.run(process_event(_commit(did, "pg1", PLAYED, "update"), allow_network=False))
    assert updated is not None and updated.activity["type"] == "Update"
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        assert note["id"] == first_id
        assert {r["type"] for r in note["relatedWith"]} == {"Status", "Rating", "Comment"}

    deleted = asyncio.run(process_event(_commit(did, "pg1", {}, "delete"), allow_network=False))
    assert deleted is not None and deleted.activity["type"] == "Delete"
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None and row.deleted_at is not None


def test_popfeed_and_postgame_records_keep_separate_notes(settings):
    # The same game on both apps: each record publishes its own Note, and the
    # Postgame record never joins the Popfeed review/listItem pair.
    did = "did:plc:player"
    review = {
        "$type": "social.popfeed.feed.review",
        "title": "Control Resonant",
        "creativeWorkType": "video_game",
        "identifiers": {"igdbId": "225582"},
        "rating": 7,
        "text": "Good.",
        "createdAt": "2026-09-01T00:00:00.000Z",
    }
    popfeed_event = {
        "did": did,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "social.popfeed.feed.review",
            "rkey": "rv1",
            "record": review,
        },
    }
    first = asyncio.run(process_event(popfeed_event, allow_network=False))
    second = asyncio.run(process_event(_commit(did, "pg1", PLAYED), allow_network=False))
    assert first is not None and second is not None
    assert first.activity["type"] == "Create" and second.activity["type"] == "Create"
    assert first.activity["object"]["id"] != second.activity["object"]["id"]
    with session_scope() as session:
        rows = session.scalars(select(Record).where(Record.did == did)).all()
        assert {r.collection for r in rows if r.ap_object_json} == {
            "social.popfeed.feed.review",
            postgame.GAME_COLLECTION,
        }
