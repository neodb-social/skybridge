"""teal.fm (``fm.teal.feed.play`` / ``fm.teal.alpha.feed.play``) -> NeoDB AP.

One Note per (author, release): the first play publishes it, further plays of
the same release send nothing, and the anchor's deletion hands the Note to the
newest surviving play.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from skybridge.activitypub import objects
from skybridge.atproto import backfill
from skybridge.db import session_scope
from skybridge.models import Record, Work
from skybridge.pipeline import process_event
from skybridge.translate import neodb, teal, works
from sqlalchemy import func, select

DID = "did:plc:listener"
KINTSUGI = "2deefc93-3d50-43b6-a380-de0de3d86ba1"

PLAY = {
    "$type": teal.PLAY_COLLECTION,
    "trackName": "The Ghosts of Beverly Drive",
    "artists": [{"artistName": "Death Cab for Cutie"}],
    "duration": None,
    "playedTime": "2026-09-07T15:22:28.000Z",
    "recordingMbId": "mbid:0256ba09-b1ca-47ed-94b3-cc0029f168e9",
    "releaseMbId": f"mbid:{KINTSUGI}",
    "releaseName": "Kintsugi",
    "submissionClientAgent": "multi-scrobbler/0.16.4",
}

# Same album, another track, a minute later.
PLAY_2 = {
    **PLAY,
    "trackName": "Black Sun",
    "recordingMbId": "mbid:6b7f0f6a-2f8e-4d61-9a0e-0d2c1a9b3c11",
    "playedTime": "2026-09-07T15:27:00.000Z",
}

APPLE_PLAY = {
    "$type": teal.PLAY_COLLECTION,
    "trackName": "TAKE ME BACK",
    "artists": [{"artistName": "Lucy Bedroque"}],
    "duration": 171,
    "musicServiceUri": "https://music.apple.com",
    "originUri": "https://music.apple.com/us/album/take-me-back/6797714291?i=6797714297",
    "playedTime": "2026-09-07T15:25:48Z",
    "releaseName": "SISTERHOOD",
    "submissionClientAgent": "piper/v0.0.11",
}

# No release identifier of any kind: only a Last.fm track page.
UNIDENTIFIED_PLAY = {
    "$type": teal.PLAY_COLLECTION,
    "trackName": "Bitter End",
    "artists": [{"artistName": "Solipsy"}],
    "musicServiceUri": "https://last.fm",
    "originUri": "https://www.last.fm/music/Solipsy/_/Bitter+End",
    "playedTime": "2026-09-07T15:24:15Z",
    "releaseName": "Bitter End",
}


def _json(value: str | None) -> dict:
    """Parse a stored AP form, asserting the row actually holds one."""
    assert value is not None
    return json.loads(value)


def _alpha(record: dict) -> dict:
    return {**record, "$type": teal.ALPHA_PLAY_COLLECTION}


def _translate(record, *, collection=teal.PLAY_COLLECTION, rkey="3lplay000001", operation="create"):
    ref = works.mint(record)
    return neodb.translate(
        did=DID,
        handle="listener.test",
        collection=collection,
        rkey=rkey,
        record=record,
        operation=operation,
        event_time=None,
        ref=ref,
    )


# --- record shape -----------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (f"mbid:{KINTSUGI}", KINTSUGI),
        (KINTSUGI, KINTSUGI),  # bare uuid
        (f"MBID:{KINTSUGI.upper()}", KINTSUGI),  # case-insensitive
        ("mbid:", None),
        ("mbid:not-a-uuid", None),
        ("https://musicbrainz.org/release/" + KINTSUGI, None),
        (None, None),
        (42, None),
    ],
)
def test_mbid_parsing(value, expected):
    assert teal.mbid(value) == expected


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("https://music.apple.com/us/album/take-me-back/6797714291?i=6797714297", "6797714291"),
        ("https://music.apple.com/gb/album/wild-love/6789444108", "6789444108"),
        ("https://music.apple.com/album/6789444108", "6789444108"),
        ("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC", None),  # a track, not an album
        ("https://www.last.fm/music/Solipsy/_/Bitter+End", None),
        (None, None),
    ],
)
def test_apple_album_id(uri, expected):
    assert teal.apple_album_id(uri) == expected


def test_artist_names_prefers_refs_and_falls_back_to_deprecated_array():
    assert teal.artist_names(PLAY) == ["Death Cab for Cutie"]
    assert teal.artist_names(
        {"artists": [{"artistName": "Calcou"}, {"artistName": " Jody Wisternoff "}]}
    ) == ["Calcou", "Jody Wisternoff"]
    assert teal.artist_names({"artists": [], "artistNames": ["Passenger"]}) == ["Passenger"]
    assert teal.artist_names({}) == []


def test_is_play_detection():
    assert teal.is_play(PLAY)
    assert teal.is_play(_alpha(PLAY))
    assert not teal.is_play({"$type": "fm.teal.actor.status"})
    assert not teal.is_play({"$type": "buzz.bookhive.book"})


# --- work identity ----------------------------------------------------------


@pytest.mark.parametrize("record", [PLAY, _alpha(PLAY)])
def test_play_mints_album_work_on_the_release(settings, record):
    ref = works.mint(record)
    assert ref is not None
    assert ref.work_type == "music"
    assert works.ap_type_for(ref.work_type) == "Album"
    assert works.category_for(ref.work_type) == "music"
    # the bare uuid keys the work (never the "mbid:" prefix)
    assert ref.work_id == f"mbReleaseId-{KINTSUGI}"
    assert ref.title == "Kintsugi"


def test_catalog_object_exposes_musicbrainz_release_url(settings):
    works.mint(PLAY)
    ref = works.work_ref(PLAY)
    assert ref is not None
    doc = objects.get_work_object(ref.work_type, ref.work_id)
    assert doc is not None
    assert doc["type"] == "Album"
    assert doc["display_title"] == "Kintsugi"
    urls = [e["url"] for e in doc["external_resources"]]
    assert urls == [f"https://musicbrainz.org/release/{KINTSUGI}"]


def test_apple_music_origin_identifies_the_album_when_no_mbid(settings):
    ref = works.mint(APPLE_PLAY)
    assert ref is not None
    assert ref.work_id == "appleMusicAlbumId-6797714291"
    assert ref.title == "SISTERHOOD"
    doc = objects.get_work_object(ref.work_type, ref.work_id)
    assert doc is not None
    assert [e["url"] for e in doc["external_resources"]] == [
        "https://music.apple.com/album/6797714291"
    ]


def test_recording_only_or_unidentified_play_mints_no_work(settings):
    # NeoDB has no track item type: a recording id must not stand in for an
    # album, and a Last.fm track page names nothing resolvable.
    recording_only = {k: v for k, v in PLAY.items() if k != "releaseMbId"}
    assert works.mint(recording_only) is None
    assert works.mint(UNIDENTIFIED_PLAY) is None
    with session_scope() as session:
        assert (session.scalar(select(func.count()).select_from(Work)) or 0) == 0


def test_play_and_popfeed_release_dedup_by_musicbrainz_release(settings):
    popfeed_album = {
        "$type": "social.popfeed.feed.review",
        "title": "Kintsugi",
        "creativeWorkType": "music",
        "identifiers": {"mbReleaseId": KINTSUGI},
        "rating": 8,
        "createdAt": "2026-07-01T00:00:00.000Z",
    }
    ref_popfeed = works.mint(popfeed_album)
    ref_play = works.mint(PLAY)
    assert ref_popfeed is not None and ref_play is not None
    assert ref_popfeed.work_key == ref_play.work_key


# --- Note shape -------------------------------------------------------------


@pytest.mark.parametrize("collection", sorted(teal.PLAY_COLLECTIONS))
def test_play_becomes_listened_status_only(settings, collection):
    record = {**PLAY, "$type": collection}
    note, activity = _translate(record, collection=collection)
    assert note is not None
    ref = works.work_ref(record)
    assert ref is not None

    kinds = [r["type"] for r in note["relatedWith"]]
    assert kinds == ["Status"]
    assert note["relatedWith"][0]["status"] == "complete"
    assert note["relatedWith"][0]["withRegardTo"] == ref.url

    # names the album (linked with the ~neodb~ marker) and the artists — and
    # NOT the track, so every play of the release derives the same Note
    assert note["content"] == (
        f'<p>Listened to <a href="{neodb._marker_url(ref.url)}">Kintsugi</a>'
        " by Death Cab for Cutie</p>"
    )
    assert "Ghosts of Beverly Drive" not in note["content"]
    assert "name" not in note
    albums = [t for t in note["tag"] if t.get("type") == "Album"]
    assert len(albums) == 1 and albums[0]["href"] == ref.url and albums[0]["name"] == "Kintsugi"
    assert {"type": "Hashtag", "name": "#music"} in note["tag"]
    assert activity["type"] == "Create"
    # playedTime is the moment the Note is about (a play has no createdAt)
    assert note["published"] == PLAY["playedTime"]


def test_note_without_artists_or_release_name(settings):
    record = {**PLAY, "artists": [], "releaseName": ""}
    note, _ = _translate(record)
    assert note is not None
    ref = works.work_ref(record)
    assert ref is not None
    # the album is never labelled with the track name
    assert (
        note["content"] == f'<p>Listened to <a href="{neodb._marker_url(ref.url)}">an album</a></p>'
    )


# --- pipeline: one Note per (author, release) -------------------------------


def _commit(rkey, record, operation="create", collection=None):
    return {
        "did": DID,
        "kind": "commit",
        "commit": {
            "operation": operation,
            "collection": collection or record.get("$type") or teal.PLAY_COLLECTION,
            "rkey": rkey,
            "record": record,
        },
    }


def _run(event):
    return asyncio.run(process_event(event, allow_network=False))


def _row(at_uri) -> Record:
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None
        session.expunge(row)
        return row


def test_first_play_publishes_and_later_plays_of_the_release_send_nothing(settings):
    first = _run(_commit("3lplay000001", PLAY))
    assert first is not None and first.activity["type"] == "Create"
    anchor = _row(first.at_uri)
    assert anchor.ap_object_json is not None
    note_id = _json(anchor.ap_object_json)["id"]
    assert note_id.endswith("/posts/3lplay000001")

    second = _run(_commit("3lplay000002", PLAY_2))
    assert second is not None
    assert second.activity == {}  # nothing to deliver
    assert second.delivered == 0
    # the second play is archived, grouped on the same work, and holds no
    # Note of its own
    other = _row(second.at_uri)
    assert other.work_key == anchor.work_key
    assert other.ap_object_json is None and other.ap_activity_json is None
    # ...and the anchor's stored forms were not rewritten into an Update
    anchor_after = _row(first.at_uri)
    assert anchor_after.ap_object_json == anchor.ap_object_json
    assert _json(anchor_after.ap_activity_json)["type"] == "Create"

    # the alpha NSID joins the same group
    third = _run(_commit("3lplay000003", _alpha(PLAY_2)))
    assert third is not None and third.activity == {}
    assert _row(third.at_uri).work_key == anchor.work_key


def test_a_different_release_gets_its_own_note(settings):
    _run(_commit("3lplay000001", PLAY))
    other = _run(_commit("3lplay000002", APPLE_PLAY))
    assert other is not None and other.activity["type"] == "Create"
    assert _json(_row(other.at_uri).ap_object_json)["id"].endswith("/posts/3lplay000002")


def test_late_release_title_updates_the_note_once(settings):
    # First play knows the release only by id; a later one names it.
    untitled = {k: v for k, v in PLAY.items() if k != "releaseName"}
    first = _run(_commit("3lplay000001", untitled))
    assert first is not None and first.activity["type"] == "Create"
    assert "an album" in _json(_row(first.at_uri).ap_object_json)["content"]

    second = _run(_commit("3lplay000002", PLAY_2))
    assert second is not None and second.activity["type"] == "Update"
    # the Update rides on the anchor's id, not the second play's
    assert second.activity["object"]["id"].endswith("/posts/3lplay000001")
    note = _json(_row(first.at_uri).ap_object_json)
    assert "Kintsugi" in note["content"] and "an album" not in note["content"]
    assert _row(second.at_uri).ap_object_json is None

    # now that nothing differs, a third play is silent again
    third = _run(_commit("3lplay000003", {**PLAY_2, "trackName": "Little Wanderer"}))
    assert third is not None and third.activity == {}


def test_deleting_a_non_anchor_play_sends_nothing(settings):
    first = _run(_commit("3lplay000001", PLAY))
    second = _run(_commit("3lplay000002", PLAY_2))
    assert first is not None and second is not None
    deleted = _run(_commit("3lplay000002", {}, "delete"))
    assert deleted is not None
    assert deleted.activity == {} and deleted.delivered == 0
    assert _row(second.at_uri).deleted_at is not None
    assert _row(first.at_uri).ap_object_json is not None


def test_deleting_the_anchor_hands_the_note_to_the_newest_survivor(settings):
    first = _run(_commit("3lplay000001", PLAY))
    second = _run(_commit("3lplay000002", PLAY_2))
    third = _run(_commit("3lplay000003", {**PLAY_2, "trackName": "Little Wanderer"}))
    assert first is not None and second is not None and third is not None

    deleted = _run(_commit("3lplay000001", {}, "delete"))
    assert deleted is not None and deleted.activity["type"] == "Delete"
    tomb = _row(first.at_uri)
    assert tomb.deleted_at is not None
    assert _json(tomb.ap_activity_json)["object"]["id"].endswith("/posts/3lplay000001")

    # the newest surviving play (highest TID) now holds a fresh Create
    survivor = _row(third.at_uri)
    assert survivor.ap_object_json is not None
    assert _json(survivor.ap_object_json)["id"].endswith("/posts/3lplay000003")
    assert _json(survivor.ap_activity_json)["type"] == "Create"
    assert _row(second.at_uri).ap_object_json is None


def test_deleting_the_last_play_leaves_only_a_tombstone(settings):
    first = _run(_commit("3lplay000001", PLAY))
    assert first is not None
    deleted = _run(_commit("3lplay000001", {}, "delete"))
    assert deleted is not None and deleted.activity["type"] == "Delete"
    with session_scope() as session:
        live = session.scalars(select(Record).where(Record.deleted_at.is_(None))).all()
        assert live == []


def test_unidentified_play_is_archived_without_ap(settings):
    result = _run(_commit("3lplay000001", UNIDENTIFIED_PLAY))
    assert result is not None and result.activity == {}
    row = _row(result.at_uri)
    assert row.work_key is None
    assert row.ap_object_json is None and row.ap_activity_json is None
    # raw teal source is archived verbatim (not the normalized work shape)
    assert json.loads(row.source_json)["$type"] == teal.PLAY_COLLECTION
    # nothing to dereference at the post URL
    assert objects.get_post_object("listener.test", "3lplay000001") is None


def test_update_moving_a_play_to_another_release_reanchors_both_groups(settings):
    first = _run(_commit("3lplay000001", PLAY))
    second = _run(_commit("3lplay000002", PLAY_2))
    assert first is not None and second is not None

    # The anchor is re-identified as a different album: its Note follows it
    # (an Update on the same id), and the release it left is re-published
    # by the survivor under its own rkey.
    moved = _run(_commit("3lplay000001", APPLE_PLAY, "update"))
    assert moved is not None and moved.activity["type"] == "Update"
    moved_note = _json(_row(first.at_uri).ap_object_json)
    assert moved_note["id"].endswith("/posts/3lplay000001")
    assert "SISTERHOOD" in moved_note["content"]
    survivor = _row(second.at_uri)
    assert survivor.ap_object_json is not None
    assert _json(survivor.ap_activity_json)["type"] == "Create"
    assert "Kintsugi" in _json(survivor.ap_object_json)["content"]


def test_update_into_a_group_with_a_holder_retracts_the_moving_note(settings):
    first = _run(_commit("3lplay000001", PLAY))
    apple = _run(_commit("3lplay000002", APPLE_PLAY))
    assert first is not None and apple is not None

    # The Apple play is re-identified as Kintsugi, which already has a Note:
    # its own Note is retracted rather than leaving two for one release.
    moved = _run(_commit("3lplay000002", PLAY_2, "update"))
    assert moved is not None
    row = _row(apple.at_uri)
    assert row.work_key == _row(first.at_uri).work_key
    assert row.ap_object_json is None
    assert _json(row.ap_activity_json)["type"] == "Delete"
    assert _json(_row(first.at_uri).ap_activity_json)["type"] == "Create"


# --- backfill ---------------------------------------------------------------


def test_backfill_fetches_plays_before_archive_only_lists(settings):
    order = backfill._content_collections()
    for collection in teal.PLAY_COLLECTIONS:
        assert order.index(collection) < order.index("social.popfeed.feed.list")
