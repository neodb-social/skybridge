"""End-to-end pipeline against the captured real-world fixture (offline)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping

from skybridge import optout, telemetry
from skybridge.atproto import identity
from skybridge.atproto.replay import read_events, replay_file
from skybridge.config import WANTED_COLLECTIONS
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, Work
from skybridge.pipeline import process_event
from skybridge.stats import collect_stats
from skybridge.translate import neodb
from sqlalchemy import func, select


def _counts_from_fixture(path):
    wanted = set(WANTED_COLLECTIONS)
    processed = 0
    creates = set()
    deletes = set()
    updates = set()
    for ev in read_events(path):
        if ev.get("kind") != "commit":
            continue
        c = ev["commit"]
        if c["collection"] not in wanted:
            continue
        processed += 1
        uri = f"at://{ev['did']}/{c['collection']}/{c['rkey']}"
        op = c["operation"]
        {"create": creates, "update": updates, "delete": deletes}[op].add(uri)
    distinct = creates | updates | deletes
    return processed, len(distinct), len(deletes)


def test_replay_persists_records(settings, fixture_path):
    results = asyncio.run(replay_file(fixture_path, allow_network=False))
    processed, distinct_uris, n_deletes = _counts_from_fixture(fixture_path)

    assert len(results) == processed  # noise (like, identity) filtered out

    with session_scope() as session:
        total = session.scalar(select(func.count()).select_from(Record)) or 0
        deleted = (
            session.scalar(
                select(func.count()).select_from(Record).where(Record.deleted_at.isnot(None))
            )
            or 0
        )
        works = session.scalar(select(func.count()).select_from(Work)) or 0
        actors = session.scalar(select(func.count()).select_from(BridgedActor)) or 0

    assert total == distinct_uris
    assert deleted == n_deletes
    assert works > 0
    assert actors >= 1


def test_update_mutates_same_uri(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    with session_scope() as session:
        updated = list(session.scalars(select(Record).where(Record.op == "update")))
        deleted = list(session.scalars(select(Record).where(Record.op == "delete")))
    # The fixture contains exactly one update and one delete.
    assert len(updated) == 1
    assert len(deleted) == 1
    assert deleted[0].deleted_at is not None
    # The deleted record is collection membership (status-less listItem):
    # never published to AP, so its delete emits no activity either.
    assert deleted[0].ap_activity_json is None


def test_stats_reflect_replay(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    _, distinct_uris, n_deletes = _counts_from_fixture(fixture_path)
    stats = collect_stats()
    assert stats["records_total"] == distinct_uris
    assert stats["records_active"] == distinct_uris - n_deletes
    assert stats["works"] > 0
    assert set(stats["records_by_collection"]).issubset(set(WANTED_COLLECTIONS))


def test_lists_are_archived_but_not_translated(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    with session_scope() as session:
        lists = list(
            session.scalars(select(Record).where(Record.collection == "social.popfeed.feed.list"))
        )
    assert lists  # stored in the archive...
    for row in lists:
        assert row.source_json != "{}"
        assert row.ap_object_json is None  # ...but never translated
        assert row.ap_activity_json is None


def test_list_delete_tombstones_without_activity(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    with session_scope() as session:
        row = session.scalars(
            select(Record).where(Record.collection == "social.popfeed.feed.list")
        ).first()
        assert row is not None
        at_uri = row.at_uri
        did, rkey = row.did, row.rkey
    event = {
        "did": did,
        "kind": "commit",
        "commit": {
            "operation": "delete",
            "collection": "social.popfeed.feed.list",
            "rkey": rkey,
        },
    }
    result = asyncio.run(process_event(event, allow_network=False))
    assert result is not None and result.operation == "delete"
    with session_scope() as session:
        row = session.get(Record, at_uri)
        assert row is not None
        assert row.deleted_at is not None
        assert row.ap_activity_json is None  # no Delete activity emitted


# --- review <-> listItem pairing: one AP Note per (author, work) -----------
#
# The Note id is anchored on whichever record publishes first; every later
# change to either record re-derives the combined Note and emits an Update
# with the same id.

_MERGE_DID = "did:plc:mergetest"

_REVIEW_REC = {
    "$type": "social.popfeed.feed.review",
    "title": "Everything Everywhere All at Once",
    "text": "",
    "rating": 9,
    "createdAt": "2026-07-03T17:16:24.038Z",
    "identifiers": {"imdbId": "tt6710474", "tmdbId": "545611"},
    "creativeWorkType": "movie",
}

_ITEM_REC = {
    "$type": "social.popfeed.feed.listItem",
    "title": "Everything Everywhere All at Once",
    "listType": "watched_movies",
    "addedAt": "2026-07-03T17:16:55.308Z",
    "identifiers": {"tmdbId": "545611"},
    "creativeWorkType": "movie",
}

_REVIEW_URI = f"at://{_MERGE_DID}/social.popfeed.feed.review/rv1"
_ITEM_URI = f"at://{_MERGE_DID}/social.popfeed.feed.listItem/it1"


def _ev(collection, rkey, record, op="create", *, did=_MERGE_DID):
    return {
        "did": did,
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {"operation": op, "collection": collection, "rkey": rkey, "record": record},
    }


def _run(event):
    return asyncio.run(process_event(event, allow_network=False))


def _related_types(note):
    if isinstance(note, str):
        note = json.loads(note)
    return {r["type"] for r in note.get("relatedWith", [])}


# Mirror of NeoDB's inbound requirements (takahe/ap_handlers.py +
# journal update_by_ap_object + catalog/sites/fedi.py) — a Note failing any
# of these is silently dropped or crashes ingestion on a NeoDB peer.
_NEODB_ITEM_TYPES = {
    "Edition",
    "Movie",
    "TVShow",
    "TVSeason",
    "TVEpisode",
    "Album",
    "Game",
    "Podcast",
    "PodcastEpisode",
    "Performance",
    "PerformanceProduction",
}
_NEODB_PIECE_TYPES = {"Status", "Rating", "Comment", "Review", "Note", "Shelf"}
_NEODB_STATUSES = {"complete", "progress", "wishlist", "dropped"}


def _assert_neodb_parseable(note):
    from datetime import datetime

    if isinstance(note, str):
        note = json.loads(note)
    # _parse_items: exactly one catalog-item tag, resolved via its href
    items = [t for t in note.get("tag", []) if t["type"] in _NEODB_ITEM_TYPES]
    assert len(items) == 1, f"need exactly one catalog item tag, got {items}"
    assert items[0]["href"].startswith("http")
    assert items[0]["type"] not in ("TVEpisode", "PodcastEpisode")  # unresolvable
    # each relatedWith entry must be ingestible by update_by_ap_object
    pieces = note.get("relatedWith", [])
    assert pieces, "no pieces to ingest"
    for p in pieces:
        assert p["type"] in _NEODB_PIECE_TYPES
        assert p["id"], f"{p['type']} missing id (used as remote_id)"
        datetime.fromisoformat(p.get("updated") or p["published"])
        datetime.fromisoformat(p["published"])
        if p["type"] == "Status":
            assert p["status"] in _NEODB_STATUSES
        elif p["type"] == "Rating":
            assert p["worst"] < p["best"]
            assert p["worst"] <= p["value"] <= p["best"]
        elif p["type"] == "Comment":
            assert p["content"].strip()


def _rows():
    with session_scope() as session:
        return session.get(Record, _REVIEW_URI), session.get(Record, _ITEM_URI)


def test_listitem_after_review_updates_review_note(settings):
    r1 = _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    r2 = _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC))
    assert r1.activity["type"] == "Create"
    assert r1.activity["object"]["type"] == "Note"
    assert "@context" in r1.activity
    # The listItem updates the review-anchored Note, no second Create.
    assert r2.activity["type"] == "Update"
    assert r2.activity["object"]["id"] == r1.activity["object"]["id"]
    review, item = _rows()
    assert _related_types(review.ap_object_json) == {"Rating", "Status"}
    assert item.ap_object_json is None  # merged: no standalone post
    assert review.work_key == item.work_key  # paired via the deduped work
    # every emitted state of the Note must be ingestible by a NeoDB peer
    _assert_neodb_parseable(r1.activity["object"])
    _assert_neodb_parseable(r2.activity["object"])


def test_review_after_listitem_updates_item_note(settings):
    r1 = _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC))
    r2 = _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    assert r1.activity["type"] == "Create"
    # The already-published item Note is reused: Update, same id, review
    # content folded in — no Delete/Create churn.
    assert r2.activity["type"] == "Update"
    assert r2.activity["object"]["id"] == r1.activity["object"]["id"]
    assert _related_types(r2.activity["object"]) == {"Rating", "Status"}
    review, item = _rows()
    assert review.ap_object_json is None  # item row anchors the pair Note
    assert _related_types(item.ap_object_json) == {"Rating", "Status"}
    _assert_neodb_parseable(r1.activity["object"])  # item-only Note
    _assert_neodb_parseable(r2.activity["object"])  # combined Note


def test_review_edit_updates_anchored_note(settings):
    _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC))
    edited = {**_REVIEW_REC, "text": "so good", "rating": 10}
    r3 = _run(_ev("social.popfeed.feed.review", "rv1", edited, op="update"))
    assert r3.activity["type"] == "Update"
    assert _related_types(r3.activity["object"]) == {"Rating", "Comment", "Status"}
    review, _ = _rows()
    note = json.loads(review.ap_object_json)
    assert "so good" in note["content"]


def test_partner_delete_rederives_note(settings):
    _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC))
    r3 = _run(_ev("social.popfeed.feed.listItem", "it1", None, op="delete"))
    # Merged-away partner: nothing of its own to retract; the anchored Note
    # is re-derived without the Status.
    assert r3.activity["type"] == "Update"
    review, item = _rows()
    assert item.deleted_at is not None
    assert _related_types(review.ap_object_json) == {"Rating"}


def test_anchor_delete_deletes_note_then_partner_heals(settings):
    r1 = _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC))
    r3 = _run(_ev("social.popfeed.feed.review", "rv1", None, op="delete"))
    # Deleting the anchoring record deletes the combined Note.
    assert r3.activity["type"] == "Delete"
    assert r3.activity["object"]["id"] == r1.activity["object"]["id"]
    review, item = _rows()
    assert review.deleted_at is not None
    assert item.deleted_at is None and item.ap_object_json is None  # AP-silent
    # The surviving partner re-publishes under its own rkey on its next event.
    r4 = _run(_ev("social.popfeed.feed.listItem", "it1", _ITEM_REC, op="update"))
    assert r4.activity["type"] == "Create"
    assert r4.activity["object"]["id"] != r1.activity["object"]["id"]
    assert _related_types(r4.activity["object"]) == {"Status"}


def test_non_status_listitem_archived_without_emission(settings):
    _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    fav = {**_ITEM_REC, "listType": "favorites"}
    r2 = _run(_ev("social.popfeed.feed.listItem", "it2", fav))
    # No shelf status => collection membership: archived, no AP emission.
    assert r2.activity == {}
    with session_scope() as session:
        item = session.get(Record, f"at://{_MERGE_DID}/social.popfeed.feed.listItem/it2")
    assert item is not None
    assert item.ap_object_json is None and item.ap_activity_json is None
    assert item.work_key is not None  # still linked to the catalog work
    review, _ = _rows()
    assert _related_types(review.ap_object_json) == {"Rating"}  # unaffected
    # Deleting membership emits nothing and leaves the review Note untouched.
    r3 = _run(_ev("social.popfeed.feed.listItem", "it2", None, op="delete"))
    assert r3.activity == {}
    review, _ = _rows()
    assert _related_types(review.ap_object_json) == {"Rating"}


# --- listItem note wording: names the parent list -------------------------

_LIST_REC = {
    "$type": "social.popfeed.feed.list",
    "name": "Best games of 2024",
    "description": "A list of the top games that came out in 2024",
    "listType": "default",
    "createdAt": "2026-01-01T00:00:00.000Z",
}


def test_list_item_note_names_the_archived_list(settings):
    _run(_ev("social.popfeed.feed.list", "lst1", _LIST_REC))
    item = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Elden Ring",
        "listType": "complete",
        "listUri": f"at://{_MERGE_DID}/social.popfeed.feed.list/lst1",
        "identifiers": {"igdbId": "119133"},
        "creativeWorkType": "video_game",
        "addedAt": "2026-01-02T00:00:00.000Z",
    }
    result = _run(_ev("social.popfeed.feed.listItem", "it-list1", item))
    assert result.activity["type"] == "Create"
    note = result.activity["object"]
    assert "Best games of 2024" in note["content"]
    assert "name" not in note


def test_list_item_note_falls_back_when_list_never_archived(settings, monkeypatch):
    # Keep this offline: an archive miss now tries a live fetch of the parent
    # list (see neodb._fetch_and_archive_list). Force it to fail so the test
    # stays deterministic and exercises the generic fallback wording.
    monkeypatch.setattr(neodb, "_fetch_and_archive_list", lambda list_uri: None)
    item = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Arc the Lad R",
        "listType": "complete",
        "listUri": f"at://{_MERGE_DID}/social.popfeed.feed.list/never-seen",
        "identifiers": {"igdbId": "26382"},
        "creativeWorkType": "video_game",
        "addedAt": "2026-01-02T00:00:00.000Z",
    }
    result = _run(_ev("social.popfeed.feed.listItem", "it-list2", item))
    assert result.activity["type"] == "Create"
    note = result.activity["object"]
    assert "to a list" in note["content"]
    assert "name" not in note


# --- social.popfeed.actor.profile: refreshes identity, never mints one -----

_PROFILE_COLLECTION = "social.popfeed.actor.profile"
_PROFILE_DID = "did:plc:profiletest"


def _fake_http_json(responses: Mapping[str, dict | None]):
    """Stand-in for ``identity._http_json`` keyed by URL substring (mirrors
    the helper of the same name in tests/test_identity.py)."""

    def fake(url: str, timeout: float = 8.0) -> dict | None:
        for substring, value in responses.items():
            if substring in url:
                return value
        raise AssertionError(f"unexpected URL requested in test: {url}")

    return fake


def test_profile_event_without_actor_is_ignored(settings):
    # A profile edit alone must never mint an actor.
    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": "Ghost"}, did=_PROFILE_DID)
    assert _run(event) is None
    with session_scope() as session:
        assert session.get(BridgedActor, _PROFILE_DID) is None


def test_profile_event_updates_display_name_offline(settings):
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": "New Nick"}, did=_PROFILE_DID)
    result = _run(event)  # allow_network=False
    assert result is not None
    assert result.activity["type"] == "Update"
    assert result.activity["object"]["type"] == "Person"
    assert result.activity["object"]["name"] == "New Nick"
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.display_name == "New Nick"
        assert actor.avatar is None  # untouched offline
        at_uri = f"at://{_PROFILE_DID}/{_PROFILE_COLLECTION}/self"
        assert session.get(Record, at_uri) is None  # identity metadata, not archived


def test_profile_event_refreshes_avatar_via_bsky_fallback(settings, monkeypatch):
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    pds = "https://pds.example"
    avatar_cid = "bafkreiabc123"
    responses = {
        "plc.directory": {"service": [{"id": "#atproto_pds", "serviceEndpoint": pds}]},
        "collection=app.bsky.actor.profile": {
            "value": {"displayName": "Bsky Name", "avatar": {"ref": {"$link": avatar_cid}}}
        },
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": ""}, did=_PROFILE_DID)
    result = asyncio.run(process_event(event, allow_network=True))

    expected_avatar = f"{pds}/xrpc/com.atproto.sync.getBlob?did={_PROFILE_DID}&cid={avatar_cid}"
    assert result is not None
    assert result.activity["object"]["icon"]["url"] == expected_avatar
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.avatar == expected_avatar


def test_profile_event_clears_display_name_when_both_sources_empty(settings, monkeypatch):
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        actor.display_name = "Old Name"
        actor.avatar = "https://pds.example/old-avatar"

    responses = {
        "plc.directory": {
            "service": [{"id": "#atproto_pds", "serviceEndpoint": "https://pds.example"}]
        },
        "collection=app.bsky.actor.profile": {"value": {"displayName": ""}},
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": ""}, did=_PROFILE_DID)
    result = asyncio.run(process_event(event, allow_network=True))

    assert result is not None
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.display_name is None  # both sources empty: genuinely removed
        assert actor.avatar == "https://pds.example/old-avatar"  # avatar is never cleared


def test_profile_event_keeps_display_name_when_plc_unreachable(settings, monkeypatch):
    # allow_network=True but PLC is down: the bsky fallback was never actually
    # consulted, so an empty popfeed displayName must NOT clear the stored one.
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        actor.display_name = "Old Name"

    monkeypatch.setattr(identity, "_http_json", _fake_http_json({"plc.directory": None}))

    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": ""}, did=_PROFILE_DID)
    result = asyncio.run(process_event(event, allow_network=True))

    assert result is not None
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.display_name == "Old Name"


def test_profile_event_opted_out_is_ignored(settings):
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    asyncio.run(optout.opt_out(_PROFILE_DID))
    event = _ev(_PROFILE_COLLECTION, "self", {"displayName": "New Nick"}, did=_PROFILE_DID)
    assert _run(event) is None
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.display_name is None  # untouched: opt-out short-circuits first


def test_profile_delete_is_a_noop(settings):
    identity.ensure_actor(_PROFILE_DID, allow_network=False)
    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        actor.display_name = "Untouched"

    event = _ev(_PROFILE_COLLECTION, "self", None, op="delete", did=_PROFILE_DID)
    assert _run(event) is None

    with session_scope() as session:
        actor = session.get(BridgedActor, _PROFILE_DID)
        assert actor is not None
        assert actor.display_name == "Untouched"


def test_profile_collection_absent_from_fixture(fixture_path):
    # Guards the _counts_from_fixture assumptions above: WANTED_COLLECTIONS
    # now includes the profile collection, but the fixture has no such
    # events, so none of the existing count assertions change.
    assert _PROFILE_COLLECTION in WANTED_COLLECTIONS
    assert not any(
        ev.get("commit", {}).get("collection") == _PROFILE_COLLECTION
        for ev in read_events(fixture_path)
        if ev.get("kind") == "commit"
    )


def test_ingest_metric_ticks_for_wanted_collection_only(settings, monkeypatch):
    calls = []
    monkeypatch.setattr(
        telemetry,
        "record_ingested",
        lambda collection, operation: calls.append((collection, operation)),
    )

    _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC))
    assert calls == [("social.popfeed.feed.review", "create")]

    # A collection we deliberately don't bridge (see config.WANTED_COLLECTIONS)
    # must not tick the ingest counter.
    assert "social.popfeed.feed.post" not in WANTED_COLLECTIONS
    result = _run(_ev("social.popfeed.feed.post", "p1", {"text": "hi"}))
    assert result is None
    assert calls == [("social.popfeed.feed.review", "create")]


def test_ingest_metric_not_ticked_for_opted_out_did(settings, monkeypatch):
    calls = []
    monkeypatch.setattr(
        telemetry,
        "record_ingested",
        lambda collection, operation: calls.append((collection, operation)),
    )

    opted_out_did = "did:plc:optedout"
    asyncio.run(optout.opt_out(opted_out_did))

    # Opted-out DID sends a wanted event: no metric tick, no processing.
    result = _run(_ev("social.popfeed.feed.review", "rv1", _REVIEW_REC, did=opted_out_did))
    assert result is None
    assert calls == []
