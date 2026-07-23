"""Episode/season work identity + the tv_episode federation cutoff.

Regression suite for the mis-mapping where positional identifier keys
(episodeNumber/seasonNumber/tmdbTvSeriesId) acted as global work aliases, so
"Dark Side of the Ring S7E4" resolved to "The Expanse S2E4" (both episode 4).
Also covers the follow-up policy: episode marks are never federated — episode
reviews archive silently, episode list-adds bridge as watching the season —
and the one-shot `repair` command that retracts the damage already broadcast.
"""

from __future__ import annotations

import asyncio
import json

from skybridge.db import session_scope
from skybridge.maintenance import repair
from skybridge.models import Record, Work, WorkIdentifier
from skybridge.pipeline import process_event
from skybridge.translate import works
from sqlalchemy import delete, select

DID = "did:plc:episodetest"

EP_IDENTIFIERS = {
    "episodeNumber": 4,
    "seasonNumber": 7,
    "tmdbId": "7377127",
    "tmdbTvSeriesId": "88401",
}

# Dark Side of the Ring S7E4 (the mis-mapped record from production).
EP_REVIEW = {
    "$type": "social.popfeed.feed.review",
    "title": "Dark Side of the Ring - S7E4 - Necro Butcher vs. Samoa Joe",
    "text": "",
    "rating": 10,
    "createdAt": "2026-07-23T01:19:28.795Z",
    "creativeWorkType": "tv_episode",
    "identifiers": dict(EP_IDENTIFIERS),
}

# The Expanse S2E4 — also "episode 4", the collision that caused the bug.
OTHER_EP_REVIEW = {
    **EP_REVIEW,
    "title": "The Expanse - S2E4 - Godspeed",
    "identifiers": {
        "episodeNumber": 4,
        "seasonNumber": 2,
        "tmdbId": "1262477",
        "tmdbTvSeriesId": "63639",
    },
}

EP_LIST_ITEM = {
    "$type": "social.popfeed.feed.listItem",
    "title": "Dark Side of the Ring - S7E4 - Necro Butcher vs. Samoa Joe",
    "listType": "watched_episodes",
    "addedAt": "2026-07-23T01:19:29.482Z",
    "creativeWorkType": "tv_episode",
    "identifiers": dict(EP_IDENTIFIERS),
}

SEASON_REVIEW = {
    "$type": "social.popfeed.feed.review",
    "title": "Dark Side of the Ring - Season 7",
    "text": "",
    "rating": 9,
    "createdAt": "2026-07-23T02:00:00.000Z",
    "creativeWorkType": "tv_season",
    "identifiers": {"tmdbId": "999001", "seasonNumber": 7, "tmdbTvSeriesId": "88401"},
}


def _ev(collection, rkey, record, op="create", *, did=DID):
    return {
        "did": did,
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {"operation": op, "collection": collection, "rkey": rkey, "record": record},
    }


def _run(event):
    return asyncio.run(process_event(event, allow_network=False))


def _aliases(work_type):
    with session_scope() as session:
        return {
            (r.id_key, r.id_value): r.work_key
            for r in session.scalars(
                select(WorkIdentifier).where(WorkIdentifier.creative_work_type == work_type)
            )
        }


# --- work identity: positional keys never merge distinct works --------------


def test_episodes_sharing_episode_number_stay_distinct(settings):
    ref1 = works.mint(OTHER_EP_REVIEW)
    ref2 = works.mint(EP_REVIEW)
    assert ref1 is not None and ref2 is not None
    # The production bug: ref2 came back as ref1's work via episodeNumber=4.
    assert ref1.work_key == "tv_episode:tmdbId-1262477"
    assert ref2.work_key == "tv_episode:tmdbId-7377127"


def test_same_show_episodes_stay_distinct(settings):
    ep5 = {
        **EP_REVIEW,
        "identifiers": {**EP_IDENTIFIERS, "episodeNumber": 5, "tmdbId": "7377128"},
    }
    ref1 = works.mint(EP_REVIEW)
    ref2 = works.mint(ep5)
    # Same seasonNumber + tmdbTvSeriesId must not fold E5 into E4's work.
    assert ref1 is not None and ref2 is not None and ref1.work_key != ref2.work_key


def test_positional_keys_are_not_registered_as_aliases(settings):
    works.mint(EP_REVIEW)
    keys = {k for k, _ in _aliases("tv_episode")}
    assert "episodeNumber" not in keys
    assert "seasonNumber" not in keys
    assert "tmdbTvSeriesId" not in keys
    assert "tmdbId" in keys


def test_positional_keys_never_become_the_primary_id(settings):
    # A record with only positional identifiers has no resolvable identity.
    record = {
        "$type": "social.popfeed.feed.review",
        "creativeWorkType": "tv_episode",
        "identifiers": {"episodeNumber": 4, "seasonNumber": 7},
    }
    assert works.work_ref(record) is None


# --- episode list-adds become season activity --------------------------------


def test_episode_list_item_minted_as_season(settings):
    ref = works.mint(EP_LIST_ITEM)
    assert ref is not None
    assert ref.work_type == "tv_season"
    assert ref.work_key == "tv_season:tmdbId-88401-season-7"
    assert ref.title == "Dark Side of the Ring - Season 7"
    assert ref.url.endswith("/catalog/tv_season/tmdbId-88401-season-7")
    # The episode's own tmdbId must not alias the season work.
    assert ("tmdbId", "7377127") not in _aliases("tv_season")


def test_episode_review_keeps_episode_work(settings):
    # Only list-adds convert; a review still mints (an unfederated) episode work.
    ref = works.mint(EP_REVIEW)
    assert ref is not None and ref.work_type == "tv_episode"


def test_converted_season_merges_with_real_season_record(settings):
    # Season record first (keyed by its own tmdbId), episode add folds into it.
    ref1 = works.mint(SEASON_REVIEW)
    ref2 = works.mint(EP_LIST_ITEM)
    assert ref1 is not None and ref2 is not None
    assert ref1.work_key == "tv_season:tmdbId-999001"
    assert ref2.work_key == ref1.work_key

    # Reverse arrival order merges too, anchored on the compound key.
    with session_scope() as session:
        session.execute(delete(WorkIdentifier))
        session.execute(delete(Work))
    ref3 = works.mint(EP_LIST_ITEM)
    ref4 = works.mint(SEASON_REVIEW)
    assert ref3 is not None and ref4 is not None
    assert ref3.work_key == "tv_season:tmdbId-88401-season-7"
    assert ref4.work_key == ref3.work_key


def test_episode_without_season_info_is_not_converted(settings):
    item = {**EP_LIST_ITEM, "identifiers": {"tmdbId": "7377127"}}
    ref = works.mint(item)
    assert ref is not None and ref.work_type == "tv_episode"


# --- pipeline: episode marks are archived, never federated -------------------


def test_episode_review_is_archived_without_ap(settings):
    result = _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    assert result is not None and result.activity == {}
    with session_scope() as session:
        row = session.get(Record, f"at://{DID}/social.popfeed.feed.review/ep1")
        assert row is not None
        assert row.work_key == "tv_episode:tmdbId-7377127"
        assert row.ap_object_json is None
        assert row.ap_activity_json is None


def test_unresolved_episode_review_is_not_published(settings):
    # Positional-only identifiers can't mint a work (ref is None); the episode
    # cutoff must still hold or the review falls through as a generic Note.
    record = {
        "$type": "social.popfeed.feed.review",
        "title": "Some Episode",
        "text": "",
        "rating": 8,
        "createdAt": "2026-07-23T01:00:00.000Z",
        "creativeWorkType": "tv_episode",
        "identifiers": {"episodeNumber": 4, "seasonNumber": 7, "tmdbTvSeriesId": "88401"},
    }
    result = _run(_ev("social.popfeed.feed.review", "ep9", record))
    assert result is not None and result.activity == {}
    with session_scope() as session:
        row = session.get(Record, f"at://{DID}/social.popfeed.feed.review/ep9")
        assert row is not None
        assert row.work_key is None
        assert row.ap_object_json is None and row.ap_activity_json is None


def test_episode_review_update_retracts_previously_published_note(settings):
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    # Simulate a Note published before the cutoff (legacy state).
    uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    note_id = "https://bridge.test/users/x/posts/ep1"
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        row.ap_object_json = json.dumps({"id": note_id})
        row.ap_activity_json = "{}"
    result = _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW, op="update"))
    # The stranded Note is Deleted on peers (by its stored id), not just
    # dropped locally, and the Delete is persisted as a pending retraction so
    # a crashed/failed delivery can be re-broadcast by repair.
    assert result is not None and result.activity.get("type") == "Delete"
    assert result.activity["object"]["id"] == note_id
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_activity_json is not None
        assert row.ap_object_json is None
        assert json.loads(row.ap_activity_json)["type"] == "Delete"
    # A further update must keep the pending retraction, not wipe it.
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW, op="update"))
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_activity_json is not None
        assert json.loads(row.ap_activity_json)["object"]["id"] == note_id


def test_episode_review_delete_does_not_republish_sibling(settings):
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    _run(_ev("social.popfeed.feed.review", "ep2", EP_REVIEW))  # rewatch, same work
    result = _run(_ev("social.popfeed.feed.review", "ep1", None, op="delete"))
    assert result is not None and result.activity == {}
    with session_scope() as session:
        sibling = session.get(Record, f"at://{DID}/social.popfeed.feed.review/ep2")
        assert sibling is not None
        assert sibling.ap_object_json is None  # _sync_pair must not resurrect it


def test_episode_list_add_publishes_season_watching_note(settings):
    result = _run(_ev("social.popfeed.feed.listItem", "it1", EP_LIST_ITEM))
    assert result is not None and result.activity.get("type") == "Create"
    note = result.activity["object"]
    (item_tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
    assert item_tag["type"] == "TVSeason"
    assert item_tag["href"].endswith("/catalog/tv_season/tmdbId-88401-season-7")
    assert item_tag["name"] == "Dark Side of the Ring - Season 7"
    (status,) = [r for r in note["relatedWith"] if r["type"] == "Status"]
    # "watched_episodes" would normally mean complete; one episode never
    # completes a season, so the season mark is always progress.
    assert status["status"] == "progress"
    assert status["withRegardTo"].endswith("/catalog/tv_season/tmdbId-88401-season-7")
    assert "Dark Side of the Ring - Season 7" in note["content"]
    assert "#tv" in {t.get("name") for t in note["tag"]}


# --- repair: retract broadcast episode notes, rebuild works, re-sync ---------


def _seed_legacy_damage(settings):
    """Archive an episode review + a movie review, then rewrite them into the
    pre-fix state: published episode Note, positional + poisoned aliases, and
    both records merged onto the wrong work."""
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    ep_uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    mv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    with session_scope() as session:
        ep = session.get(Record, ep_uri)
        mv = session.get(Record, mv_uri)
        assert ep is not None and mv is not None
        ep.work_key = "tv_episode:tmdbId-1262477"  # merged into the wrong episode
        ep.ap_object_json = json.dumps({"id": "https://bridge.test/users/x/posts/ep1"})
        ep.ap_activity_json = "{}"
        mv.work_key = "movie:tmdbId-999999"  # merged into the wrong movie
        assert mv.ap_object_json is not None
        note = json.loads(mv.ap_object_json)
        for tag in note.get("tag", []):
            if "href" in tag:
                tag["href"] = settings.catalog_id("movie", "tmdbId-999999")
        mv.ap_object_json = json.dumps(note)
        session.merge(
            WorkIdentifier(
                creative_work_type="tv_episode",
                id_key="episodeNumber",
                id_value="4",
                work_key="tv_episode:tmdbId-1262477",
            )
        )
        session.merge(
            WorkIdentifier(
                creative_work_type="tv_episode",
                id_key="tmdbId",
                id_value="7377127",  # poisoned by the wrong merge
                work_key="tv_episode:tmdbId-1262477",
            )
        )
    return ep_uri, mv_uri


MOVIE_REVIEW = {
    "$type": "social.popfeed.feed.review",
    "title": "Everything Everywhere All at Once",
    "text": "",
    "rating": 9,
    "createdAt": "2026-07-03T17:16:24.038Z",
    "identifiers": {"imdbId": "tt6710474", "tmdbId": "545611"},
    "creativeWorkType": "movie",
}


def test_repair_retracts_rebuilds_and_resyncs(settings):
    ep_uri, mv_uri = _seed_legacy_damage(settings)

    report = asyncio.run(repair(None))
    assert report.retracted == 1
    assert report.resent == 0  # a fresh retraction is not also "pending"
    assert report.remapped == 2
    assert report.resynced == 1

    with session_scope() as session:
        ep = session.get(Record, ep_uri)
        assert ep is not None
        assert ep.work_key == "tv_episode:tmdbId-7377127"
        # Unpublished, but the Delete is kept as a re-broadcastable pending
        # retraction (delivery is best-effort and in-memory).
        assert ep.ap_object_json is None and ep.ap_activity_json is not None
        assert json.loads(ep.ap_activity_json)["type"] == "Delete"
        mv = session.get(Record, mv_uri)
        assert mv is not None and mv.ap_object_json is not None
        assert mv.work_key == "movie:imdbId-tt6710474"
        note = json.loads(mv.ap_object_json)
        (item_tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
        assert item_tag["href"] == settings.catalog_id("movie", "imdbId-tt6710474")

    aliases = _aliases("tv_episode")
    assert ("episodeNumber", "4") not in aliases
    assert aliases[("tmdbId", "7377127")] == "tv_episode:tmdbId-7377127"

    # Idempotent: nothing left to retract or remap on a second run; only the
    # pending retraction is re-broadcast (a no-op for peers).
    again = asyncio.run(repair(None))
    assert again.retracted == 0
    assert again.remapped == 0
    assert again.resynced == 0
    assert again.resent == 1


def test_repair_resyncs_both_sides_of_a_moved_partner(settings):
    """A pair's Note lives on one anchor row; when a wrongly-merged partner
    moves to its own work during rebuild, both works need re-publishing —
    the old one to drop the partner's folded status, the new one to gain a
    Note — even though the moved row itself was AP-silent."""
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    rv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    item_uri = f"at://{DID}/social.popfeed.feed.listItem/li1"
    other_item = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Another Movie",
        "listType": "watched_movies",
        "addedAt": "2026-07-04T00:00:00.000Z",
        "identifiers": {"tmdbId": "77777"},
        "creativeWorkType": "movie",
    }
    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        assert rv is not None and rv.work_key is not None and rv.ap_object_json is not None
        # Legacy wrong merge: the listItem of a different movie rode the
        # review's work, its status folded into the review's Note.
        note = json.loads(rv.ap_object_json)
        note["relatedWith"].append(
            {
                "id": f"{note['id']}#status",
                "type": "Status",
                "withRegardTo": settings.catalog_id("movie", rv.work_key.split(":", 1)[1]),
                "attributedTo": note["attributedTo"],
                "href": f"{note['id']}#status",
                "published": note["published"],
                "updated": note["published"],
                "status": "complete",
            }
        )
        rv.ap_object_json = json.dumps(note)
        session.add(
            Record(
                at_uri=item_uri,
                did=DID,
                collection="social.popfeed.feed.listItem",
                rkey="li1",
                source_json=json.dumps(other_item),
                op="create",
                work_key=rv.work_key,
            )
        )

    report = asyncio.run(repair(None))
    assert report.remapped == 1  # only the listItem moves
    assert report.resynced == 2  # ...but both pairs re-publish

    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        item = session.get(Record, item_uri)
        assert rv is not None and item is not None and rv.ap_object_json is not None
        # The review's Note was re-derived without the stale folded status...
        rv_note = json.loads(rv.ap_object_json)
        assert {r["type"] for r in rv_note["relatedWith"]} == {"Rating"}
        # ...and the moved partner now anchors its own Note on the right work.
        assert item.work_key == "movie:tmdbId-77777"
        assert item.ap_object_json is not None
        item_note = json.loads(item.ap_object_json)
        (status,) = [r for r in item_note["relatedWith"] if r["type"] == "Status"]
        assert status["status"] == "complete"
        assert status["withRegardTo"] == settings.catalog_id("movie", "tmdbId-77777")


def test_repair_new_pair_anchor_skips_membership_rows(settings):
    """A fresh Note for a remapped pair must anchor on a contributing row.

    An older status-less list membership on the same work would otherwise win
    the anchor (oldest row first), and deleting that membership later would
    retract the real status Note."""
    # Older membership-only row on the target work: archived, no AP, no status.
    membership = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Another Movie",
        "listType": "favorites",
        "addedAt": "2026-07-01T00:00:00.000Z",
        "identifiers": {"tmdbId": "77777"},
        "creativeWorkType": "movie",
    }
    _run(_ev("social.popfeed.feed.listItem", "fav1", membership))
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    rv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    item_uri = f"at://{DID}/social.popfeed.feed.listItem/li1"
    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        assert rv is not None and rv.work_key is not None
        # Legacy wrong merge: a contributing listItem of movie 77777 rode the
        # review's work; the rebuild will move it home.
        session.add(
            Record(
                at_uri=item_uri,
                did=DID,
                collection="social.popfeed.feed.listItem",
                rkey="li1",
                source_json=json.dumps({**membership, "listType": "watched_movies"}),
                op="create",
                work_key=rv.work_key,
            )
        )

    asyncio.run(repair(None))

    with session_scope() as session:
        membership_row = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/fav1")
        item = session.get(Record, item_uri)
        assert membership_row is not None and item is not None
        assert membership_row.work_key == item.work_key == "movie:tmdbId-77777"
        # The Note anchors on the contributing row, not the older membership.
        assert membership_row.ap_object_json is None
        assert item.ap_object_json is not None


def test_repair_updates_published_episode_listitem_in_place(settings):
    """A published episode listItem that converts to a season keeps its Note:
    Delete + Create of the same rkey-derived object id would hit peers'
    tombstone caches, so the Note is Updated in place instead."""
    _run(_ev("social.popfeed.feed.listItem", "it1", EP_LIST_ITEM))
    uri = f"at://{DID}/social.popfeed.feed.listItem/it1"
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        note_id = note["id"]
        # Doctor into the legacy pre-fix state: episode work, episode-flavored
        # Note published under the same object id.
        row.work_key = "tv_episode:tmdbId-7377127"
        for tag in note.get("tag", []):
            if "href" in tag:
                tag["type"] = "Link"
                tag["href"] = settings.catalog_id("tv_episode", "tmdbId-7377127")
        row.ap_object_json = json.dumps(note)

    report = asyncio.run(repair(None))
    assert report.retracted == 0  # never Deleted...
    assert report.resynced == 1  # ...corrected in place instead

    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        assert row.work_key == "tv_season:tmdbId-88401-season-7"
        assert row.ap_object_json is not None and row.ap_activity_json is not None
        fixed = json.loads(row.ap_object_json)
        assert fixed["id"] == note_id  # same object id, no tombstone conflict
        (item_tag,) = [t for t in fixed["tag"] if t["type"] != "Hashtag"]
        assert item_tag["type"] == "TVSeason"
        assert item_tag["href"].endswith("/catalog/tv_season/tmdbId-88401-season-7")
        assert json.loads(row.ap_activity_json)["type"] == "Update"


def test_repair_resync_is_stateless_across_interrupted_runs(settings):
    """Simulates a crash between the rebuild (work_key committed) and the
    re-sync broadcast: the stored Note still references the old work while
    the record's key is already correct, so `remapped` is empty on the next
    run. The re-sync must detect the divergence from stored state alone."""
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    rv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    good_href = settings.catalog_id("movie", "imdbId-tt6710474")
    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        assert rv is not None and rv.ap_object_json is not None
        note = json.loads(rv.ap_object_json)
        for tag in note.get("tag", []):
            if "href" in tag:
                tag["href"] = settings.catalog_id("movie", "tmdbId-999999")
        rv.ap_object_json = json.dumps(note)

    report = asyncio.run(repair(None))
    assert report.remapped == 0  # the rebuild sees nothing to move...
    assert report.resynced == 1  # ...but the stale Note is still corrected

    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        assert rv is not None and rv.ap_object_json is not None
        note = json.loads(rv.ap_object_json)
        (item_tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
        assert item_tag["href"] == good_href


def test_repair_resends_pending_retraction_left_by_a_crash(settings):
    """A retraction persisted but never delivered (crash before the fanout
    drained) must be re-broadcast by the next run, forever if need be."""
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    pending = {
        "type": "Delete",
        "id": "https://bridge.test/users/x/posts/ep1#delete",
        "object": {"id": "https://bridge.test/users/x/posts/ep1", "type": "Tombstone"},
    }
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_object_json is None
        row.ap_activity_json = json.dumps(pending)

    report = asyncio.run(repair(None))
    assert report.retracted == 0  # nothing newly published to retract
    assert report.resent == 1

    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_activity_json is not None
        # The payload survives for future reruns (delivery is best-effort).
        assert json.loads(row.ap_activity_json)["type"] == "Delete"


def test_repair_collapses_duplicate_episode_notes_into_one_season_note(settings):
    """Several published legacy episode Notes from one season collapse into a
    single pair: exactly one Note survives (Updated in place to season
    content), every other holder is Deleted — not left federated and stale."""
    ep5_item = {
        **EP_LIST_ITEM,
        "title": "Dark Side of the Ring - S7E5 - The Dynamite Kid",
        "identifiers": {**EP_IDENTIFIERS, "episodeNumber": 5, "tmdbId": "7377128"},
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", EP_LIST_ITEM))
    _run(_ev("social.popfeed.feed.listItem", "it2", ep5_item))
    it1_uri = f"at://{DID}/social.popfeed.feed.listItem/it1"
    it2_uri = f"at://{DID}/social.popfeed.feed.listItem/it2"
    # Legacy pre-fix state: one Note per watched episode, each on its own
    # episode work (the fixed pipeline folds it2 into it1's season Note).
    with session_scope() as session:
        r1 = session.get(Record, it1_uri)
        r2 = session.get(Record, it2_uri)
        assert r1 is not None and r2 is not None and r1.ap_object_json is not None
        r1.work_key = "tv_episode:tmdbId-7377127"
        r2.work_key = "tv_episode:tmdbId-7377128"
        r2.ap_object_json = json.dumps({"id": "https://bridge.test/users/x/posts/it2"})
        r2.ap_activity_json = "{}"
        note_ids = {
            it1_uri: json.loads(r1.ap_object_json)["id"],
            it2_uri: "https://bridge.test/users/x/posts/it2",
        }

    report = asyncio.run(repair(None))
    assert report.retracted == 1  # the non-anchor duplicate is Deleted...
    assert report.resynced == 1  # ...and the surviving Note corrected in place

    with session_scope() as session:
        rows = []
        for uri in (it1_uri, it2_uri):
            row = session.get(Record, uri)
            assert row is not None
            assert row.work_key == "tv_season:tmdbId-88401-season-7"
            rows.append(row)
        published = [r for r in rows if r.ap_object_json is not None]
        retracted = [r for r in rows if r.ap_object_json is None]
        assert len(published) == 1 and len(retracted) == 1
        assert published[0].ap_object_json is not None
        note = json.loads(published[0].ap_object_json)
        (tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
        assert tag["type"] == "TVSeason"
        assert tag["href"].endswith("/catalog/tv_season/tmdbId-88401-season-7")
        assert retracted[0].ap_activity_json is not None
        delete = json.loads(retracted[0].ap_activity_json)
        assert delete["type"] == "Delete"
        assert delete["object"]["id"] == note_ids[retracted[0].at_uri]


def test_repair_retracts_workless_episode_review_note(settings):
    """A legacy generic Note published for an episode review that never
    minted a work (work_key NULL) is still found and retracted."""
    record = {
        "$type": "social.popfeed.feed.review",
        "title": "Some Episode",
        "text": "",
        "rating": 8,
        "createdAt": "2026-07-23T01:00:00.000Z",
        "creativeWorkType": "tv_episode",
        "identifiers": {"episodeNumber": 4, "seasonNumber": 7, "tmdbTvSeriesId": "88401"},
    }
    _run(_ev("social.popfeed.feed.review", "ep9", record))
    uri = f"at://{DID}/social.popfeed.feed.review/ep9"
    note_id = "https://bridge.test/users/x/posts/ep9"
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.work_key is None
        row.ap_object_json = json.dumps({"id": note_id})
        row.ap_activity_json = "{}"

    report = asyncio.run(repair(None))
    assert report.retracted == 1
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.ap_object_json is None
        assert row.ap_activity_json is not None
        assert json.loads(row.ap_activity_json)["object"]["id"] == note_id


def test_repair_retracts_to_historical_delivery_targets(settings):
    """A peer that received the Note and has since unfollowed (or a removed
    relay) is not in fanout()'s audience — the Delete must also go to every
    inbox the delivery log remembers for the record."""
    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.models import Delivery

    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        row.ap_object_json = json.dumps({"id": "https://bridge.test/users/x/posts/ep1"})
        row.ap_activity_json = "{}"
        session.add(
            Delivery(
                record_uri=uri,
                target_inbox="http://old-peer.example/inbox",
                activity_type="Create",
                status="sent",
            )
        )

    worker = DeliveryWorker()  # never started: tasks stay observable in-queue
    report = asyncio.run(repair(worker))
    assert report.retracted == 1

    tasks = []
    while not worker.queue.empty():
        tasks.append(worker.queue.get_nowait())
    assert any(
        t.target_inbox == "http://old-peer.example/inbox" and t.activity["type"] == "Delete"
        for t in tasks
    )


def test_repair_resends_pending_retraction_on_tombstoned_row(settings):
    """Deleting the source record after a retraction went pending must not
    hide the pending Delete from later runs — the remote Note is still up."""
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    pending = {
        "type": "Delete",
        "id": "https://bridge.test/users/x/posts/ep1#delete",
        "object": {"id": "https://bridge.test/users/x/posts/ep1", "type": "Tombstone"},
    }
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        row.ap_activity_json = json.dumps(pending)
    _run(_ev("social.popfeed.feed.review", "ep1", None, op="delete"))

    report = asyncio.run(repair(None))
    assert report.resent == 1
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None and row.deleted_at is not None
        assert row.ap_activity_json is not None
        assert json.loads(row.ap_activity_json)["type"] == "Delete"


def test_delivery_drain_waits_for_backoff_retries(settings, monkeypatch):
    """repair relies on drain(): stop() alone abandons scheduled retries, and
    a re-sync correction that failed its first attempt is never re-derived by
    a later run once the corrected Note is stored."""
    import dataclasses

    from skybridge.activitypub import delivery as delivery_mod
    from skybridge.config import set_settings

    set_settings(dataclasses.replace(settings, retry_backoff=(0, 0)))
    attempts: list[int] = []

    async def fake_post_signed(client, *, inbox, key_id, private_pem, body):
        attempts.append(len(attempts))
        return (len(attempts) >= 2, 202 if len(attempts) >= 2 else 500)

    monkeypatch.setattr(delivery_mod, "post_signed", fake_post_signed)

    async def _go():
        worker = delivery_mod.DeliveryWorker()
        worker.start()
        await worker.enqueue(
            delivery_mod.Task("at://x/y/z", "http://peer/inbox", "kid", "pem", {"type": "Delete"})
        )
        await worker.drain()
        await worker.stop()

    asyncio.run(_go())
    assert len(attempts) == 2  # the backoff retry ran before drain returned


def test_repair_dry_run_changes_nothing(settings):
    ep_uri, _ = _seed_legacy_damage(settings)
    report = asyncio.run(repair(None, dry_run=True))
    assert report.dry_run and report.retracted == 1
    assert report.would_retract == [(ep_uri, "tv_episode:tmdbId-1262477")]
    with session_scope() as session:
        ep = session.get(Record, ep_uri)
        assert ep is not None
        assert ep.ap_object_json is not None  # untouched
        assert ep.work_key == "tv_episode:tmdbId-1262477"
