"""Episode/season work identity + the tv_episode federation cutoff.

Regression suite for the mis-mapping where positional identifier keys
(episodeNumber/seasonNumber/tmdbTvSeriesId) acted as global work aliases, so
"Dark Side of the Ring S7E4" resolved to "The Expanse S2E4" (both episode 4).
Also covers the follow-up policy: episode marks are never federated — episode
reviews archive silently, episode list-adds bridge as watching the season.
"""

from __future__ import annotations

import asyncio
import json

from skybridge.db import session_scope
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

MOVIE_REVIEW = {
    "$type": "social.popfeed.feed.review",
    "title": "Everything Everywhere All at Once",
    "text": "",
    "rating": 9,
    "createdAt": "2026-07-03T17:16:24.038Z",
    "identifiers": {"imdbId": "tt6710474", "tmdbId": "545611"},
    "creativeWorkType": "movie",
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
    # a crashed/failed delivery leaves a discoverable record of it.
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


def test_update_into_episode_resyncs_the_prior_pair(settings):
    """A published paired record updated into an episode leaves its old pair:
    the surviving partner must republish under its own rkey (the pair's Note
    was just retracted with the mover), not stay AP-silent forever."""
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    item = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Everything Everywhere All at Once",
        "listType": "watched_movies",
        "addedAt": "2026-07-03T17:16:55.308Z",
        "identifiers": {"tmdbId": "545611"},
        "creativeWorkType": "movie",
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", item))
    rv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    it_uri = f"at://{DID}/social.popfeed.feed.listItem/it1"
    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        it = session.get(Record, it_uri)
        assert rv is not None and rv.ap_object_json is not None  # pair anchor
        assert it is not None and it.ap_object_json is None  # folded partner
        old_note_id = json.loads(rv.ap_object_json)["id"]

    # popfeed reassigns the record to an episode: the anchor Note is
    # retracted, and the partner takes over the movie pair.
    result = _run(_ev("social.popfeed.feed.review", "mv1", EP_REVIEW, op="update"))
    assert result is not None and result.activity.get("type") == "Delete"
    assert result.activity["object"]["id"] == old_note_id

    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        it = session.get(Record, it_uri)
        assert rv is not None and it is not None
        assert rv.work_key == "tv_episode:tmdbId-7377127"
        assert rv.ap_object_json is None  # retracted (pending Delete kept)
        assert it.ap_object_json is not None  # partner republished
        note = json.loads(it.ap_object_json)
        (status,) = [r for r in note["relatedWith"] if r["type"] == "Status"]
        assert status["status"] == "complete"
        assert status["withRegardTo"] == settings.catalog_id("movie", "imdbId-tt6710474")


def test_episode_item_moving_seasons_resyncs_the_old_season(settings):
    """An episode item updated to the next season takes the pair Note along;
    the season it left must republish from its surviving items, not go
    silent while still being watched."""
    ep5_item = {
        **EP_LIST_ITEM,
        "title": "Dark Side of the Ring - S7E5 - The Dynamite Kid",
        "identifiers": {**EP_IDENTIFIERS, "episodeNumber": 5, "tmdbId": "7377128"},
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", EP_LIST_ITEM))  # anchors S7 Note
    _run(_ev("social.popfeed.feed.listItem", "it2", ep5_item))  # folds into it
    s8_item = {
        **EP_LIST_ITEM,
        "title": "Dark Side of the Ring - S8E1 - Premiere",
        "identifiers": {
            "episodeNumber": 1,
            "seasonNumber": 8,
            "tmdbId": "8000001",
            "tmdbTvSeriesId": "88401",
        },
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", s8_item, op="update"))

    with session_scope() as session:
        it1 = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/it1")
        it2 = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/it2")
        assert it1 is not None and it2 is not None
        assert it1.work_key == "tv_season:tmdbId-88401-season-8"
        assert it2.work_key == "tv_season:tmdbId-88401-season-7"
        # The S7 pair republished from the surviving item.
        assert it2.ap_object_json is not None
        note = json.loads(it2.ap_object_json)
        (status,) = [r for r in note["relatedWith"] if r["type"] == "Status"]
        assert status["withRegardTo"].endswith("/catalog/tv_season/tmdbId-88401-season-7")


def test_anchor_moving_into_existing_pair_retracts_its_duplicate_note(settings):
    """One Note per (author, work): an anchoring record moving into a pair
    that already holds a published Note must retract its own Note instead of
    leaving two published for the same work."""
    s8_item = {
        **EP_LIST_ITEM,
        "title": "Dark Side of the Ring - S8E1 - Premiere",
        "identifiers": {
            "episodeNumber": 1,
            "seasonNumber": 8,
            "tmdbId": "8000001",
            "tmdbTvSeriesId": "88401",
        },
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", EP_LIST_ITEM))  # anchors S7 Note
    _run(_ev("social.popfeed.feed.listItem", "it2", s8_item))  # anchors S8 Note
    it2_uri = f"at://{DID}/social.popfeed.feed.listItem/it2"
    with session_scope() as session:
        it2 = session.get(Record, it2_uri)
        assert it2 is not None and it2.ap_object_json is not None
        s8_note_id = json.loads(it2.ap_object_json)["id"]

    # it2 moves into S7, which already has it1's Note.
    s7e5 = {
        **EP_LIST_ITEM,
        "title": "Dark Side of the Ring - S7E5 - The Dynamite Kid",
        "identifiers": {**EP_IDENTIFIERS, "episodeNumber": 5, "tmdbId": "7377128"},
    }
    result = _run(_ev("social.popfeed.feed.listItem", "it2", s7e5, op="update"))
    # The S7 pair is served by it1's existing Note (an Update, not a second
    # Create), and it2's S8 Note was retracted.
    assert result is not None and result.activity.get("type") == "Update"
    assert result.activity["object"]["id"].endswith("/posts/it1")
    with session_scope() as session:
        it1 = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/it1")
        it2 = session.get(Record, it2_uri)
        assert it1 is not None and it2 is not None
        assert it1.ap_object_json is not None
        assert it2.ap_object_json is None
        assert it2.ap_activity_json is not None
        delete = json.loads(it2.ap_activity_json)
        assert delete["type"] == "Delete"
        assert delete["object"]["id"] == s8_note_id


def test_flip_back_from_episode_anchors_on_an_unburned_partner(settings):
    """A record flipped to an episode had its Note Deleted; when it flips
    back, the pair's fresh Create must anchor on a partner whose object id
    was never tombstoned — peers may cache the Tombstone and reject a Create
    reusing the Deleted id."""
    _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW))
    item = {
        "$type": "social.popfeed.feed.listItem",
        "title": "Everything Everywhere All at Once",
        "listType": "watched_movies",
        "addedAt": "2026-07-03T17:16:55.308Z",
        "identifiers": {"tmdbId": "545611"},
        "creativeWorkType": "movie",
    }
    _run(_ev("social.popfeed.feed.listItem", "it1", item))
    rv_uri = f"at://{DID}/social.popfeed.feed.review/mv1"
    it_uri = f"at://{DID}/social.popfeed.feed.listItem/it1"
    # Simulate a completed episode flip whose prior-pair resync never ran
    # (crash): the review is a pending retraction on an episode work, the
    # partner is AP-silent.
    with session_scope() as session:
        rv = session.get(Record, rv_uri)
        it = session.get(Record, it_uri)
        assert rv is not None and it is not None
        rv.work_key = "tv_episode:tmdbId-7377127"
        rv.ap_object_json = None
        rv.ap_activity_json = json.dumps({"type": "Delete", "object": {"id": "x"}})
        it.ap_object_json = None
        it.ap_activity_json = None

    result = _run(_ev("social.popfeed.feed.review", "mv1", MOVIE_REVIEW, op="update"))
    assert result is not None and result.activity.get("type") == "Create"
    # Anchored on the partner's never-Deleted id, not the burned review id.
    assert result.activity["object"]["id"].endswith("/posts/it1")
    with session_scope() as session:
        it = session.get(Record, it_uri)
        assert it is not None and it.ap_object_json is not None


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
