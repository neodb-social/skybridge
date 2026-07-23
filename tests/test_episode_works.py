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
    assert ref.work_key == "tv_season:tmdbSeason-88401-7"
    assert ref.title == "Dark Side of the Ring - Season 7"
    assert ref.url.endswith("/catalog/tv_season/tmdbSeason-88401-7")
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
    assert ref3.work_key == "tv_season:tmdbSeason-88401-7"
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


def test_episode_review_update_clears_previously_published_ap(settings):
    _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW))
    # Simulate a Note published before the cutoff (legacy state).
    uri = f"at://{DID}/social.popfeed.feed.review/ep1"
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        row.ap_object_json = json.dumps({"id": "https://bridge.test/users/x/posts/ep1"})
        row.ap_activity_json = "{}"
    result = _run(_ev("social.popfeed.feed.review", "ep1", EP_REVIEW, op="update"))
    assert result is not None and result.activity == {}
    with session_scope() as session:
        row = session.get(Record, uri)
        assert row is not None
        assert row.ap_object_json is None and row.ap_activity_json is None


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
    assert item_tag["href"].endswith("/catalog/tv_season/tmdbSeason-88401-7")
    assert item_tag["name"] == "Dark Side of the Ring - Season 7"
    (status,) = [r for r in note["relatedWith"] if r["type"] == "Status"]
    # "watched_episodes" would normally mean complete; one episode never
    # completes a season, so the season mark is always progress.
    assert status["status"] == "progress"
    assert status["withRegardTo"].endswith("/catalog/tv_season/tmdbSeason-88401-7")
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
    assert report.remapped == 2
    assert report.resynced == 1

    with session_scope() as session:
        ep = session.get(Record, ep_uri)
        assert ep is not None
        assert ep.work_key == "tv_episode:tmdbId-7377127"
        assert ep.ap_object_json is None and ep.ap_activity_json is None
        mv = session.get(Record, mv_uri)
        assert mv is not None and mv.ap_object_json is not None
        assert mv.work_key == "movie:imdbId-tt6710474"
        note = json.loads(mv.ap_object_json)
        (item_tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
        assert item_tag["href"] == settings.catalog_id("movie", "imdbId-tt6710474")

    aliases = _aliases("tv_episode")
    assert ("episodeNumber", "4") not in aliases
    assert aliases[("tmdbId", "7377127")] == "tv_episode:tmdbId-7377127"

    # Idempotent: nothing left to retract or remap on a second run.
    again = asyncio.run(repair(None))
    assert again.retracted == 0
    assert again.remapped == 0
    assert again.resynced == 0


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
