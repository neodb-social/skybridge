"""Episode-shaped titles on show/season works, and repair3 for historical rows.

popfeed labels show- and season-typed records with the watched episode's
title ("Baron Noir - S1E5 - Grenelle"). A tv_show work should carry just the
show name; a tv_season work should name its season, never a single episode.
"""

from __future__ import annotations

import asyncio
import json

from skybridge.db import session_scope
from skybridge.maintenance import repair_titles
from skybridge.models import Record, Work
from skybridge.pipeline import process_event
from skybridge.translate import works

DID = "did:plc:titletest"

# The real production case: a show-typed "currently watching" listItem whose
# title is the watched episode's.
BARON_NOIR_ITEM = {
    "$type": "social.popfeed.feed.listItem",
    "title": "Baron Noir - S1E5 - Grenelle",
    "listType": "currently_watching_tv_shows",
    "addedAt": "2026-07-21T20:00:00.000Z",
    "creativeWorkType": "tv_show",
    "identifiers": {"tmdbId": "65430"},
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


# --- normalize_title ---------------------------------------------------------


def test_show_title_drops_episode_suffix():
    assert works.normalize_title("tv_show", "Baron Noir - S1E5 - Grenelle", {}) == "Baron Noir"


def test_show_title_without_episode_suffix_passes_through():
    assert works.normalize_title("tv_show", "Baron Noir", {}) == "Baron Noir"


def test_season_title_keeps_the_season():
    assert (
        works.normalize_title("tv_season", "Baron Noir - S1E5 - Grenelle", {})
        == "Baron Noir - Season 1"
    )


def test_season_title_prefers_identifier_season_number():
    assert (
        works.normalize_title("tv_season", "Baron Noir - S1E5 - Grenelle", {"seasonNumber": "2"})
        == "Baron Noir - Season 2"
    )


def test_season_title_appends_season_when_only_show_named():
    assert (
        works.normalize_title("tv_season", "Baron Noir", {"seasonNumber": "1"})
        == "Baron Noir - Season 1"
    )


def test_season_title_already_labeled_passes_through():
    assert (
        works.normalize_title("tv_season", "Baron Noir - Season 1", {"seasonNumber": "1"})
        == "Baron Noir - Season 1"
    )


def test_other_types_pass_through():
    title = "Mission: Impossible - S1E1 Special Cut"
    assert works.normalize_title("movie", title, {}) == title


# --- mint + Note use the normalized title ------------------------------------


def test_show_work_and_note_use_show_title(settings):
    result = _run(_ev("social.popfeed.feed.listItem", "it1", BARON_NOIR_ITEM))
    assert result is not None
    with session_scope() as session:
        work = session.get(Work, "tv_show:tmdbId-65430")
        assert work is not None and work.title == "Baron Noir"
    note = result.activity["object"]
    (tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
    assert tag["name"] == "Baron Noir"
    assert ">Baron Noir</a>" in note["content"]


# --- repair3: historical works minted before normalization -------------------


def test_repair3_retitles_and_resyncs(settings):
    _run(_ev("social.popfeed.feed.listItem", "it1", BARON_NOIR_ITEM))
    # Regress to the pre-normalization state: episode title on the work AND
    # baked into the published Note.
    with session_scope() as session:
        work = session.get(Work, "tv_show:tmdbId-65430")
        assert work is not None
        work.title = "Baron Noir - S1E5 - Grenelle"
        row = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/it1")
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        note["content"] = note["content"].replace("Baron Noir", "Baron Noir - S1E5 - Grenelle")
        for tag in note.get("tag", []):
            if tag.get("type") != "Hashtag":
                tag["name"] = "Baron Noir - S1E5 - Grenelle"
        row.ap_object_json = json.dumps(note)

    preview = asyncio.run(repair_titles(None, dry_run=True))
    assert preview.dry_run
    assert preview.retitled == [
        ("tv_show:tmdbId-65430", "Baron Noir - S1E5 - Grenelle", "Baron Noir")
    ]
    with session_scope() as session:  # dry-run changed nothing
        work = session.get(Work, "tv_show:tmdbId-65430")
        assert work is not None and work.title == "Baron Noir - S1E5 - Grenelle"

    report = asyncio.run(repair_titles(None))
    assert len(report.retitled) == 1
    assert report.resynced == 1
    with session_scope() as session:
        work = session.get(Work, "tv_show:tmdbId-65430")
        assert work is not None and work.title == "Baron Noir"
        row = session.get(Record, f"at://{DID}/social.popfeed.feed.listItem/it1")
        assert row is not None and row.ap_object_json is not None
        note = json.loads(row.ap_object_json)
        (tag,) = [t for t in note["tag"] if t["type"] != "Hashtag"]
        assert tag["name"] == "Baron Noir"
        assert json.loads(row.ap_activity_json or "{}").get("type") == "Update"

    # Idempotent: a second run finds nothing to retitle or re-sync.
    again = asyncio.run(repair_titles(None))
    assert again.retitled == [] and again.resynced == 0
