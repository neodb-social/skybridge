"""End-to-end pipeline against the captured real-world fixture (offline)."""

from __future__ import annotations

import asyncio

from skybridge.atproto.replay import read_events, replay_file
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, Work
from skybridge.stats import collect_stats
from sqlalchemy import func, select


def _counts_from_fixture(path):
    wanted = {
        "social.popfeed.feed.post",
        "social.popfeed.feed.list",
        "social.popfeed.feed.listItem",
    }
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
    assert deleted[0].ap_activity_json is not None


def test_stats_reflect_replay(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    _, distinct_uris, n_deletes = _counts_from_fixture(fixture_path)
    stats = collect_stats()
    assert stats["records_total"] == distinct_uris
    assert stats["records_active"] == distinct_uris - n_deletes
    assert stats["works"] > 0
    assert set(stats["records_by_collection"]).issubset(
        {
            "social.popfeed.feed.post",
            "social.popfeed.feed.list",
            "social.popfeed.feed.listItem",
        }
    )


def test_translated_activity_is_neodb_shaped(settings, fixture_path):
    results = asyncio.run(replay_file(fixture_path, allow_network=False))
    posts = [
        r for r in results if r.collection == "social.popfeed.feed.post" and r.operation == "create"
    ]
    assert posts
    activity = posts[0].activity
    assert activity["type"] == "Create"
    assert activity["object"]["type"] == "Note"
    assert "@context" in activity
