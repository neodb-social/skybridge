"""The public pages stay bounded on a large, skewed archive.

One teal.fm author can hold hundreds of thousands of plays and one album
thousands, so a page query that reads every row of an author or a work, or
sorts them all, takes seconds. These tests run the pages' own queries through
``EXPLAIN QUERY PLAN`` on a file database upgraded the way production is:
built without the listing indexes, then reopened.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

import pytest
from skybridge import db, main
from skybridge.activitypub import objects
from skybridge.config import Settings, set_settings
from skybridge.models import Record
from sqlalchemy import event

HEAVY = "did:plc:heavy0000000000000000000"
WORK = "album:mb-popular"
LISTING_INDEXES = ("ix_record_author_posts", "ix_record_work_posts")


def _rows():
    """One author with many plays and one album with many listeners; a Note
    for one play in twenty, as play grouping leaves it."""
    note = json.dumps({"id": "https://bridge.test/x", "relatedWith": []})
    for i in range(3000):
        did = HEAVY if i < 2000 else f"did:plc:other{i % 50:020d}"
        work = WORK if i >= 1500 else f"album:mb-{i % 300}"
        published = i % 20 == 0
        yield (
            f"at://{did}/fm.teal.feed.play/r{i:08d}",
            did,
            "fm.teal.feed.play",
            f"r{i:08d}",
            note if published else None,
            work,
            f"2026-01-01 00:{i // 60 % 60:02d}:{i % 60:02d}.000000",
        )


@pytest.fixture
def upgraded(settings: Settings, tmp_path):
    """A file database holding the skewed archive, last opened by a release
    without the listing indexes, now reopened by this one."""
    set_settings(replace(settings, db_path=str(tmp_path / "skybridge.db")))
    db.init_db()
    with db.get_engine().begin() as conn:
        for name in LISTING_INDEXES:
            conn.exec_driver_sql(f"DROP INDEX {name}")
        conn.exec_driver_sql(
            "INSERT INTO record (at_uri, did, collection, rkey, ap_object_json, work_key,"
            " created_at, updated_at, source_json, op)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', 'create')",
            [(*row, row[-1]) for row in _rows()],
        )
    db.get_engine().dispose()
    db.init_db()
    yield
    db.get_engine().dispose()


def _plans(run) -> list[str]:
    """The query plan of every statement ``run`` executes."""
    statements: list[tuple[str, Any]] = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement, parameters))

    engine = db.get_engine()
    event.listen(engine, "before_cursor_execute", capture)
    try:
        run()
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    with engine.connect() as conn:
        return [
            "; ".join(row[3] for row in conn.exec_driver_sql("EXPLAIN QUERY PLAN " + s, p))
            for s, p in statements
        ]


def test_an_upgrade_builds_the_listing_indexes_with_statistics(upgraded):
    with db.get_engine().connect() as conn:
        indexes = {
            row[0]
            for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='index'")
        }
        analyzed = {row[0] for row in conn.exec_driver_sql("SELECT idx FROM sqlite_stat1")}
    assert set(LISTING_INDEXES) <= indexes
    # Statistics from the start: without them SQLite may prefer the older,
    # wider ix_record_work_key plus a sort of every row of the work.
    assert set(LISTING_INDEXES) <= analyzed


def _count(did: str) -> int:
    with db.session_scope() as session:
        return main._post_count(session, did)


@pytest.mark.parametrize(
    ("page", "run", "index"),
    [
        (
            "profile listing",
            lambda: main._recent_posts(main.AUTHOR_POSTS, Record.did == HEAVY),
            "INDEX ix_record_author_posts",
        ),
        # Labelled a plain, not covering, index search. The bytecode only
        # defers a seek it never makes, since the index's own predicate
        # already holds: 34k Notes counted in 2 ms on a 1M-row archive.
        ("post count", lambda: _count(HEAVY), "INDEX ix_record_author_posts"),
        (
            "catalog listing",
            lambda: main._recent_posts(
                main.WORK_POSTS,
                Record.work_key == WORK,
                Record.did.not_in(main._anonymous_hidden_dids()),
            ),
            "INDEX ix_record_work_posts",
        ),
        # The skew this file builds is the case that misleads the planner:
        # published Notes cluster on the popular album, so the partial index
        # averages more rows per work than ix_record_work_key does.
        ("rating average", lambda: main._count_rating(WORK), "INDEX ix_record_work_posts"),
    ],
)
def test_listings_walk_their_partial_index(upgraded, page, run, index):
    (plan,) = _plans(run)
    assert f"USING {index} " in plan, (page, plan)
    assert "TEMP B-TREE" not in plan, (page, plan)


def test_listing_results(upgraded):
    """Pinning the index changes how the rows are found, not which."""
    assert _count(HEAVY) == 100
    newest = main._recent_posts(main.AUTHOR_POSTS, Record.did == HEAVY)
    assert len(newest) == main.RECENT_POSTS
    assert newest[0].rkey == "r00001980"
    assert [r.created_at for r in newest] == sorted((r.created_at for r in newest), reverse=True)
    listed = main._recent_posts(main.WORK_POSTS, Record.work_key == WORK)
    assert len(listed) == 75
    assert {r.work_key for r in listed} == {WORK}


def _insert_plays(start: int, count: int) -> None:
    with db.get_engine().begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO record (at_uri, did, collection, rkey, source_json, op, created_at,"
            " updated_at) VALUES (?, ?, 'fm.teal.feed.play', ?, '{}', 'create',"
            " '2026-01-01 00:00:00', '2026-01-01 00:00:00')",
            [
                (f"at://did:plc:p{i % 97}/fm.teal.feed.play/{i}", f"did:plc:p{i % 97}", str(i))
                for i in range(start, start + count)
            ],
        )


def _did_stat() -> str:
    with db.get_engine().connect() as conn:
        return conn.exec_driver_sql(
            "SELECT stat FROM sqlite_stat1 WHERE idx = 'ix_record_did'"
        ).scalar_one()


def _reopen() -> None:
    db.get_engine().dispose()
    db.init_db()


def test_startup_reanalyzes_a_table_that_outgrew_its_statistics(settings, tmp_path):
    """Production's sqlite_stat1 said 356k rows for a 3.77M-row record table."""
    set_settings(replace(settings, db_path=str(tmp_path / "skybridge.db")))
    db.init_db()
    _insert_plays(0, 12_000)
    db.optimize()
    assert _did_stat().startswith("12000 ")
    _insert_plays(12_000, 20_000)  # 32k rows: past twice the recorded 12k
    _reopen()
    assert _did_stat().startswith("32000 ")
    db.get_engine().dispose()


def test_startup_leaves_current_statistics_alone(settings, tmp_path):
    """ANALYZE holds the write lock while it walks every index, so a boot
    whose statistics still fit the table skips it."""
    set_settings(replace(settings, db_path=str(tmp_path / "skybridge.db")))
    db.init_db()
    _insert_plays(0, 12_000)
    db.optimize()
    _insert_plays(12_000, 6_000)  # 18k rows: within twice the recorded 12k
    _reopen()
    assert _did_stat().startswith("12000 ")
    db.get_engine().dispose()


def test_post_lookups_read_no_table_scan(upgraded):
    def lookups() -> None:
        assert objects._record_for(HEAVY, "r00000040") is not None
        assert objects._record_for(HEAVY, "missing") is None

    for plan in _plans(lookups):
        assert "SCAN record" not in plan or "COVERING INDEX" in plan, plan
        assert "TEMP B-TREE" not in plan, plan
