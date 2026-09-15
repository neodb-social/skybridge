"""Engine/session management for the SQLite store."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from skybridge.config import get_settings
from skybridge.models import Base

log = logging.getLogger("skybridge.db")

_engine: Engine | None = None
_Session: sessionmaker[Session] | None = None

# Seconds a blocked writer waits for the lock before raising "database is
# locked". The bridge runs live ingest, delivery retries and (optionally) a
# long archive import in one process, with blocking DB work dispatched to
# threads via asyncio.to_thread — so writers do genuinely contend.
_BUSY_TIMEOUT_MS = 15_000

# Columns added after the initial release. Base.metadata.create_all() creates
# missing *tables* but never alters existing ones, so an upgraded deployment
# needs these applied by hand. Additive only: SQLite's ALTER TABLE ADD COLUMN
# is O(1) and safe to run against a live database.
_ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("record", "last_seq", "INTEGER"),
    ("cursor", "seq", "INTEGER"),
    ("bridged_actor", "inactive_status", "VARCHAR"),
    ("bridged_actor", "inactive_at", "DATETIME"),
    ("bridged_actor", "last_profile_seq", "INTEGER"),
    ("import_job", "last_segment", "VARCHAR"),
    # Booleans carry an explicit NOT NULL DEFAULT so existing rows read as
    # "not set" instead of NULL: both flags gate what we publish, and a NULL
    # third state would only invite a wrong `is False` test somewhere.
    ("bridged_actor", "hide_from_recommendations", "BOOLEAN NOT NULL DEFAULT 0"),
    ("bridged_actor", "no_unauthenticated", "BOOLEAN NOT NULL DEFAULT 0"),
    ("bridged_actor", "last_visibility_seq", "INTEGER"),
    ("record", "play_group", "VARCHAR"),
    ("record", "played_at", "DATETIME"),
    ("record", "ap_sent_at", "DATETIME"),
)


def _configure_connection(engine: Engine) -> None:
    """Apply per-connection SQLite pragmas.

    WAL lets readers proceed while a writer holds the lock, which matters
    because a long-running archive import writes continuously alongside live
    ingest; the default rollback journal would block them against each other.
    An in-memory database can't use WAL and has only one connection anyway.
    """
    in_memory = get_settings().db_path == ":memory:"

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, _record) -> None:
        cursor = dbapi_conn.cursor()
        try:
            if not in_memory:
                cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        finally:
            cursor.close()


# Indexes on columns added after the initial release. create_all() builds the
# indexes of a table it creates and nothing else, so a column _ensure_columns
# adds to an existing table arrives unindexed however the model declares it —
# and the teal.fm session lookups would then scan an author's whole play
# history. Named as SQLAlchemy names them, so a fresh database and an upgraded
# one end up with the same schema.
_ADDED_INDEXES: tuple[tuple[str, str, tuple[str, ...], str], ...] = (
    ("ix_record_play_window", "record", ("did", "work_key", "played_at"), ""),
    ("ix_record_play_session", "record", ("did", "play_group", "rkey"), ""),
    # Partial: the rows holding a session's published Note, one per session.
    (
        "ix_record_play_holder",
        "record",
        ("did", "play_group", "rkey"),
        "ap_object_json IS NOT NULL AND deleted_at IS NULL",
    ),
)


def _ensure_columns(engine: Engine) -> None:
    """Add any post-release columns, and their indexes, missing from an
    existing database."""
    with engine.begin() as conn:
        for table, column, coltype in _ADDED_COLUMNS:
            existing = {
                row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
            }
            if existing and column not in existing:
                conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        for name, table, columns, predicate in _ADDED_INDEXES:
            present = {
                row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
            }
            if set(columns) <= present:
                spec = ", ".join(columns)
                where = f" WHERE {predicate}" if predicate else ""
                conn.exec_driver_sql(
                    f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({spec}){where}"
                )


def _make_engine() -> Engine:
    settings = get_settings()
    path = settings.db_path
    # check_same_thread=False so the async delivery worker and request handlers
    # can share the engine; SQLite serializes writes internally.
    if path == ":memory:":
        # StaticPool keeps a single shared connection so the in-memory DB
        # persists across sessions (and threads) within the process.
        return create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(
        f"sqlite:///{path}", connect_args={"check_same_thread": False}, future=True
    )


def init_db(reset: bool = False) -> Engine:
    """(Re)initialise the engine + schema. Idempotent.

    Ends with a write probe so a read-only database (e.g. a bind-mounted
    data folder the container user cannot write) fails loudly at startup
    instead of on the first ingested record.
    """
    global _engine, _Session
    _engine = _make_engine()
    _configure_connection(_engine)
    _Session = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    if reset:
        Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)
    _ensure_columns(_engine)
    try:
        with _engine.begin() as conn:
            # Rewriting user_version (even unchanged) is a real header-page
            # write, without leaving any schema residue behind.
            version = conn.exec_driver_sql("PRAGMA user_version").scalar() or 0
            conn.exec_driver_sql(f"PRAGMA user_version = {int(version)}")
    except Exception as exc:
        raise RuntimeError(f"database at {get_settings().db_path} is not writable: {exc}") from exc
    return _engine


def get_engine() -> Engine:
    if _engine is None:
        init_db()
    assert _engine is not None
    return _engine


# How much of each index ANALYZE samples. SQLite's own recommendation for a
# live database: enough rows to tell a selective index from a useless one,
# few enough that the pass never blocks writers for long.
_ANALYSIS_LIMIT = 400
OPTIMIZE_INTERVAL = 24 * 60 * 60


def optimize() -> None:
    """Refresh the query planner's statistics.

    Without them SQLite guesses between indexes by shape alone, and it guesses
    wrong where two index the same columns: the teal.fm holder lookup takes
    the full ix_record_play_session over the partial ix_record_play_holder and
    walks a whole listening session, per scrobble. One pass over a populated
    table settles it. ``PRAGMA optimize`` analyses only what has changed
    enough to be worth it, so most passes do nothing at all.
    """
    with get_engine().begin() as conn:
        conn.exec_driver_sql(f"PRAGMA analysis_limit={_ANALYSIS_LIMIT}")
        conn.exec_driver_sql("PRAGMA optimize")


async def optimize_loop(interval: float = OPTIMIZE_INTERVAL) -> None:
    """Keep those statistics current; started by the app lifespan.

    A pass at startup, then daily: the tables that matter grow by ingestion,
    so a database that was empty when the process started is not the one it is
    querying a week later.
    """
    while True:
        try:
            await asyncio.to_thread(optimize)
        except Exception:
            log.exception("database optimize failed")
        await asyncio.sleep(interval)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional session context: commit on success, rollback on error."""
    if _Session is None:
        init_db()
    assert _Session is not None
    session = _Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
