"""Engine/session management for the SQLite store."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from skybridge.config import get_settings
from skybridge.models import Base

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


def _ensure_columns(engine: Engine) -> None:
    """Add any post-release columns missing from an existing database."""
    with engine.begin() as conn:
        for table, column, coltype in _ADDED_COLUMNS:
            existing = {
                row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
            }
            if existing and column not in existing:
                conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


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
