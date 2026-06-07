"""Engine/session management for the SQLite store."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from skybridge.config import get_settings
from skybridge.models import Base

_engine: Engine | None = None
_Session: sessionmaker[Session] | None = None


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
    return create_engine(
        f"sqlite:///{path}", connect_args={"check_same_thread": False}, future=True
    )


def init_db(reset: bool = False) -> Engine:
    """(Re)initialise the engine + schema. Idempotent."""
    global _engine, _Session
    _engine = _make_engine()
    _Session = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    if reset:
        Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)
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
