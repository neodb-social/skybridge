"""SQLAlchemy 2.0 ORM models backing the relay's SQLite store.

See the module docstrings on each class for what role it plays in the
ingest → translate → deliver pipeline.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class BridgedActor(Base):
    """One hosted ``Person`` actor per popfeed author (Bridgy-Fed style).

    The keypair is minted on first sight and used to sign that author's
    outbound activities so they appear "as if from this server".
    """

    __tablename__ = "bridged_actor"

    did: Mapped[str] = mapped_column(String, primary_key=True)
    handle: Mapped[str] = mapped_column(String, index=True)
    display_name: Mapped[str | None] = mapped_column(String, default=None)
    avatar: Mapped[str | None] = mapped_column(String, default=None)
    private_key_pem: Mapped[str] = mapped_column(Text)
    public_key_pem: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    # Set when the underlying atproto user opts out (see OptOut + optout.py).
    opted_out: Mapped[bool] = mapped_column(Boolean, default=False)
    opted_out_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class OptOut(Base):
    """A DID that has opted out of the bridge (after authenticating).

    Recorded separately from :class:`BridgedActor` so an opt-out persists even
    for DIDs we have never bridged (pre-emptive opt-out) and is checked by the
    pipeline before any actor is created.
    """

    __tablename__ = "opt_out"

    did: Mapped[str] = mapped_column(String, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Subscriber(Base):
    """A peer Fediverse instance that followed our relay ``Application`` actor.

    Accepted subscribers receive an ``Announce`` of every translated activity.
    """

    __tablename__ = "subscriber"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor_id: Mapped[str] = mapped_column(String, unique=True)
    inbox: Mapped[str] = mapped_column(String)
    shared_inbox: Mapped[str | None] = mapped_column(String, default=None)
    state: Mapped[str] = mapped_column(String, default="pending")  # pending|accepted
    subscribed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Follow(Base):
    """A remote actor following one specific bridged author."""

    __tablename__ = "follow"
    __table_args__ = (UniqueConstraint("local_did", "follower_actor_id", name="uq_follow"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    local_did: Mapped[str] = mapped_column(String, index=True)
    follower_actor_id: Mapped[str] = mapped_column(String)
    follower_inbox: Mapped[str] = mapped_column(String)
    follower_shared_inbox: Mapped[str | None] = mapped_column(String, default=None)
    state: Mapped[str] = mapped_column(String, default="accepted")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Work(Base):
    """A minted catalog item that NeoDB ``withRegardTo`` links point at."""

    __tablename__ = "work"

    work_key: Mapped[str] = mapped_column(String, primary_key=True)  # "<type>:<id>"
    creative_work_type: Mapped[str] = mapped_column(String)
    title: Mapped[str | None] = mapped_column(String, default=None)
    poster_url: Mapped[str | None] = mapped_column(String, default=None)
    identifiers_json: Mapped[str] = mapped_column(Text, default="{}")
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WorkIdentifier(Base):
    """Secondary index: every known external identifier -> its minted work.

    Records for the same work can carry different identifier subsets (e.g. a
    review with imdb+tmdb ids but a listItem with only the tmdb id). Looking up
    each incoming identifier here lets them all resolve to one catalog entry
    instead of minting duplicates keyed by whichever id happened to win.
    """

    __tablename__ = "work_identifier"

    creative_work_type: Mapped[str] = mapped_column(String, primary_key=True)
    id_key: Mapped[str] = mapped_column(String, primary_key=True)
    id_value: Mapped[str] = mapped_column(String, primary_key=True)
    work_key: Mapped[str] = mapped_column(String, index=True)


class Record(Base):
    """Archive of every processed atproto record + its translated AP forms.

    Keyed by the atproto ``at://`` URI so updates mutate and deletes tombstone
    the same row. Powers the archive view, dedup, and update/delete linkage.
    """

    __tablename__ = "record"

    at_uri: Mapped[str] = mapped_column(String, primary_key=True)
    did: Mapped[str] = mapped_column(String, index=True)
    collection: Mapped[str] = mapped_column(String, index=True)
    rkey: Mapped[str] = mapped_column(String)
    cid: Mapped[str | None] = mapped_column(String, default=None)
    source_json: Mapped[str] = mapped_column(Text, default="{}")
    ap_object_json: Mapped[str | None] = mapped_column(Text, default=None)
    ap_activity_json: Mapped[str | None] = mapped_column(Text, default=None)
    op: Mapped[str] = mapped_column(String, default="create")  # create|update|delete
    work_key: Mapped[str | None] = mapped_column(String, index=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class Delivery(Base):
    """Per-target outbound delivery log; drives retries and stats."""

    __tablename__ = "delivery"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    record_uri: Mapped[str] = mapped_column(String, index=True)
    target_inbox: Mapped[str] = mapped_column(String)
    activity_type: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="pending")  # pending|sent|failed
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    response_code: Mapped[int | None] = mapped_column(Integer, default=None)
    last_attempt: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Cursor(Base):
    """Single-row Jetstream ``time_us`` cursor for resumable ingestion."""

    __tablename__ = "cursor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    time_us: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
