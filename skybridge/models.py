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
    # Set from a Jetstream `account` event reporting active: false with a
    # status other than "deleted" (deactivated / suspended / takendown). A
    # reversible gate: ingestion skips the DID while it is set and nothing is
    # retracted, so the account resumes cleanly if it comes back. A true
    # deletion purges instead — see pipeline._process_account.
    inactive_status: Mapped[str | None] = mapped_column(String, default=None)
    inactive_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    # High-water mark for profile commits. Profile edits are identity metadata
    # and deliberately never land in the Record archive, so they have no row to
    # carry Record.last_seq — without this a replayed profile event would
    # re-emit Update(Person) to every follower on each import.
    last_profile_seq: Mapped[int | None] = mapped_column(Integer, default=None)


class HandleAlias(Base):
    """A handle a bridged actor used to hold, kept resolvable after a rename.

    Actor and post URLs are keyed on the handle (``/users/<handle>``), so a
    rename would otherwise 404 every follower that stored the old actor id and
    every object id already federated. Route lookups fall back to this table
    and redirect to the live handle. An alias is dropped as soon as another DID
    takes the name for real: on atproto a handle points at one DID at a time.
    """

    __tablename__ = "handle_alias"

    handle: Mapped[str] = mapped_column(String, primary_key=True)
    did: Mapped[str] = mapped_column(String, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class OptOut(Base):
    """A DID that has opted out of the bridge (after authenticating).

    Recorded separately from :class:`BridgedActor` so an opt-out persists even
    for DIDs we have never bridged (pre-emptive opt-out) and is checked by the
    pipeline before any actor is created.
    """

    __tablename__ = "opt_out"

    did: Mapped[str] = mapped_column(String, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Relay(Base):
    """An external Fediverse relay we subscribe to as a client (Mastodon-style).

    We ``Follow`` the relay's ``as:Public`` and, once it ``Accept``s, deliver
    every author-signed post plus forwarded ``Like`` to its inbox.
    """

    __tablename__ = "relay"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    inbox: Mapped[str] = mapped_column(String, unique=True)
    follow_activity_id: Mapped[str | None] = mapped_column(String, default=None)
    # pending|accepted|rejected|unsubscribed (removed from SKYBRIDGE_RELAYS)
    state: Mapped[str] = mapped_column(String, default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Like(Base):
    """A ``Like`` received from a peer AP server for one of our local posts.

    Stored for dedup and forwarded (Announce-wrapped), signed by the service
    actor, to every accepted :class:`Relay`. No uniqueness on
    ``(actor_id, object_id)``: a forged Like must never be able to shadow a
    victim's genuine later Like on the same post.
    """

    __tablename__ = "like"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    activity_id: Mapped[str] = mapped_column(String, unique=True)
    actor_id: Mapped[str] = mapped_column(String, index=True)
    object_id: Mapped[str] = mapped_column(String, index=True)  # local post URL
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


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
    # Highest Jetstream v2 `seq` applied to this row: the high-water mark that
    # keeps a replayed archive event from overwriting newer live state, and
    # makes the live tail's at-least-once redelivery idempotent. NULL on rows
    # written before v2 ingestion (see pipeline._is_stale).
    last_seq: Mapped[int | None] = mapped_column(Integer, index=True, default=None)
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
    """Single-row Jetstream cursor for resumable ingestion.

    ``time_us`` is the v1 unit (microseconds since the epoch); ``seq`` is v2's
    monotonic event counter. Both are kept because a v2 host accepts either —
    it tells them apart by magnitude — so an existing deployment upgrading to
    v2 resumes from its stored ``time_us`` and starts recording ``seq``.
    """

    __tablename__ = "cursor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    seq: Mapped[int | None] = mapped_column(Integer, default=None)
    time_us: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SeenCollection(Base):
    """An atproto collection observed under a bridged namespace wildcard.

    Populated by ``python -m skybridge discover`` (see
    :mod:`skybridge.atproto.discover`), which subscribes to
    ``social.popfeed.*``/``buzz.bookhive.*`` rather than the explicit ingest
    list. Lets a new collection announce itself instead of being noticed by
    hand — the maintenance problem the commentary above WANTED_COLLECTIONS
    describes.
    """

    __tablename__ = "seen_collection"

    nsid: Mapped[str] = mapped_column(String, primary_key=True)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    # One representative record, so the shape can be judged without a re-run.
    sample_json: Mapped[str | None] = mapped_column(Text, default=None)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ImportJob(Base):
    """A historical archive import (Jetstream v2 Network Replay).

    One row per requested import, kept across restarts so a run interrupted by
    a metering 429 or a redeploy resumes from where it stopped rather than
    re-downloading (and re-paying for) what it already has.
    """

    __tablename__ = "import_job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # pending|running|paused|done|cancelled|failed
    state: Mapped[str] = mapped_column(String, default="pending", index=True)
    after_seq: Mapped[int] = mapped_column(Integer, default=0)
    # Upper bound, pinned at request time to the live cursor so the import can
    # never contend with the live tail for the same events.
    before_seq: Mapped[int] = mapped_column(Integer, default=0)
    # Planner progress: resume the plan loop from here (see archive.py).
    planned_through_seq: Mapped[int] = mapped_column(Integer, default=0)
    sealed_tip_seq: Mapped[int] = mapped_column(Integer, default=0)
    last_applied_seq: Mapped[int] = mapped_column(Integer, default=0)
    segments_done: Mapped[int] = mapped_column(Integer, default=0)
    # Name of the last segment fully applied. planned_through_seq only moves
    # when an entire plan page completes, and a page can hold the whole
    # archive — so without this a failure at 84% would restart from zero.
    last_segment: Mapped[str | None] = mapped_column(String, default=None)
    segments_total: Mapped[int] = mapped_column(Integer, default=0)
    events_applied: Mapped[int] = mapped_column(Integer, default=0)
    bytes_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    # Whether to fan the imported records out to peers. Off by default:
    # replaying years of history would flood every subscriber with Creates.
    deliver: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
