"""The ingest → translate → persist → deliver pipeline.

A single :func:`process_event` handles one Jetstream-shaped commit event,
regardless of whether it came from the live firehose or a replayed fixture.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from skybridge import optout, telemetry
from skybridge.activitypub import actors
from skybridge.activitypub.delivery import DeliveryWorker, fanout, fanout_actor_update
from skybridge.atproto import events, identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, utcnow
from skybridge.translate import neodb, teal, works

log = logging.getLogger("skybridge.pipeline")

# Collections we archive (full source, dedup, stats) but do NOT translate or
# deliver: we don't support emitting AP posts for lists/collections yet. The
# records are kept so listItem.listUri stays resolvable and so a future
# NeoDB-Collection mapping can backfill from the archive.
ARCHIVE_ONLY_COLLECTIONS = frozenset({"social.popfeed.feed.list"})

# One popfeed action ("watched + rated") writes both a review and a listItem.
# The pair is bridged as ONE AP Note per (author, work): whichever record
# publishes first anchors the Note id (/users/<handle>/posts/<rkey> — rkeys
# are immutable, unlike work identifiers, which popfeed may reassign), and any
# later change to either record re-derives the combined Note and sends an
# Update with that same id (rewatches included). Deleting the anchoring record
# Deletes the Note — the surviving partner re-publishes under its own rkey on
# its next event — while deleting the partner just re-derives the Note.
# listItems whose listType has no shelf status (plain membership) are
# archived without AP emission. See _sync_pair.
_REVIEW_COLLECTION = "social.popfeed.feed.review"
_LIST_ITEM_COLLECTION = "social.popfeed.feed.listItem"
_PAIRED_COLLECTIONS = (_REVIEW_COLLECTION, _LIST_ITEM_COLLECTION)

# A profile edit refreshes the bridged actor's display name/avatar (see
# identity.refresh_actor) and emits an Update(Person) to that author's own
# followers. It carries no per-work content, so it never touches the Record
# archive and never mints an actor of its own — see _process_profile.
_PROFILE_COLLECTION = "social.popfeed.actor.profile"

# Bluesky's "hide my posts from algorithmic recommendations" declaration. Takes
# the same path as a profile edit — never archived, never mints an actor, emits
# an Update(Person) to that author's own followers — see _process_visibility.
_VISIBILITY_COLLECTION = identity.VISIBILITY_COLLECTION

# Jetstream ``identity`` events (handle changes) carry no collection at all.
# Reported on Processed.collection so /stats and logs can tell them apart from
# a commit.
_IDENTITY_KIND = "identity"

# Jetstream v2 ``account`` events report atproto account lifecycle. Like
# identity events they carry no collection and arrive for the whole network.
_ACCOUNT_KIND = "account"

# The only status we treat as permanent. A deleted repo is gone, so everything
# bridged from it is retracted. Every other inactive status
# (deactivated/suspended/takendown) is reversible, so it gates ingestion
# without retracting: a user who deactivates for a week and returns should
# find their federated history intact rather than destroyed.
_ACCOUNT_DELETED = "deleted"


@dataclass
class Processed:
    at_uri: str
    operation: str
    collection: str
    activity: dict[str, Any]
    delivered: int = 0


def _is_stale(at_uri: str, seq: int | None, *, from_archive: bool) -> bool:
    """Would applying this event move a record backwards in time?

    Jetstream orders events by a monotonic ``seq`` and delivers at-least-once,
    so the same event can arrive twice (an inclusive-cursor reconnect) and a
    replayed archive event can arrive long after the live tail has moved on.
    Comparing against the row's high-water mark makes both harmless.

    A row with no ``last_seq`` predates v2 ingestion, so there is nothing to
    compare against: a live event is current by definition and applies, while
    an archive event is rejected — an import is bounded strictly below the live
    cursor (see :mod:`skybridge.atproto.archive`), so anything the live path
    already wrote is necessarily newer.
    """
    if seq is None:
        return False
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None:
            return False
        if row.last_seq is None:
            return from_archive
        return seq <= row.last_seq


def _at_uri(did: str, collection: str, rkey: str) -> str:
    return f"at://{did}/{collection}/{rkey}"


def _unlisted(did: str) -> bool:
    """Should this author's posts be addressed unlisted rather than public?

    True for an author carrying Bluesky's ``!no-unauthenticated`` label. Read
    per translation rather than threaded through the call chain: the flag can
    change between two events for the same author, and every one of these
    paths is already several queries deep.
    """
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return actor is not None and bool(actor.no_unauthenticated)


def _wanted(collection: str) -> bool:
    return collection in get_settings().wanted_collections


def _item_status(source: dict) -> str | None:
    return neodb.list_item_status(source)


def _source_dict(source_json: str | None) -> dict | None:
    try:
        source = json.loads(source_json or "{}")
    except (TypeError, ValueError):
        return None
    return source if isinstance(source, dict) else None


def _contributes_row(collection: str, source_json: str | None) -> bool:
    """Same contribution test as _pair_rows, on raw row columns."""
    if collection == _REVIEW_COLLECTION:
        return True
    source = _source_dict(source_json)
    return source is not None and _item_status(source) is not None


def _pair_has_other_holder(did: str, work_key: str, *, exclude_uri: str) -> bool:
    """Does (author, work) already have a published contributing Note on a
    row other than *exclude_uri*? (Published status-less membership rows hold
    standalone Notes and don't count.)"""
    with session_scope() as session:
        rows = session.execute(
            select(Record.collection, Record.source_json).where(
                Record.did == did,
                Record.work_key == work_key,
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.ap_object_json.is_not(None),
                Record.deleted_at.is_(None),
                Record.at_uri != exclude_uri,
            )
        ).all()
    return any(_contributes_row(collection, source_json) for collection, source_json in rows)


def _stored_note_id(ap_object_json: str | None) -> str | None:
    """The id of a stored Note: the one peers actually received.

    Object ids are minted from the handle of the day, so they must be read
    back rather than recomputed — the author may have been renamed since.
    """
    note = _source_dict(ap_object_json) if ap_object_json else None
    return note.get("id") if note else None


def _prior_state(at_uri: str) -> tuple[str | None, str | None]:
    """(published Note id, work_key) of the record before this event.

    The Note id is read from the stored AP object — the id peers actually
    received — never recomputed from the current handle, so a retraction
    always names the right object. Tombstoned rows yield (None, None): their
    Note was already retracted on delete.
    """
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None or row.deleted_at is not None:
            return None, None
        work_key = row.work_key
        ap_object_json = row.ap_object_json
    return _stored_note_id(ap_object_json), work_key


def _contributes(collection: str, record: dict) -> bool:
    """Does this record contribute to the single per-(author, work) Note?"""
    if collection == _REVIEW_COLLECTION:
        return True
    return collection == _LIST_ITEM_COLLECTION and _item_status(record) is not None


async def process_event(
    event: dict[str, Any],
    *,
    worker: DeliveryWorker | None = None,
    allow_network: bool = True,
    from_archive: bool = False,
) -> Processed | None:
    """Process one commit or identity event. Returns ``None`` if filtered/ignored.

    ``from_archive`` marks events replayed from the Jetstream archive rather
    than read live; it only relaxes/tightens the staleness rule (see
    :func:`_is_stale`) and never changes how a record translates.
    """
    # Accept either Jetstream dialect: v2 envelopes are mapped onto the v1
    # shape the rest of this module (and the fixtures) are written against.
    normalized = events.normalize(event)
    if normalized is None:
        return None
    event = normalized
    if event.get("kind") == _IDENTITY_KIND:
        # Handle change: no collection filter applies (Jetstream sends identity
        # events for the whole network), so this branches before _wanted.
        return await _process_identity(event, worker=worker)
    if event.get("kind") == _ACCOUNT_KIND:
        # Account lifecycle, likewise network-wide and collection-less.
        return await _process_account(event, worker=worker)
    if event.get("kind") != "commit":
        return None
    collection = event.get("collection", "")
    if not _wanted(collection):
        return None

    did = event["did"]
    operation = event.get("operation", "create")

    # Honour opt-outs before creating any actor or persisting anything.
    if optout.is_opted_out(did):
        return None

    # An account currently deactivated/suspended/taken down is gated: its
    # records stay as they are, but nothing new is bridged until it is active
    # again (see _process_account).
    if _is_gated(did):
        return None

    # Ingest-volume metric: ticks for every wanted commit event from non-opted-out
    # authors, regardless of what the pipeline later does with it (archive-only,
    # merge, ...).
    telemetry.record_ingested(collection, operation)

    rkey = event.get("rkey", "")
    event_time = event.get("time")
    seq = event.get("seq")
    at_uri = _at_uri(did, collection, rkey)

    if _is_stale(at_uri, seq, from_archive=from_archive):
        # A redelivered or archive-replayed event for a record that has since
        # moved on. Dropping it here keeps a stale Create from resurrecting a
        # tombstoned Note on peers.
        return None

    if collection == _PROFILE_COLLECTION:
        # A profile edit only ever refreshes an existing actor (see
        # identity.refresh_actor) — it must never mint one, so this branches
        # before ensure_actor is called below.
        return await _process_profile(
            at_uri=at_uri,
            did=did,
            operation=operation,
            record=event.get("record") or {},
            event_time=event_time,
            seq=seq,
            worker=worker,
            allow_network=allow_network,
        )

    if collection == _VISIBILITY_COLLECTION:
        # Same rule as a profile edit, and for the same reason: a preference
        # is not content, so it branches before ensure_actor. This one arrives
        # for the whole network, so most events land on a DID we do not bridge
        # and stop inside _process_visibility.
        return await _process_visibility(
            at_uri=at_uri,
            did=did,
            operation=operation,
            record=event.get("record") or {},
            seq=seq,
            worker=worker,
        )

    ident = identity.ensure_actor(did, allow_network=allow_network)
    handle = ident.handle

    if collection in ARCHIVE_ONLY_COLLECTIONS:
        return _process_archive_only(at_uri, did, collection, rkey, event, operation, seq)

    if operation == "delete":
        return await _process_delete(at_uri, did, collection, rkey, handle, worker, seq)

    record = event.get("record") or {}
    ref = works.mint(record)

    if collection in teal.PLAY_COLLECTIONS:
        # A scrobble: one Note per listening session, not per play. See
        # _process_play for the grouping rules.
        return await _process_play(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            handle=handle,
            record=record,
            ref=ref,
            operation=operation,
            seq=seq,
            cid=event.get("cid"),
            event_time=event_time,
            worker=worker,
        )

    is_episode_work = ref is not None and ref.work_type == works.EPISODE_TYPE
    is_unresolved_episode = ref is None and record.get("creativeWorkType") == works.EPISODE_TYPE
    if is_episode_work or is_unresolved_episode:
        # NeoDB doesn't federate episode-level marks. Episode listItems are
        # bridged as season activity (works.season_view) and never reach this
        # branch; whatever still resolves to a tv_episode work (reviews, or an
        # episode that can't name its season) is archived without AP emission
        # — including episode records whose identifiers can't mint a work at
        # all, which would otherwise fall through and publish a generic Note.
        # A Note this record already published (legacy pre-cutoff state, or a
        # record updated into an episode) is retracted: the Delete — targeting
        # the stored Note id — is persisted in ap_activity_json BEFORE the
        # fanout, so a crash or failed delivery leaves a discoverable pending
        # retraction (an unpublished row carrying a Delete) rather than a Note
        # stranded on peers with nothing recording that it still needs one.
        note_id, prior_key = _prior_state(at_uri)
        retraction = None
        if note_id is not None:
            _, retraction = neodb.translate(
                did=did,
                unlisted=_unlisted(did),
                handle=handle,
                collection=collection,
                rkey=rkey,
                record=None,
                operation="delete",
                event_time=None,
                prior_object_id=note_id,
            )
        new_key = ref.work_key if ref is not None else None
        _persist(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            seq=seq,
            cid=event.get("cid"),
            source=record,
            note=None,
            activity=retraction,
            operation=operation,
            work_key=new_key,
            # No new retraction: keep any pending (not yet delivered) one
            # from an earlier event rather than wiping it.
            preserve_ap=retraction is None,
        )
        delivered = 0
        if worker is not None and retraction is not None:
            delivered = await fanout(worker, record_uri=at_uri, did=did, activity=retraction)
        if prior_key and prior_key != new_key and not works.is_episode_key(prior_key):
            # The record left a non-episode pair (an update turned it into an
            # episode): re-derive that pair so a surviving partner republishes
            # under its own rkey (its anchor Note may just have been
            # retracted) or drops this record's now-stale contribution.
            trigger_uri = _pair_trigger(did, prior_key)
            prior_pair = None
            if trigger_uri is not None:
                prior_pair = _sync_pair(
                    did=did, work_key=prior_key, handle=handle, trigger_uri=trigger_uri
                )
            if worker is not None and prior_pair is not None:
                delivered += await fanout(
                    worker,
                    record_uri=prior_pair.anchor_uri,
                    did=did,
                    activity=prior_pair.activity,
                )
        return Processed(at_uri, operation, collection, retraction or {}, delivered)

    is_membership_only = ref is None or not _contributes(collection, record)
    if collection == _LIST_ITEM_COLLECTION and is_membership_only:
        # Collection membership (a status-less list) or an item with no
        # resolvable work: archived like feed.list itself, no AP emission —
        # NeoDB Collections and their membership are not bridged yet.
        _persist(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            seq=seq,
            cid=event.get("cid"),
            source=record,
            note=None,
            activity=None,
            operation=operation,
            work_key=ref.work_key if ref else None,
        )
        return Processed(at_uri, operation, collection, {})

    if ref is not None and collection in _PAIRED_COLLECTIONS and _contributes(collection, record):
        # Persist the source first (keeping any Note this row already
        # anchors), then re-derive the pair's single Note.
        note_id, prior_key = _prior_state(at_uri)
        retraction = None
        if (
            note_id is not None
            and prior_key != ref.work_key
            and _pair_has_other_holder(did, ref.work_key, exclude_uri=at_uri)
        ):
            # This record anchors a Note but is moving into a pair that
            # already has one: carrying its Note along would leave two
            # published Notes for one (author, work). Retract it — the
            # destination's existing holder absorbs the contribution below.
            _, retraction = neodb.translate(
                did=did,
                unlisted=_unlisted(did),
                handle=handle,
                collection=collection,
                rkey=rkey,
                record=None,
                operation="delete",
                event_time=None,
                prior_object_id=note_id,
            )
        _persist(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            seq=seq,
            cid=event.get("cid"),
            source=record,
            note=None,
            activity=retraction,
            operation=operation,
            work_key=ref.work_key,
            # The retraction replaces the stored AP forms (pending-Delete
            # shape); otherwise the row's Note or pending state is kept.
            preserve_ap=retraction is None,
        )
        delivered = 0
        if worker is not None and retraction is not None:
            delivered += await fanout(worker, record_uri=at_uri, did=did, activity=retraction)
        pair = _sync_pair(did=did, work_key=ref.work_key, handle=handle, trigger_uri=at_uri)
        activity = pair.activity if pair is not None else None
        if worker is not None and pair is not None:
            delivered += await fanout(
                worker, record_uri=pair.anchor_uri, did=did, activity=pair.activity
            )
        if prior_key and prior_key != ref.work_key and not works.is_episode_key(prior_key):
            # The update moved this record to a different work (popfeed
            # reassigned identifiers, or an episode item advanced to the next
            # season): re-derive the pair it left, so a surviving partner
            # republishes or drops this record's stale contribution.
            trigger_uri = _pair_trigger(did, prior_key)
            prior_pair = None
            if trigger_uri is not None:
                prior_pair = _sync_pair(
                    did=did, work_key=prior_key, handle=handle, trigger_uri=trigger_uri
                )
            if worker is not None and prior_pair is not None:
                delivered += await fanout(
                    worker,
                    record_uri=prior_pair.anchor_uri,
                    did=did,
                    activity=prior_pair.activity,
                )
        return Processed(at_uri, operation, collection, activity or {}, delivered)

    note, activity = neodb.translate(
        did=did,
        unlisted=_unlisted(did),
        handle=handle,
        collection=collection,
        rkey=rkey,
        record=record,
        operation=operation,
        event_time=event_time,
        ref=ref,
        # Keep an already-published Note on its own id (None re-mints, which
        # is what a never-published or revived row wants).
        prior_object_id=_prior_state(at_uri)[0],
    )
    _persist(
        at_uri=at_uri,
        did=did,
        collection=collection,
        rkey=rkey,
        seq=seq,
        cid=event.get("cid"),
        source=record,
        note=note,
        activity=activity,
        operation=operation,
        work_key=ref.work_key if ref else None,
    )
    delivered = 0
    if worker is not None:
        delivered = await fanout(worker, record_uri=at_uri, did=did, activity=activity)
    return Processed(at_uri, operation, collection, activity, delivered)


def _pair_rows(did: str, work_key: str) -> tuple[Record | None, Record | None, Record | None]:
    """(latest review, latest status-bearing listItem, current Note holder)
    among the active paired records for one (author, work)."""
    with session_scope() as session:
        rows = list(
            session.scalars(
                select(Record)
                .where(
                    Record.did == did,
                    Record.work_key == work_key,
                    Record.collection.in_(_PAIRED_COLLECTIONS),
                    Record.deleted_at.is_(None),
                )
                .order_by(Record.created_at.desc())
            )
        )
    review_row = next((r for r in rows if r.collection == _REVIEW_COLLECTION), None)
    item_row = next(
        (
            r
            for r in rows
            if r.collection == _LIST_ITEM_COLLECTION
            and _item_status(json.loads(r.source_json or "{}"))
        ),
        None,
    )
    # The anchor may be an older row (e.g. the first review of a rewatch);
    # status-less items hold standalone Notes and never anchor the pair.
    holder = next(
        (
            r
            for r in rows
            if r.ap_object_json
            and (
                r.collection == _REVIEW_COLLECTION
                or _item_status(json.loads(r.source_json or "{}"))
            )
        ),
        None,
    )
    return review_row, item_row, holder


@dataclass
class DerivedPair:
    """A shared Note derived for one (author, work) — by :func:`_derive_pair`
    for a review/listItem pair or by :func:`_derive_play_group` for a group of
    teal.fm plays — not yet persisted."""

    anchor_uri: str
    stored_note_json: str | None  # the anchor's currently stored Note, if any
    note: dict[str, Any]
    activity: dict[str, Any]


def _derive_pair(*, did: str, work_key: str, handle: str, trigger_uri: str) -> DerivedPair | None:
    """Derive the single combined Note for an (author, work) pair.

    The Note stays anchored on the row that first published it; if nothing is
    published yet, the triggering record's rkey becomes the anchor (Create).
    Returns ``None`` when nothing contributes. Persisting the result is the
    caller's call — _sync_pair always writes.
    """
    if works.is_episode_key(work_key):
        # Episode-level marks are never (re)published — without this guard a
        # delete of one episode record could re-derive and re-emit a Note for
        # a surviving sibling record of the same episode work.
        return None
    review_row, item_row, anchor = _pair_rows(did, work_key)
    if review_row is None and item_row is None:
        return None
    operation = "update"
    if anchor is None:
        with session_scope() as session:
            trigger = session.get(Record, trigger_uri)

        def _burned(row: Record) -> bool:
            # A pending retraction (unpublished row still carrying its
            # Delete): the rkey-derived object id was tombstoned on peers,
            # and tombstone-caching servers may reject a Create reusing it.
            return row.ap_object_json is None and bool(row.ap_activity_json)

        candidates = [r for r in (trigger, review_row, item_row) if r is not None]
        # Prefer an anchor whose object id was never Deleted. When every
        # contributing row is burned (e.g. a partnerless record flipped to an
        # episode and back), the id is reused — same known limit as the
        # opt-out revive path (see _persist).
        anchor = next((r for r in candidates if not _burned(r)), None)
        if anchor is None and candidates:
            anchor = candidates[0]
        operation = "create"
        if anchor is None:
            return None

    status = _item_status(json.loads(item_row.source_json or "{}")) if item_row else None
    if review_row is not None:
        source = json.loads(review_row.source_json or "{}")
        collection = _REVIEW_COLLECTION
        shelf_status = status
    else:
        # No review: an item-only Note (its branch derives the Status itself).
        assert item_row is not None
        source = json.loads(item_row.source_json or "{}")
        collection = _LIST_ITEM_COLLECTION
        shelf_status = None

    note, activity = neodb.translate(
        did=did,
        unlisted=_unlisted(did),
        handle=handle,
        collection=collection,
        rkey=anchor.rkey,
        record=source,
        operation=operation,
        # An anchor that already published keeps its id; a fresh anchor
        # (operation == "create") mints one from the current handle.
        prior_object_id=_stored_note_id(anchor.ap_object_json) if operation == "update" else None,
        event_time=None,
        ref=works.mint(source),
        shelf_status=shelf_status,
    )
    assert note is not None  # never a delete translation on this path
    return DerivedPair(anchor.at_uri, anchor.ap_object_json, note, activity)


def _sync_pair(*, did: str, work_key: str, handle: str, trigger_uri: str) -> DerivedPair | None:
    """Re-derive and persist the pair's Note; return the derivation.

    Callers fan the activity out with ``record_uri=derived.anchor_uri`` —
    the row the Note actually lives on — so the delivery log stays
    associated with the pair rather than whichever partner record happened
    to trigger the update.
    """
    derived = _derive_pair(did=did, work_key=work_key, handle=handle, trigger_uri=trigger_uri)
    if derived is None:
        return None
    _update_ap(derived.anchor_uri, derived.note, derived.activity)
    return derived


def _pair_trigger(did: str, work_key: str) -> str | None:
    """An active paired row of (author, work) to hand _sync_pair as trigger.

    Prefers the row holding the published Note (the pair's anchor). When
    nothing is published yet, only a *contributing* row qualifies — same test
    as _pair_rows — since _sync_pair anchors a fresh Create on the trigger:
    anchoring on a status-less list membership would tie the pair's Note to a
    record whose later deletion must not retract it. ``None`` when the pair
    has no contributing rows left.
    """
    with session_scope() as session:
        rows = session.execute(
            select(Record.at_uri, Record.collection, Record.source_json, Record.ap_object_json)
            .where(
                Record.did == did,
                Record.work_key == work_key,
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.deleted_at.is_(None),
            )
            .order_by(Record.created_at.asc(), Record.at_uri.asc())
        ).all()

    candidates = [
        (at_uri, ap)
        for at_uri, collection, source_json, ap in rows
        if _contributes_row(collection, source_json)
    ]
    for at_uri, ap_object_json in candidates:
        if ap_object_json is not None:
            return at_uri
    return candidates[0][0] if candidates else None


# --- teal.fm plays: one Note per listening session -------------------------
#
# A scrobbler writes one record per track, so bridging each play as its own
# Note would post a 12-track album twelve times — to followers' timelines and
# onto the item page of every NeoDB peer. Instead the plays of one (author,
# release) are cut into SESSIONS, and every play of one session shares ONE
# Note, anchored on the play that published it first
# (/users/<handle>/posts/<rkey>).
#
# A play joins the newest session of that release when it follows the session's
# latest play by no more than `teal_window_days` (default 14). A longer silence
# means the listener came back to the album later, which is worth its own post:
# the play founds a new session and a new Create goes out, while the earlier
# session keeps its own Note untouched. The session a play belongs to is
# decided once, at ingest, and kept in Record.play_group ("<work_key>#<founder
# rkey>"), so it never moves under a Note already published.
#
# Within a session the Note's content names only the album and its artists,
# never a track or a count, so a further play derives an identical Note. It
# still refreshes the mark — the Note says the author IS LISTENING to the album
# — but at most once per `teal_update_hours` (default 24): an Update per
# scrobbled track would flood relays for a mark that did not move. A real
# change to the Note (a release title that arrived late, a changed visibility
# preference, a rename of the anchor) is sent at once, never throttled.
#
# Deleting the anchor Deletes the Note, and the newest surviving play of the
# SAME session re-publishes under its own rkey; deleting any other play just
# re-derives (and sends nothing). A play whose release cannot be identified
# mints no work and is archived without AP emission — NeoDB has no track item
# to mark.


def _play_time(record: dict, fallback: datetime) -> datetime:
    """When a play happened: its ``playedTime``, else *fallback*.

    ``playedTime`` is optional in the teal lexicon; the caller passes the
    row's own ingest time (or the event time) for a play without one.
    """
    played = record.get("playedTime")
    if isinstance(played, str) and played:
        try:
            parsed = datetime.fromisoformat(played)
        except ValueError:
            return fallback
        return _aware(parsed)
    return fallback


def _aware(moment: datetime) -> datetime:
    """A timestamp as UTC-aware: SQLite hands a DATETIME column back naive."""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _event_datetime(event_time: str | None) -> datetime:
    """The firehose event time as a datetime, falling back to *now*.

    Only the fallback play time of a play without ``playedTime`` — a session
    is cut on when the listening happened, and the event time is the closest
    stand-in the record offers.
    """
    if event_time:
        try:
            return _aware(datetime.fromisoformat(event_time))
        except ValueError:
            pass
    return utcnow()


def _row_play_time(row: Record) -> datetime:
    """The play time of an archived play row (ingest time as the fallback)."""
    return _play_time(_source_dict(row.source_json) or {}, _aware(row.created_at))


def _play_group(*, did: str, work_key: str, rkey: str, at_uri: str, played: datetime) -> str:
    """The listening session a play belongs to, as ``"<work_key>#<rkey>"``.

    The newest other active play of the same release decides it: within
    ``teal_window_days`` of *played* the new play joins that play's session,
    beyond it (or with no other play at all) it founds its own. One bounded
    query — a heavy listener's album can hold thousands of plays.

    Sessions are cut on arrival order, which for both live ingest and a
    backfill replay is play order (backfill replays oldest-first by write
    time). A play that arrives late and lands inside an older silence founds
    its own session rather than merging the two around it; likewise deleting
    the plays in the middle of a session never splits it. Both keep an already
    published Note where peers saw it, which matters more than a perfectly cut
    history.
    """
    window = timedelta(days=get_settings().teal_window_days)
    with session_scope() as session:
        newest = session.scalars(
            select(Record)
            .where(
                Record.did == did,
                Record.work_key == work_key,
                Record.collection.in_(teal.PLAY_COLLECTIONS),
                Record.at_uri != at_uri,
                Record.play_group.is_not(None),
                Record.deleted_at.is_(None),
            )
            .order_by(Record.rkey.desc(), Record.at_uri.desc())
            .limit(1)
        ).first()
        if newest is not None and abs(played - _row_play_time(newest)) <= window:
            return newest.play_group or f"{work_key}#{rkey}"
    return f"{work_key}#{rkey}"


def _prior_play_state(at_uri: str) -> tuple[str | None, str | None]:
    """(published Note id, play_group) of a play before this event.

    The play equivalent of :func:`_prior_state`, which reports the work rather
    than the session: a play's Note is shared per session, so that is the key
    the retraction and re-derivation paths need.
    """
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None or row.deleted_at is not None:
            return None, None
        return _stored_note_id(row.ap_object_json), row.play_group


def _active_plays(did: str, play_group: str):
    """Base query: the active plays of one listening session, newest first.

    Play rkeys are TIDs, which sort by write time as strings, so the first row
    is the most recent play — the one that re-anchors the Note when the
    current anchor is deleted. Callers always add a ``LIMIT``: a session can
    hold thousands of plays, and derivation needs one.
    """
    return (
        select(Record)
        .where(
            Record.did == did,
            Record.play_group == play_group,
            Record.collection.in_(teal.PLAY_COLLECTIONS),
            Record.deleted_at.is_(None),
        )
        .order_by(Record.rkey.desc(), Record.at_uri.desc())
    )


def _play_holder(did: str, play_group: str, *, exclude_uri: str | None = None) -> Record | None:
    """The play currently holding the session's published Note (other than
    *exclude_uri*, when given)."""
    query = _active_plays(did, play_group).where(Record.ap_object_json.is_not(None))
    if exclude_uri is not None:
        query = query.where(Record.at_uri != exclude_uri)
    with session_scope() as session:
        return session.scalars(query.limit(1)).first()


def _play_anchor(did: str, play_group: str) -> tuple[Record | None, str]:
    """``(anchor, operation)`` for the session's Note; ``(None, ...)`` if empty.

    The play holding the published Note anchors an ``update``. Otherwise the
    newest play whose object id peers never saw Deleted anchors a ``create``
    (a row in the pending-retraction shape carries a Delete and no Note);
    when every survivor is burned, the newest one is reused — same known
    limit as _persist. Three bounded queries, never the whole session.
    """
    holder = _play_holder(did, play_group)
    if holder is not None:
        return holder, "update"
    with session_scope() as session:
        fresh = session.scalars(
            _active_plays(did, play_group)
            .where(Record.ap_object_json.is_(None), Record.ap_activity_json.is_(None))
            .limit(1)
        ).first()
        if fresh is not None:
            return fresh, "create"
        return session.scalars(_active_plays(did, play_group).limit(1)).first(), "create"


def _without_volatile(obj: Any) -> Any:
    """*obj* with every ``updated`` stamp removed, at any depth.

    A re-derived Note differs from the stored one in ``updated`` (top level
    and on each relatedWith facet) even when nothing else moved; that stamp
    is the one thing that must not count as a change.
    """
    if isinstance(obj, dict):
        return {k: _without_volatile(v) for k, v in obj.items() if k != "updated"}
    if isinstance(obj, list):
        return [_without_volatile(v) for v in obj]
    return obj


def _same_note(stored_json: str | None, note: dict) -> bool:
    stored = _source_dict(stored_json) if stored_json else None
    return stored is not None and _without_volatile(stored) == _without_volatile(note)


def _refresh_due(anchor_uri: str) -> bool:
    """Has ``teal_update_hours`` passed since the session's Note went out?

    The clock is the anchor row's own ``updated_at``, which _update_ap bumps
    every time the Note is published or refreshed — the moment peers last
    received it. It costs no extra state and survives a restart, unlike a
    timer held in memory. It is deliberately NOT the Note's ``published``:
    that is the play time, which a tracker may report long after the fact.
    A row that has gone missing reads as due; one Update too many is
    harmless, a mark frozen for ever is not.
    """
    interval = timedelta(hours=get_settings().teal_update_hours)
    if not interval:
        return True
    with session_scope() as session:
        row = session.get(Record, anchor_uri)
        sent = _aware(row.updated_at) if row is not None else None
    return sent is None or utcnow() - sent >= interval


def _derive_play_group(*, did: str, play_group: str, handle: str) -> DerivedPair | None:
    """Derive the single Note for every active play of one listening session.

    Anchored on the play holding the published Note; with none published, the
    newest play whose object id was never tombstoned becomes the anchor
    (Create) — see _play_anchor. Returns ``None`` when no active play remains.
    """
    anchor, operation = _play_anchor(did, play_group)
    if anchor is None:
        return None
    source = json.loads(anchor.source_json or "{}")
    # `playedTime` is optional in the lexicon. Without it `published` would
    # fall back to *now* on every derivation and differ every time, which
    # _same_note would read as a real change — so the anchor row's own (fixed)
    # ingest time is the fallback instead.
    note, activity = neodb.translate(
        did=did,
        unlisted=_unlisted(did),
        handle=handle,
        collection=anchor.collection,
        rkey=anchor.rkey,
        record=source,
        operation=operation,
        event_time=_aware(anchor.created_at).isoformat(),
        ref=works.mint(source),
        prior_object_id=_stored_note_id(anchor.ap_object_json) if operation == "update" else None,
    )
    assert note is not None  # never a delete translation on this path
    return DerivedPair(anchor.at_uri, anchor.ap_object_json, note, activity)


def _sync_play_group(*, did: str, play_group: str, handle: str) -> DerivedPair | None:
    """Re-derive the session's Note and persist it — if it is worth sending.

    Returns ``None`` when the session is empty, and when the derived Note
    equals the stored one apart from its ``updated`` stamps while that stamp
    is younger than ``teal_update_hours``: nothing is written and the caller
    sends nothing, which is what keeps a scrobbler's every play from becoming
    an Update on the fediverse. Past that interval the unchanged Note IS sent
    again, to refresh the "is listening" mark on NeoDB peers.
    """
    derived = _derive_play_group(did=did, play_group=play_group, handle=handle)
    if derived is None:
        return None
    if (
        derived.stored_note_json is not None
        and _same_note(derived.stored_note_json, derived.note)
        and not _refresh_due(derived.anchor_uri)
    ):
        return None
    _update_ap(derived.anchor_uri, derived.note, derived.activity)
    return derived


async def _process_play(
    *,
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    handle: str,
    record: dict,
    ref: works.WorkRef | None,
    operation: str,
    seq: int | None,
    cid: str | None,
    event_time: str | None,
    worker: DeliveryWorker | None,
) -> Processed:
    """Create/update of one teal.fm play; see the section comment above.

    The play is archived first (keeping any Note this row already anchors),
    then the Note of the session it now belongs to is re-derived, and so is
    the Note of the session it left if an update moved it. A play that anchors
    a Note but loses its release, or moves into a session whose Note another
    play already holds, has its own Note retracted first — one session must
    never end up with two published Notes.
    """
    note_id, prior_group = _prior_play_state(at_uri)
    new_group = (
        _play_group(
            did=did,
            work_key=ref.work_key,
            rkey=rkey,
            at_uri=at_uri,
            played=_play_time(record, _event_datetime(event_time)),
        )
        if ref is not None
        else None
    )
    retraction = None
    if note_id is not None and (
        new_group is None
        or (
            prior_group != new_group
            and _play_holder(did, new_group, exclude_uri=at_uri) is not None
        )
    ):
        _, retraction = neodb.translate(
            did=did,
            unlisted=_unlisted(did),
            handle=handle,
            collection=collection,
            rkey=rkey,
            record=None,
            operation="delete",
            event_time=None,
            prior_object_id=note_id,
        )
    _persist(
        at_uri=at_uri,
        did=did,
        collection=collection,
        rkey=rkey,
        seq=seq,
        cid=cid,
        source=record,
        note=None,
        activity=retraction,
        operation=operation,
        work_key=ref.work_key if ref is not None else None,
        play_group=new_group,
        # The retraction replaces the stored AP forms (pending-Delete shape);
        # otherwise the row keeps the Note it may anchor for _derive_play_group.
        preserve_ap=retraction is None,
    )
    delivered = 0
    if worker is not None and retraction is not None:
        delivered += await fanout(worker, record_uri=at_uri, did=did, activity=retraction)
    activity = retraction
    if new_group is not None:
        group = _sync_play_group(did=did, play_group=new_group, handle=handle)
        if group is not None:
            activity = group.activity
            if worker is not None:
                delivered += await fanout(
                    worker, record_uri=group.anchor_uri, did=did, activity=group.activity
                )
    if prior_group and prior_group != new_group:
        # The play left another session (an update re-identified it): that
        # session may have lost its anchor and needs a survivor to publish.
        prior = _sync_play_group(did=did, play_group=prior_group, handle=handle)
        if worker is not None and prior is not None:
            delivered += await fanout(
                worker, record_uri=prior.anchor_uri, did=did, activity=prior.activity
            )
    return Processed(at_uri, operation, collection, activity or {}, delivered)


def _update_ap(at_uri: str, note: dict | None, activity: dict | None) -> None:
    """Replace only the stored AP forms of a record (op/source untouched)."""
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is not None:
            row.ap_object_json = json.dumps(note) if note is not None else None
            row.ap_activity_json = json.dumps(activity) if activity is not None else None
            row.updated_at = utcnow()


def _process_archive_only(
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    event: dict[str, Any],
    operation: str,
    seq: int | None = None,
) -> Processed:
    """Persist (or tombstone) the record without any AP translation/delivery."""
    if operation == "delete":
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.op = "delete"
                row.deleted_at = utcnow()
                row.updated_at = utcnow()
                _mark_seq(row, seq)
        return Processed(at_uri, "delete", collection, {})
    _persist(
        at_uri=at_uri,
        did=did,
        collection=collection,
        rkey=rkey,
        seq=seq,
        cid=event.get("cid"),
        source=event.get("record") or {},
        note=None,
        activity=None,
        operation=operation,
        work_key=None,
    )
    return Processed(at_uri, operation, collection, {})


async def _process_profile(
    *,
    at_uri: str,
    did: str,
    operation: str,
    record: dict[str, Any],
    event_time: str | None,
    seq: int | None,
    worker: DeliveryWorker | None,
    allow_network: bool,
) -> Processed | None:
    """Refresh a bridged actor's display name/avatar from a profile edit and
    emit an ``Update(Person)`` to that author's own followers.

    Never archived in the ``Record`` table: a profile record is identity
    metadata, not content, so keeping it out keeps /archive and stats
    focused on actual posts.
    """
    if operation == "delete":
        # Deleting the popfeed profile record doesn't change identity; bsky
        # data remains authoritative on the next refresh.
        return None

    if _profile_seen(did, seq):
        return None

    row = identity.refresh_actor(did, record, allow_network=allow_network)
    if row is None:
        return None

    _mark_profile_seq(did, seq)
    activity, delivered = await deliver_person_update(row, seq=seq, worker=worker)
    return Processed(at_uri, operation, _PROFILE_COLLECTION, activity, delivered)


async def _process_visibility(
    *,
    at_uri: str,
    did: str,
    operation: str,
    record: dict[str, Any],
    seq: int | None,
    worker: DeliveryWorker | None,
) -> Processed | None:
    """Apply an ``app.bsky.actor.contentVisibilityDeclaration`` commit.

    The flag rides onto the fediverse as ``discoverable: false`` on the bridged
    ``Person``, so a change is published the same way a renamed handle is: an
    ``Update(Person)`` direct to that author's own followers.

    Deleting the record and setting the field to false are the same statement —
    the lexicon requires a missing record to read as false — so both paths land
    on ``hide=False`` rather than being treated as "no opinion".
    """
    if _visibility_seen(did, seq):
        return None

    hide = False if operation == "delete" else bool(record.get(identity.HIDE_FIELD))
    row = identity.set_hide_from_recommendations(did, hide)
    _mark_visibility_seq(did, seq)
    if row is None:
        # Not a DID we bridge, or the value did not move. Either way there is
        # nothing to tell anyone about.
        return None

    activity, delivered = await deliver_person_update(row, seq=seq, worker=worker)
    return Processed(at_uri, operation, _VISIBILITY_COLLECTION, activity, delivered)


async def deliver_person_update(
    row: BridgedActor, *, seq: int | None, worker: DeliveryWorker | None
) -> tuple[dict[str, Any], int]:
    """Send an ``Update(Person)`` for a refreshed actor to its own followers.

    Never relayed as an ``Announce``: identity metadata is only of interest to
    servers that already follow this author.
    """
    settings = get_settings()
    actor_id = settings.actor_id(row.handle)
    update_id = seq or int(datetime.now(UTC).timestamp() * 1_000_000)
    activity = {
        # The toot prefix belongs on the ACTIVITY, not just on the Person it
        # carries: a receiver compacting an inbound activity uses the outer
        # context, so without it the embedded actor's `toot:discoverable`
        # compacts to a full-IRI key and the preference is read as unset.
        "@context": [actors.AS_CONTEXT, actors.SECURITY_CONTEXT, actors.TOOT_TERMS],
        "id": f"{actor_id}#updates/{update_id}",
        "type": "Update",
        "actor": actor_id,
        "to": [neodb.PUBLIC],
        "cc": [f"{actor_id}/followers"],
        "object": actors.person_actor(row),
    }
    delivered = 0
    if worker is not None:
        delivered = await fanout_actor_update(worker, did=row.did, activity=activity)
    return activity, delivered


def _profile_seen(did: str, seq: int | None) -> bool:
    """Has this profile commit (or a newer one) already been applied?

    The Record-based staleness guard can't cover profile edits — they are
    never archived as records — so the mark lives on the actor instead.
    """
    if seq is None:
        return False
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return (
            actor is not None
            and actor.last_profile_seq is not None
            and (seq <= actor.last_profile_seq)
        )


def _mark_profile_seq(did: str, seq: int | None) -> None:
    if seq is None:
        return
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        if actor is not None:
            actor.last_profile_seq = max(actor.last_profile_seq or 0, seq)


def _visibility_seen(did: str, seq: int | None) -> bool:
    """Has this contentVisibilityDeclaration commit already been applied?

    Its own mark rather than ``last_profile_seq``: the two records change
    independently, and sharing one high-water mark would let whichever moved
    last suppress the other.
    """
    if seq is None:
        return False
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return (
            actor is not None
            and actor.last_visibility_seq is not None
            and (seq <= actor.last_visibility_seq)
        )


def _mark_visibility_seq(did: str, seq: int | None) -> None:
    if seq is None:
        return
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        if actor is not None:
            actor.last_visibility_seq = max(actor.last_visibility_seq or 0, seq)


def _is_gated(did: str) -> bool:
    """Is this DID currently inactive (not deleted) on atproto?"""
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return actor is not None and actor.inactive_status is not None


async def _process_account(
    event: dict[str, Any], *, worker: DeliveryWorker | None
) -> Processed | None:
    """Apply an atproto account lifecycle change to an actor we already bridge.

    Like identity events, these arrive for every account on the network, so
    the cheap "do we know this DID at all" test comes first — the vast
    majority are for accounts we have never bridged and cost one indexed
    lookup each.

    A ``deleted`` account is purged: everything bridged from it is retracted,
    since the source repo no longer exists. Every other inactive status is a
    reversible gate — nothing is retracted and ingestion resumes when the
    account goes active again. This never mints an actor: we bridge people
    because of what they post, so lifecycle news about an unknown DID is not
    our business.
    """
    payload = event.get(_ACCOUNT_KIND) or {}
    did = event.get("did") or payload.get("did") or ""
    if not did or not _is_bridged(did):
        return None

    active = payload.get("active")
    status = payload.get("status") or ""

    if active is False and status == _ACCOUNT_DELETED:
        # Not recorded as an opt-out: the DID is gone, not opting out, and a
        # standing OptOut row would wrongly suppress it if it ever returned.
        purged = await optout.purge_did(did, worker=worker, mark_opt_out=False)
        _set_gate(did, None)
        log.info("account deleted %s; retracted %d record(s)", did, purged)
        return Processed(f"at://{did}", "delete", _ACCOUNT_KIND, {}, purged)

    if active is False:
        _set_gate(did, status or "inactive")
        log.info("account %s gated (%s)", did, status or "inactive")
        return Processed(f"at://{did}", "update", _ACCOUNT_KIND, {})

    if active is True and _is_gated(did):
        _set_gate(did, None)
        log.info("account %s active again; resuming", did)
        return Processed(f"at://{did}", "update", _ACCOUNT_KIND, {})

    return None


def _is_bridged(did: str) -> bool:
    with session_scope() as session:
        return session.get(BridgedActor, did) is not None


def _set_gate(did: str, status: str | None) -> None:
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        if actor is not None:
            actor.inactive_status = status
            actor.inactive_at = utcnow() if status else None


async def _process_identity(
    event: dict[str, Any], *, worker: DeliveryWorker | None
) -> Processed | None:
    """Apply a handle change to an actor we already bridge.

    Like a profile edit, this never mints an actor: we bridge people because
    of what they post, so an identity event for an unknown DID is not our
    business. The retired handle stays resolvable (see identity.rename_actor),
    and followers get an ``Update(Person)`` carrying the new
    ``preferredUsername``.
    """
    payload = event.get("identity") or {}
    did = event.get("did") or payload.get("did") or ""
    handle = payload.get("handle") or ""
    if not did or not handle:
        return None
    if optout.is_opted_out(did):
        return None

    row = identity.rename_actor(did, handle)
    if row is None:
        return None

    activity, delivered = await deliver_person_update(row, seq=event.get("seq"), worker=worker)
    return Processed(f"at://{did}", "update", _IDENTITY_KIND, activity, delivered)


def _mark_seq(row: Record | None, seq: int | None) -> None:
    """Advance a row's high-water mark on a path that doesn't call _persist.

    Tombstoning must move the mark too: otherwise a later archive replay of an
    event newer than the record's *create* but older than its *delete* would
    pass the staleness test and resurrect a record peers already dropped.
    """
    if row is not None and seq is not None:
        row.last_seq = max(row.last_seq or 0, seq)


async def _process_delete(
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    handle: str,
    worker: DeliveryWorker | None,
    seq: int | None = None,
) -> Processed:
    settings = get_settings()
    with session_scope() as session:
        row = session.get(Record, at_uri)
        row_exists = row is not None
        had_note = row is not None and row.ap_object_json is not None
        work_key = row.work_key if row is not None else None
        play_group = row.play_group if row is not None else None
        stored_id = _stored_note_id(row.ap_object_json) if row is not None else None
    # Name the object id peers actually received. Recomputing it from the
    # current handle would tombstone a URL that was never published once the
    # author has been renamed since (see identity.rename_actor).
    prior_object_id = stored_id or settings.post_id(handle, rkey)

    if had_note or not row_exists:
        # The record anchored a published Note (or is unknown — retract
        # best-effort): Delete it. A merged-away partner stays AP-silent
        # until its own next event re-publishes it under its own rkey.
        _, activity = neodb.translate(
            did=did,
            unlisted=_unlisted(did),
            handle=handle,
            collection=collection,
            rkey=rkey,
            record=None,
            operation="delete",
            event_time=None,
            prior_object_id=prior_object_id,
        )
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.op = "delete"
                row.deleted_at = utcnow()
                row.updated_at = utcnow()
                row.ap_activity_json = json.dumps(activity)
                _mark_seq(row, seq)
        delivered = 0
        if worker is not None:
            delivered = await fanout(worker, record_uri=at_uri, did=did, activity=activity)
        if collection in teal.PLAY_COLLECTIONS and play_group:
            # The anchor of a listening session is gone: the newest surviving
            # play of the SAME session re-publishes its Note under that play's
            # own rkey. An earlier session of the album is never touched.
            group = _sync_play_group(did=did, play_group=play_group, handle=handle)
            if worker is not None and group is not None:
                delivered += await fanout(
                    worker, record_uri=group.anchor_uri, did=did, activity=group.activity
                )
        return Processed(at_uri, "delete", collection, activity, delivered)

    # Record without a Note of its own (merged into a pair Note, or archived
    # collection membership): tombstone it, then re-derive the pair's Note
    # from what remains — but only if this record actually contributed to it.
    with session_scope() as session:
        row = session.get(Record, at_uri)
        source = json.loads(row.source_json or "{}") if row is not None else {}
        if row is not None:
            row.op = "delete"
            row.deleted_at = utcnow()
            row.updated_at = utcnow()
            _mark_seq(row, seq)
    pair = None
    if collection in _PAIRED_COLLECTIONS and work_key and _contributes(collection, source):
        pair = _sync_pair(did=did, work_key=work_key, handle=handle, trigger_uri=at_uri)
    elif collection in teal.PLAY_COLLECTIONS and play_group:
        # A non-anchor play: the session's Note is re-derived from what remains
        # and, its content not depending on this play, almost always unchanged.
        pair = _sync_play_group(did=did, play_group=play_group, handle=handle)
    delivered = 0
    if worker is not None and pair is not None:
        delivered = await fanout(
            worker, record_uri=pair.anchor_uri, did=did, activity=pair.activity
        )
    return Processed(
        at_uri, "delete", collection, pair.activity if pair is not None else {}, delivered
    )


def _persist(
    *,
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    cid: str | None,
    source: dict,
    note: dict | None,
    activity: dict | None,
    operation: str,
    work_key: str | None,
    seq: int | None = None,
    play_group: str | None = None,
    preserve_ap: bool = False,
) -> None:
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None:
            row = Record(at_uri=at_uri, did=did, collection=collection, rkey=rkey)
            session.add(row)
        if seq is not None:
            # Monotonic: _is_stale already rejected anything older, but a
            # max() keeps the mark correct if a caller ever persists twice
            # for one event (e.g. a retraction followed by a pair re-derive).
            row.last_seq = max(row.last_seq or 0, seq)
        row.cid = cid
        row.source_json = json.dumps(source)
        # preserve_ap keeps the stored AP forms — except when reviving a
        # tombstoned row (e.g. re-import after opt-out -> opt-in): its Note
        # was already retracted from peers, so keeping it would make
        # _sync_pair emit an Update for an object remote servers deleted
        # (and ignore). Clearing re-anchors the pair and publishes a fresh
        # Create instead. (preserve_ap callers pass note/activity as None.)
        # Known limit: the Create reuses the same rkey-derived object id the
        # Delete named, and peers that cache tombstones may still reject it.
        if not preserve_ap or row.deleted_at is not None:
            row.ap_object_json = json.dumps(note) if note is not None else None
            row.ap_activity_json = json.dumps(activity) if activity is not None else None
        row.op = operation
        row.work_key = work_key
        # Only a teal.fm play carries one; every other collection passes None.
        row.play_group = play_group
        row.deleted_at = None
        row.updated_at = utcnow()
