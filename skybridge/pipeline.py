"""The ingest → translate → persist → deliver pipeline.

A single :func:`process_event` handles one Jetstream-shaped commit event,
regardless of whether it came from the live firehose or a replayed fixture.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from skybridge import optout, telemetry
from skybridge.activitypub import actors
from skybridge.activitypub.delivery import DeliveryWorker, fanout, fanout_actor_update
from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Record, utcnow
from skybridge.translate import neodb, works

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


@dataclass
class Processed:
    at_uri: str
    operation: str
    collection: str
    activity: dict[str, Any]
    delivered: int = 0


def _at_uri(did: str, collection: str, rkey: str) -> str:
    return f"at://{did}/{collection}/{rkey}"


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
    note = _source_dict(ap_object_json) if ap_object_json else None
    return (note.get("id") if note else None), work_key


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
) -> Processed | None:
    """Process one commit event. Returns ``None`` if filtered/ignored."""
    if event.get("kind") != "commit":
        return None
    commit = event.get("commit") or {}
    collection = commit.get("collection", "")
    if not _wanted(collection):
        return None

    did = event["did"]
    operation = commit.get("operation", "create")

    # Honour opt-outs before creating any actor or persisting anything.
    if optout.is_opted_out(did):
        return None

    # Ingest-volume metric: ticks for every wanted commit event from non-opted-out
    # authors, regardless of what the pipeline later does with it (archive-only,
    # merge, ...).
    telemetry.record_ingested(collection, operation)

    rkey = commit.get("rkey", "")
    time_us = event.get("time_us")
    at_uri = _at_uri(did, collection, rkey)

    if collection == _PROFILE_COLLECTION:
        # A profile edit only ever refreshes an existing actor (see
        # identity.refresh_actor) — it must never mint one, so this branches
        # before ensure_actor is called below.
        return await _process_profile(
            at_uri=at_uri,
            did=did,
            operation=operation,
            record=commit.get("record") or {},
            time_us=time_us,
            worker=worker,
            allow_network=allow_network,
        )

    ident = identity.ensure_actor(did, allow_network=allow_network)
    handle = ident.handle

    if collection in ARCHIVE_ONLY_COLLECTIONS:
        return _process_archive_only(at_uri, did, collection, rkey, commit, operation)

    if operation == "delete":
        return await _process_delete(at_uri, did, collection, rkey, handle, worker)

    record = commit.get("record") or {}
    ref = works.mint(record)

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
        # retraction (an unpublished row carrying a Delete) that the repair
        # command re-broadcasts, instead of a Note stranded on peers forever.
        note_id, prior_key = _prior_state(at_uri)
        retraction = None
        if note_id is not None:
            _, retraction = neodb.translate(
                did=did,
                handle=handle,
                collection=collection,
                rkey=rkey,
                record=None,
                operation="delete",
                time_us=None,
                prior_object_id=note_id,
            )
        new_key = ref.work_key if ref is not None else None
        _persist(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            cid=commit.get("cid"),
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
            cid=commit.get("cid"),
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
                handle=handle,
                collection=collection,
                rkey=rkey,
                record=None,
                operation="delete",
                time_us=None,
                prior_object_id=note_id,
            )
        _persist(
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            cid=commit.get("cid"),
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
        handle=handle,
        collection=collection,
        rkey=rkey,
        record=record,
        operation=operation,
        time_us=time_us,
        ref=ref,
    )
    _persist(
        at_uri=at_uri,
        did=did,
        collection=collection,
        rkey=rkey,
        cid=commit.get("cid"),
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
    """A pair Note derived by :func:`_derive_pair`, not yet persisted."""

    anchor_uri: str
    stored_note_json: str | None  # the anchor's currently stored Note, if any
    note: dict[str, Any]
    activity: dict[str, Any]


def _derive_pair(*, did: str, work_key: str, handle: str, trigger_uri: str) -> DerivedPair | None:
    """Derive the single combined Note for an (author, work) pair.

    The Note stays anchored on the row that first published it; if nothing is
    published yet, the triggering record's rkey becomes the anchor (Create).
    Returns ``None`` when nothing contributes. Persisting the result is the
    caller's call — _sync_pair always writes, the repair command first checks
    whether the derivation differs from what was already published.
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
        handle=handle,
        collection=collection,
        rkey=anchor.rkey,
        record=source,
        operation=operation,
        time_us=None,
        ref=works.mint(source),
        shelf_status=shelf_status,
    )
    assert note is not None  # never a delete translation on this path
    return DerivedPair(anchor.at_uri, anchor.ap_object_json, note, activity)


def _sync_pair(*, did: str, work_key: str, handle: str, trigger_uri: str) -> DerivedPair | None:
    """Re-derive and persist the pair's Note; return the derivation.

    Callers fan the activity out with ``record_uri=derived.anchor_uri`` —
    the row the Note actually lives on — so the delivery log stays
    associated with the pair and repair's historical-recipient discovery
    (maintenance._pair_historical_targets) can find those inboxes later.
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
    commit: dict[str, Any],
    operation: str,
) -> Processed:
    """Persist (or tombstone) the record without any AP translation/delivery."""
    if operation == "delete":
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.op = "delete"
                row.deleted_at = utcnow()
                row.updated_at = utcnow()
        return Processed(at_uri, "delete", collection, {})
    _persist(
        at_uri=at_uri,
        did=did,
        collection=collection,
        rkey=rkey,
        cid=commit.get("cid"),
        source=commit.get("record") or {},
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
    time_us: int | None,
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

    row = identity.refresh_actor(did, record, allow_network=allow_network)
    if row is None:
        return None

    settings = get_settings()
    actor_id = settings.actor_id(row.handle)
    update_id = time_us or int(datetime.now(UTC).timestamp() * 1_000_000)
    activity = {
        "@context": [actors.AS_CONTEXT, actors.SECURITY_CONTEXT],
        "id": f"{actor_id}#updates/{update_id}",
        "type": "Update",
        "actor": actor_id,
        "to": [neodb.PUBLIC],
        "cc": [f"{actor_id}/followers"],
        "object": actors.person_actor(row),
    }
    delivered = 0
    if worker is not None:
        delivered = await fanout_actor_update(worker, did=did, activity=activity)
    return Processed(at_uri, operation, _PROFILE_COLLECTION, activity, delivered)


async def _process_delete(
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    handle: str,
    worker: DeliveryWorker | None,
) -> Processed:
    settings = get_settings()
    with session_scope() as session:
        row = session.get(Record, at_uri)
        row_exists = row is not None
        had_note = row is not None and row.ap_object_json is not None
        work_key = row.work_key if row is not None else None

    if had_note or not row_exists:
        # The record anchored a published Note (or is unknown — retract
        # best-effort): Delete it. A merged-away partner stays AP-silent
        # until its own next event re-publishes it under its own rkey.
        _, activity = neodb.translate(
            did=did,
            handle=handle,
            collection=collection,
            rkey=rkey,
            record=None,
            operation="delete",
            time_us=None,
            prior_object_id=settings.post_id(handle, rkey),
        )
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.op = "delete"
                row.deleted_at = utcnow()
                row.updated_at = utcnow()
                row.ap_activity_json = json.dumps(activity)
        delivered = 0
        if worker is not None:
            delivered = await fanout(worker, record_uri=at_uri, did=did, activity=activity)
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
    pair = None
    if collection in _PAIRED_COLLECTIONS and work_key and _contributes(collection, source):
        pair = _sync_pair(did=did, work_key=work_key, handle=handle, trigger_uri=at_uri)
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
    preserve_ap: bool = False,
) -> None:
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None:
            row = Record(at_uri=at_uri, did=did, collection=collection, rkey=rkey)
            session.add(row)
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
        row.deleted_at = None
        row.updated_at = utcnow()
