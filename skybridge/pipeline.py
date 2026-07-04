"""The ingest → translate → persist → deliver pipeline.

A single :func:`process_event` handles one Jetstream-shaped commit event,
regardless of whether it came from the live firehose or a replayed fixture.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from skybridge import optout
from skybridge.activitypub.delivery import DeliveryWorker, fanout
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
    return neodb.shelf_status(source.get("listType") or "")


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
    # Honour opt-outs before creating any actor or persisting anything.
    if optout.is_opted_out(did):
        return None

    rkey = commit.get("rkey", "")
    operation = commit.get("operation", "create")
    time_us = event.get("time_us")
    at_uri = _at_uri(did, collection, rkey)

    ident = identity.ensure_actor(did, allow_network=allow_network)
    handle = ident.handle

    if collection in ARCHIVE_ONLY_COLLECTIONS:
        return _process_archive_only(at_uri, did, collection, rkey, commit, operation)

    if operation == "delete":
        return await _process_delete(at_uri, did, collection, rkey, handle, worker)

    record = commit.get("record") or {}
    ref = works.mint(record)

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
            work_key=ref.work_key,
            preserve_ap=True,
        )
        activity = _sync_pair(did=did, work_key=ref.work_key, handle=handle, trigger_uri=at_uri)
        delivered = 0
        if worker is not None and activity is not None:
            delivered = await fanout(worker, record_uri=at_uri, did=did, activity=activity)
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


def _sync_pair(*, did: str, work_key: str, handle: str, trigger_uri: str) -> dict | None:
    """Re-derive the single combined Note for an (author, work) pair.

    The Note stays anchored on the row that first published it; if nothing is
    published yet, the triggering record's rkey becomes the anchor (Create).
    Returns the Create/Update activity, or None when nothing contributes.
    """
    review_row, item_row, anchor = _pair_rows(did, work_key)
    if review_row is None and item_row is None:
        return None
    operation = "update"
    if anchor is None:
        with session_scope() as session:
            anchor = session.get(Record, trigger_uri)
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
    _update_ap(anchor.at_uri, note, activity)
    return activity


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
    activity = None
    if collection in _PAIRED_COLLECTIONS and work_key and _contributes(collection, source):
        activity = _sync_pair(did=did, work_key=work_key, handle=handle, trigger_uri=at_uri)
    delivered = 0
    if worker is not None and activity is not None:
        delivered = await fanout(worker, record_uri=at_uri, did=did, activity=activity)
    return Processed(at_uri, "delete", collection, activity or {}, delivered)


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
        if not preserve_ap:
            row.ap_object_json = json.dumps(note) if note is not None else None
            row.ap_activity_json = json.dumps(activity) if activity is not None else None
        row.op = operation
        row.work_key = work_key
        row.deleted_at = None
        row.updated_at = utcnow()
