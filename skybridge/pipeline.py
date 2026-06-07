"""The ingest → translate → persist → deliver pipeline.

A single :func:`process_event` handles one Jetstream-shaped commit event,
regardless of whether it came from the live firehose or a replayed fixture.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from skybridge import optout
from skybridge.activitypub.delivery import DeliveryWorker, fanout
from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Record, utcnow
from skybridge.translate import neodb, works

log = logging.getLogger("skybridge.pipeline")


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

    if operation == "delete":
        return await _process_delete(at_uri, did, collection, rkey, handle, worker)

    record = commit.get("record") or {}
    ref = works.mint(record)
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


async def _process_delete(
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    handle: str,
    worker: DeliveryWorker | None,
) -> Processed:
    settings = get_settings()
    prior_object_id = settings.post_id(handle, rkey)
    _, activity = neodb.translate(
        did=did,
        handle=handle,
        collection=collection,
        rkey=rkey,
        record=None,
        operation="delete",
        time_us=None,
        prior_object_id=prior_object_id,
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


def _persist(
    *,
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    cid: str | None,
    source: dict,
    note: dict | None,
    activity: dict,
    operation: str,
    work_key: str | None,
) -> None:
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is None:
            row = Record(at_uri=at_uri, did=did, collection=collection, rkey=rkey)
            session.add(row)
        row.cid = cid
        row.source_json = json.dumps(source)
        row.ap_object_json = json.dumps(note) if note is not None else None
        row.ap_activity_json = json.dumps(activity)
        row.op = operation
        row.work_key = work_key
        row.deleted_at = None
        row.updated_at = utcnow()
