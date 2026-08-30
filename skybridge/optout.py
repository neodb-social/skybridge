"""Opt-out service: stop bridging a DID and tombstone what was federated.

Opting out (1) records the DID so the pipeline skips its future events, (2)
marks the bridged actor, and (3) emits ``Delete`` activities for every record
already bridged so remote instances drop them. Opting back in just clears the
flag — future activity bridges again; previously-tombstoned posts stay deleted.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from sqlalchemy import func, select

from skybridge.activitypub.delivery import DeliveryWorker, fanout
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, OptOut, Record, utcnow
from skybridge.translate import neodb

log = logging.getLogger("skybridge.optout")


def is_opted_out(did: str) -> bool:
    with session_scope() as session:
        return session.get(OptOut, did) is not None


@dataclass(frozen=True)
class BridgeStatus:
    """Snapshot of a DID's bridging state for the self-service page."""

    did: str
    handle: str | None
    bridged: bool
    opted_out: bool
    record_count: int
    recent_rows: list[Record]
    # The Bluesky visibility preferences we are currently carrying for this
    # account, shown back to the account holder so a sign-in refresh is
    # visible rather than silent.
    hide_from_recommendations: bool = False
    no_unauthenticated: bool = False


def lookup_status(did: str, *, recent_limit: int = 200) -> BridgeStatus:
    """Look up a DID's bridging status (DB only, no network)."""
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        active = select(Record).where(Record.did == did, Record.deleted_at.is_(None))
        count = (
            session.scalar(
                select(func.count())
                .select_from(Record)
                .where(Record.did == did, Record.deleted_at.is_(None))
            )
            or 0
        )
        recent = list(
            session.scalars(active.order_by(Record.updated_at.desc()).limit(recent_limit))
        )
        return BridgeStatus(
            did=did,
            handle=actor.handle if actor is not None else None,
            bridged=actor is not None,
            opted_out=session.get(OptOut, did) is not None,
            record_count=count,
            recent_rows=recent,
            hide_from_recommendations=actor is not None and bool(actor.hide_from_recommendations),
            no_unauthenticated=actor is not None and bool(actor.no_unauthenticated),
        )


async def purge_did(did: str, *, worker: DeliveryWorker | None = None, mark_opt_out: bool) -> int:
    """Tombstone everything bridged for ``did``; returns the count retracted.

    Shared by the user-initiated opt-out and by account deletion on the
    firehose (see ``pipeline._process_account``), which need the same
    retraction but differ in one respect: only an opt-out records an
    :class:`OptOut` row, because that is a standing choice by the account
    holder. A deleted account is simply gone, and re-recording it as an
    opt-out would wrongly suppress a DID that later returns.

    If a delivery ``worker`` is supplied, ``Delete`` activities are fanned out
    to subscribers and the actor's followers.
    """
    # Stop any in-flight import first: a half-done replay suspended inside
    # delivery could otherwise enqueue a Create AFTER the purge's Delete for
    # the same record, leaving the opted-out content live on remote peers.
    # (Late import: backfill imports this module for its opt-out guard.)
    # This guards IN-PROCESS imports only — a `backfill --deliver` running
    # in a separate process delivers through its own queue and can still
    # race the purge; don't run one concurrently with live opt-outs.
    from skybridge.atproto import archive, backfill

    await backfill.cancel_import(did)

    # Same hazard from the historical archive import, which is not per-DID.
    # Only a *delivering* import can leak past the purge; a normal one
    # federates nothing, and its per-event opt-out check stops it writing
    # anything more for this DID. Cancelling is cheap because the job is
    # resumable: it is left `paused` and archive.watch_jobs picks it up again,
    # this time with the opt-out in force.
    await archive.cancel_if_delivering()

    settings = get_settings()
    pending: list[tuple[str, dict]] = []

    with session_scope() as session:
        if mark_opt_out and session.get(OptOut, did) is None:
            session.add(OptOut(did=did))

        actor = session.get(BridgedActor, did)
        handle = actor.handle if actor is not None else did
        # Retractions are addressed the same way the posts they retract were.
        unlisted = actor is not None and bool(actor.no_unauthenticated)
        if actor is not None and mark_opt_out:
            actor.opted_out = True
            actor.opted_out_at = utcnow()

        rows = list(
            session.scalars(select(Record).where(Record.did == did, Record.deleted_at.is_(None)))
        )
        for row in rows:
            was_published = row.ap_object_json is not None
            row.op = "delete"
            row.deleted_at = utcnow()
            row.updated_at = utcnow()
            if not was_published:
                # Archived-only (lists, collection membership, merged-away
                # pair records): nothing was federated, nothing to retract.
                continue
            _, activity = neodb.translate(
                did=did,
                handle=handle,
                collection=row.collection,
                rkey=row.rkey,
                record=None,
                operation="delete",
                event_time=None,
                prior_object_id=settings.post_id(handle, row.rkey),
                unlisted=unlisted,
            )
            row.ap_activity_json = json.dumps(activity)
            pending.append((row.at_uri, activity))

    if worker is not None:
        for at_uri, activity in pending:
            await fanout(worker, record_uri=at_uri, did=did, activity=activity)

    log.info(
        "%s %s; tombstoned %d record(s)",
        "opted out" if mark_opt_out else "purged deleted account",
        did,
        len(pending),
    )
    return len(pending)


async def opt_out(did: str, *, worker: DeliveryWorker | None = None) -> int:
    """Opt ``did`` out and retract everything already bridged for it."""
    return await purge_did(did, worker=worker, mark_opt_out=True)


def opt_in(did: str) -> bool:
    """Clear an opt-out. Returns ``True`` if the DID was previously opted out."""
    with session_scope() as session:
        row = session.get(OptOut, did)
        if row is not None:
            session.delete(row)
        actor = session.get(BridgedActor, did)
        if actor is not None:
            actor.opted_out = False
            actor.opted_out_at = None
        return row is not None
