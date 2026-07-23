"""One-shot data repair for the episode/season work-mapping fix.

Positional identifier keys (``episodeNumber``/``seasonNumber``/
``tmdbTvSeriesId``) used to act as global work aliases, so distinct episodes
(and seasons) were merged into whichever work registered the number first —
and the mis-mapped Notes were federated. :func:`repair` cleans that up:

1. **Retract** — broadcast a ``Delete`` for every still-published Note whose
   record resolves to a ``tv_episode`` work (NeoDB doesn't federate
   episode-level marks, and the pipeline no longer emits them) and clear the
   stored AP forms so the records become archive-only.
2. **Rebuild** — wipe ``work`` / ``work_identifier`` and re-mint every
   archived review/listItem source through the fixed logic, re-pointing each
   record's ``work_key``. Episode listItems come back as season works
   (works.season_view).
3. **Re-sync** — for every record whose work mapping changed, re-derive the
   (author, work) pair Notes on both sides of the move: the work it left
   drops the mover's contribution from its Note (Update), the work it
   joined gets one published for it (Create/Update).

Idempotent: a second run finds nothing left to retract or re-map.
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass, field

from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select

from skybridge.activitypub.delivery import DeliveryWorker, fanout
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, Work, WorkIdentifier, utcnow
from skybridge.pipeline import _PAIRED_COLLECTIONS, _REVIEW_COLLECTION, _item_status, _sync_pair
from skybridge.translate import neodb, works

log = logging.getLogger("skybridge.maintenance")


@dataclass
class RepairReport:
    retracted: int = 0  # episode Notes deleted from peers / cleared locally
    works_before: int = 0
    works_after: int = 0
    remapped: int = 0  # records whose work_key changed in the rebuild
    resynced: int = 0  # published Notes re-derived and Updated
    deliveries: int = 0  # outbound tasks enqueued (0 when run without a worker)
    dry_run: bool = False
    # (at_uri, old_work_key) of what phase 1 would retract; only kept in
    # dry-run mode, where phases 2-3 don't run.
    would_retract: list[tuple[str, str | None]] = field(default_factory=list)


def _handle_for(did: str) -> str | None:
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return actor.handle if actor is not None else None


async def _retract_episode_notes(
    worker: DeliveryWorker | None, report: RepairReport, *, dry_run: bool
) -> None:
    """Delete every published episode Note from peers and clear its AP forms.

    The Tombstone targets the *stored* Note id — not one recomputed from the
    current handle — so the Delete names exactly the object peers received.
    """
    with session_scope() as session:
        rows = session.execute(
            select(
                Record.at_uri,
                Record.did,
                Record.collection,
                Record.rkey,
                Record.work_key,
                Record.ap_object_json,
            ).where(
                Record.work_key.like(f"{works.EPISODE_TYPE}:%"),
                Record.ap_object_json.is_not(None),
                Record.deleted_at.is_(None),
            )
        ).all()

    for at_uri, did, collection, rkey, work_key, ap_object_json in rows:
        if dry_run:
            report.would_retract.append((at_uri, work_key))
            report.retracted += 1
            continue
        note_id = None
        with contextlib.suppress(TypeError, ValueError):
            note_id = (json.loads(ap_object_json) or {}).get("id")
        handle = _handle_for(did)
        activity = None
        if note_id and handle:
            _, activity = neodb.translate(
                did=did,
                handle=handle,
                collection=collection,
                rkey=rkey,
                record=None,
                operation="delete",
                time_us=None,
                prior_object_id=note_id,
            )
        # Enqueue the Delete before clearing the row: a crash in between
        # costs at worst a duplicate Delete on the rerun (peers ignore those),
        # whereas clearing first would lose the retraction forever — the
        # rerun's query can no longer find the row.
        if worker is not None and activity is not None:
            report.deliveries += await fanout(worker, record_uri=at_uri, did=did, activity=activity)
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.ap_object_json = None
                row.ap_activity_json = None
                row.updated_at = utcnow()
        report.retracted += 1


def _rebuild_works(report: RepairReport) -> list[tuple[str, str, str | None, str | None]]:
    """Re-mint every archived review/listItem source through the fixed logic.

    Returns ``(did, at_uri, old_work_key, new_work_key)`` for each record
    whose work_key changed. Records are replayed in original arrival order so
    the first record's best identifier anchors each work_key, as in live
    ingestion.
    """
    with session_scope() as session:
        report.works_before = session.scalar(select(func.count()).select_from(Work)) or 0
        session.execute(sql_delete(WorkIdentifier))
        session.execute(sql_delete(Work))
        rows = session.execute(
            select(
                Record.at_uri, Record.did, Record.collection, Record.source_json, Record.work_key
            )
            .where(Record.collection.in_(_PAIRED_COLLECTIONS))
            # at_uri tiebreak: created_at can collide (backfill bursts), and a
            # stable replay order keeps rebuilt work_keys identical across runs.
            .order_by(Record.created_at.asc(), Record.at_uri.asc())
        ).all()

    remapped: list[tuple[str, str, str | None, str | None]] = []
    for at_uri, did, collection, source_json, old_key in rows:
        try:
            source = json.loads(source_json or "{}")
        except ValueError:
            continue
        if not isinstance(source, dict) or not source:
            continue
        # Archived sources normally carry $type; backstop it from the
        # collection so works.season_view keys off the right record kind.
        source.setdefault("$type", collection)
        ref = works.mint(source)
        new_key = ref.work_key if ref is not None else None
        if new_key == old_key:
            continue
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if row is not None:
                row.work_key = new_key
                row.updated_at = utcnow()
        remapped.append((did, at_uri, old_key, new_key))

    with session_scope() as session:
        report.works_after = session.scalar(select(func.count()).select_from(Work)) or 0
    report.remapped = len(remapped)
    return remapped


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

    def contributes(collection: str, source_json: str | None) -> bool:
        if collection == _REVIEW_COLLECTION:
            return True
        try:
            source = json.loads(source_json or "{}")
        except ValueError:
            return False
        return isinstance(source, dict) and _item_status(source) is not None

    candidates = [
        (at_uri, ap)
        for at_uri, collection, source_json, ap in rows
        if contributes(collection, source_json)
    ]
    for at_uri, ap_object_json in candidates:
        if ap_object_json is not None:
            return at_uri
    return candidates[0][0] if candidates else None


async def _resync_remapped(
    worker: DeliveryWorker | None,
    remapped: list[tuple[str, str, str | None, str | None]],
    report: RepairReport,
) -> None:
    """Re-derive and broadcast the pairs on *both* sides of every move.

    A pair's Note lives on one anchor row while partner records stay
    AP-silent, so the moved record itself often isn't the published one:
    the work it left keeps a Note that must drop the mover's contribution
    (Update), and the work it joined may have no Note at all yet (Create,
    anchored on a contributing row). Episode works are never re-published;
    one re-sync per (author, work).
    """
    seen: set[tuple[str, str]] = set()
    for did, _at_uri, old_key, new_key in remapped:
        for work_key in (old_key, new_key):
            if not work_key or (did, work_key) in seen or works.is_episode_key(work_key):
                continue
            seen.add((did, work_key))
            trigger_uri = _pair_trigger(did, work_key)
            if trigger_uri is None:
                continue
            handle = _handle_for(did)
            if handle is None:
                continue
            activity = _sync_pair(
                did=did, work_key=work_key, handle=handle, trigger_uri=trigger_uri
            )
            if activity is None:
                continue
            report.resynced += 1
            if worker is not None:
                report.deliveries += await fanout(
                    worker, record_uri=trigger_uri, did=did, activity=activity
                )


async def repair(worker: DeliveryWorker | None = None, *, dry_run: bool = False) -> RepairReport:
    """Run the full repair. ``dry_run`` reports phase 1 and stops (phases 2-3
    can't be previewed without mutating the works tables).

    A mutating run destroys the state its broadcasts derive from (stored Note
    ids, episode-typed work_keys), so ``worker=None`` — which mutates without
    broadcasting — is for tests only; the CLI always passes a worker.
    """
    report = RepairReport(dry_run=dry_run)
    await _retract_episode_notes(worker, report, dry_run=dry_run)
    if dry_run:
        return report
    remapped = _rebuild_works(report)
    await _resync_remapped(worker, remapped, report)
    log.info(
        "repair: retracted=%d works %d->%d remapped=%d resynced=%d deliveries=%d",
        report.retracted,
        report.works_before,
        report.works_after,
        report.remapped,
        report.resynced,
        report.deliveries,
    )
    return report
