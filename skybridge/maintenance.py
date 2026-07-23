"""One-shot data repair for the episode/season work-mapping fix.

Positional identifier keys (``episodeNumber``/``seasonNumber``/
``tmdbTvSeriesId``) used to act as global work aliases, so distinct episodes
(and seasons) were merged into whichever work registered the number first —
and the mis-mapped Notes were federated. :func:`repair` cleans that up:

1. **Re-send pending retractions** — Deletes persisted by an earlier run
   (or the pipeline's episode cutoff) whose delivery can't be confirmed are
   re-broadcast; peers treat duplicates as no-ops.
2. **Rebuild** — wipe ``work`` / ``work_identifier`` and re-mint every
   archived review/listItem source through the fixed logic, re-pointing each
   record's ``work_key``. Episode listItems come back as season works
   (works.season_view).
3. **Retract** — broadcast a ``Delete`` for every still-published Note that
   remains bound to episode-level content after the rebuild (NeoDB doesn't
   federate episode-level marks, and the pipeline no longer emits them), and
   for every extra published Note beyond the one anchor a pair keeps (e.g.
   several watched-episode Notes that collapsed into one season). Each
   Delete is persisted in the row's ``ap_activity_json`` *before* it is
   enqueued, so a crash or failed delivery leaves a discoverable pending
   retraction — an unpublished row carrying a Delete — for step 1 of every
   later run. listItems converted to seasons keep their Note: step 4 Updates
   it in place under its existing object id (a Delete + Create of the same
   id would hit peers' tombstone caches).
4. **Re-sync** — statelessly: re-derive every active (author, work) pair
   Note and broadcast wherever the derivation differs from what is stored —
   an Update for a Note that referenced the wrong work or carried a moved
   partner's status, a Create for a pair whose Note ended up on a different
   work. Driven purely by stored state, never by this run's bookkeeping, so
   an interrupted run converges on the next one.

Idempotent and crash-resumable: a completed second run finds nothing to
retract or re-map and every pair derivation matching its stored Note; only
pending (undeliverable-so-far) retractions are re-broadcast, which peers
treat as no-ops.
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass, field

from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select

from skybridge.activitypub.delivery import (
    DeliveryWorker,
    deliver_to,
    fanout,
    follower_targets,
    relay_inboxes,
)
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Record, Work, WorkIdentifier, utcnow
from skybridge.pipeline import (
    _LIST_ITEM_COLLECTION,
    _PAIRED_COLLECTIONS,
    _contributes_row,
    _derive_pair,
    _pair_rows,
    _pair_trigger,
    _source_dict,
    _update_ap,
)
from skybridge.translate import neodb, works

log = logging.getLogger("skybridge.maintenance")


@dataclass
class RepairReport:
    retracted: int = 0  # freshly retracted episode Notes (Delete persisted + sent)
    resent: int = 0  # pending retractions from earlier runs re-broadcast
    works_before: int = 0
    works_after: int = 0
    remapped: int = 0  # records whose work_key changed in the rebuild
    resynced: int = 0  # pair Notes whose derivation differed from the stored one
    deliveries: int = 0  # outbound tasks enqueued (0 when run without a worker)
    dry_run: bool = False
    # (at_uri, old_work_key) of what phase 1 would retract; only kept in
    # dry-run mode, where the later phases don't run.
    would_retract: list[tuple[str, str | None]] = field(default_factory=list)


def _handle_for(did: str) -> str | None:
    with session_scope() as session:
        actor = session.get(BridgedActor, did)
        return actor.handle if actor is not None else None


def _is_episode_source(source_json: str | None) -> bool:
    source = _source_dict(source_json)
    return source is not None and source.get("creativeWorkType") == works.EPISODE_TYPE


def _historical_targets(did: str) -> list[str]:
    """Inboxes this *author's* records were ever delivered to, minus the
    current audience.

    A peer that unfollowed (or a relay since removed from the config) still
    holds the Notes it received back then; fanout() no longer reaches it, so
    retractions additionally target every inbox the delivery log remembers.
    Scoped per author rather than per record: a pair's Note is delivered
    under whichever partner record triggered the Create/Update, so a
    per-record lookup would miss partner-triggered deliveries. The superset
    is safe — a Delete for an object a peer never had is a no-op.
    """
    current = set(relay_inboxes()) | set(follower_targets(did))
    with session_scope() as session:
        past = session.scalars(
            select(Delivery.target_inbox)
            .join(Record, Record.at_uri == Delivery.record_uri)
            .where(Record.did == did)
            .distinct()
        ).all()
    return [inbox for inbox in past if inbox not in current]


async def _broadcast(
    worker: DeliveryWorker | None,
    report: RepairReport,
    *,
    at_uri: str,
    did: str,
    activity: dict,
) -> None:
    """Fan a repair activity out to the current audience AND every inbox the
    author's delivery log remembers — retractions and in-place corrections
    alike must reach peers that received the damaged Note but have since
    left the audience."""
    if worker is None:
        return
    report.deliveries += await fanout(worker, record_uri=at_uri, did=did, activity=activity)
    report.deliveries += await deliver_to(
        worker,
        record_uri=at_uri,
        did=did,
        activity=activity,
        inboxes=_historical_targets(did),
    )


async def _preview_retractions(report: RepairReport) -> None:
    """Dry-run approximation of what the mutating run would retract.

    Runs pre-rebuild, so convertible episode listItems (which the rebuild
    turns into season works whose Notes are Updated in place, never Deleted)
    are excluded by inspecting their source; duplicate-holder retractions
    can't be previewed without the rebuild and are not listed.
    """
    with session_scope() as session:
        rows = session.execute(
            select(Record.at_uri, Record.collection, Record.work_key, Record.source_json).where(
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.ap_object_json.is_not(None),
                Record.deleted_at.is_(None),
            )
        ).all()
    for at_uri, collection, work_key, source_json in rows:
        episode_bound = works.is_episode_key(work_key) or (
            work_key is None and _is_episode_source(source_json)
        )
        if not episode_bound:
            continue
        if collection == _LIST_ITEM_COLLECTION:
            source = _source_dict(source_json)
            if source is not None and works.season_view(source) is not None:
                continue
        report.would_retract.append((at_uri, work_key))
        report.retracted += 1


async def _retract(
    worker: DeliveryWorker | None,
    report: RepairReport,
    *,
    at_uri: str,
    did: str,
    collection: str,
    rkey: str,
    ap_object_json: str | None,
) -> None:
    """Retract one published Note: persist the pending Delete, then enqueue.

    The Tombstone targets the *stored* Note id — not one recomputed from the
    current handle — so the Delete names exactly the object peers received.
    Persisting the pending retraction (unpublished row carrying the Delete)
    BEFORE enqueueing means a crash or failed delivery leaves durable state
    that _resend_pending_retractions rediscovers, instead of a Note stranded
    on peers with nothing left to find it by.
    """
    note_id = None
    with contextlib.suppress(TypeError, ValueError):
        note_id = (json.loads(ap_object_json or "") or {}).get("id")
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
    with session_scope() as session:
        row = session.get(Record, at_uri)
        if row is not None:
            row.ap_object_json = None
            row.ap_activity_json = json.dumps(activity) if activity is not None else None
            row.updated_at = utcnow()
    report.retracted += 1
    if activity is not None:
        await _broadcast(worker, report, at_uri=at_uri, did=did, activity=activity)


async def _retract_episode_notes(worker: DeliveryWorker | None, report: RepairReport) -> None:
    """Delete every published Note still bound to episode-level content.

    Runs after the rebuild, so listItems converted to season works are
    already off the tv_episode keys and keep their Note (to be Updated in
    place — a Delete + Create of the same rkey-derived object id would hit
    peers' tombstone caches). Also covers episode records whose identifiers
    couldn't mint any work (work_key NULL) but that published a generic Note
    before the episode cutoff existed.
    """
    with session_scope() as session:
        rows = session.execute(
            select(
                Record.at_uri,
                Record.did,
                Record.collection,
                Record.rkey,
                Record.work_key,
                Record.source_json,
            ).where(
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.ap_object_json.is_not(None),
                Record.deleted_at.is_(None),
            )
        ).all()
    for at_uri, did, collection, rkey, work_key, source_json in rows:
        episode_bound = works.is_episode_key(work_key) or (
            work_key is None and _is_episode_source(source_json)
        )
        if not episode_bound:
            continue
        with session_scope() as session:
            row = session.get(Record, at_uri)
            ap_object_json = row.ap_object_json if row is not None else None
        await _retract(
            worker,
            report,
            at_uri=at_uri,
            did=did,
            collection=collection,
            rkey=rkey,
            ap_object_json=ap_object_json,
        )


async def _retract_duplicate_holders(worker: DeliveryWorker | None, report: RepairReport) -> None:
    """Keep at most one published Note per (author, work) pair.

    Legacy episode Notes that collapsed into one season during the rebuild
    (one per watched episode) all stay published, but a pair has a single
    anchor — the others would keep their stale content federated forever.
    The row _pair_rows selects as holder keeps its Note (which _resync_pairs
    then corrects in place); every other published *contributing* row is
    Deleted. Published status-less membership rows hold standalone Notes by
    design and are left alone.
    """
    with session_scope() as session:
        pairs = session.execute(
            select(Record.did, Record.work_key)
            .where(
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.work_key.is_not(None),
                Record.ap_object_json.is_not(None),
                Record.deleted_at.is_(None),
            )
            .distinct()
            .order_by(Record.did.asc(), Record.work_key.asc())
        ).all()

    for did, work_key in pairs:
        if works.is_episode_key(work_key):
            continue  # episode-keyed rows are retracted wholesale above
        with session_scope() as session:
            rows = session.execute(
                select(Record.at_uri, Record.collection, Record.rkey, Record.source_json)
                .where(
                    Record.did == did,
                    Record.work_key == work_key,
                    Record.collection.in_(_PAIRED_COLLECTIONS),
                    Record.ap_object_json.is_not(None),
                    Record.deleted_at.is_(None),
                )
                .order_by(Record.created_at.asc(), Record.at_uri.asc())
            ).all()
        contributing = [r for r in rows if _contributes_row(r[1], r[3])]
        if len(contributing) <= 1:
            continue
        _, _, holder = _pair_rows(did, work_key)
        keep = holder.at_uri if holder is not None else contributing[-1][0]
        for at_uri, collection, rkey, _source_json in contributing:
            if at_uri == keep:
                continue
            with session_scope() as session:
                row = session.get(Record, at_uri)
                ap_object_json = row.ap_object_json if row is not None else None
            await _retract(
                worker,
                report,
                at_uri=at_uri,
                did=did,
                collection=collection,
                rkey=rkey,
                ap_object_json=ap_object_json,
            )


def _rebuild_works(report: RepairReport) -> list[tuple[str, str, str | None, str | None]]:
    """Re-mint every archived review/listItem source through the fixed logic.

    Returns ``(did, at_uri, old_work_key, new_work_key)`` for each record
    whose work_key changed. Records are replayed in original arrival order so
    the first record's best identifier anchors each work_key, as in live
    ingestion.

    The wipe, every re-mint, and every work_key update commit as ONE
    transaction: concurrent ingestion can never mint against a half-rebuilt
    catalog or have its freshly-written work_key clobbered from a stale
    snapshot (its write serializes before the snapshot or after the commit),
    and a crash mid-rebuild rolls the whole phase back.
    """
    remapped: list[tuple[str, str, str | None, str | None]] = []
    with session_scope() as session:
        # The alias wipe goes FIRST so the transaction's opening statement is
        # a write: SQLite's deferred transactions start as read transactions
        # on a SELECT and abort with SQLITE_BUSY when upgrading to write
        # after a concurrent writer commits — write-first is equivalent to
        # BEGIN IMMEDIATE. The works count is read after; Work rows are
        # still intact at that point.
        session.execute(sql_delete(WorkIdentifier))
        report.works_before = session.scalar(select(func.count()).select_from(Work)) or 0
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
            ref = works.mint(source, session=session)
            new_key = ref.work_key if ref is not None else None
            if new_key == old_key:
                continue
            row = session.get(Record, at_uri)
            if row is not None:
                row.work_key = new_key
                row.updated_at = utcnow()
            remapped.append((did, at_uri, old_key, new_key))

        report.works_after = session.scalar(select(func.count()).select_from(Work)) or 0
    report.remapped = len(remapped)
    return remapped


async def _resend_pending_retractions(worker: DeliveryWorker | None, report: RepairReport) -> None:
    """Re-broadcast retractions persisted by an earlier, interrupted run.

    A pending retraction is a row whose Note was unpublished but that still
    carries a Delete in ``ap_activity_json`` (written by
    _retract_episode_notes or the pipeline's episode cutoff). Delivery is
    best-effort and in-memory, so the payload is kept and re-sent on every
    run — peers treat a Delete for an already-deleted object as a no-op.

    Tombstoned rows are deliberately included: deleting the source record
    after a retraction went pending keeps the pending Delete (see
    _process_delete's no-note branch), and the stranded remote Note still
    needs it. Rows deleted the ordinary way keep their ``ap_object_json``
    alongside the Delete, so they don't match this shape.
    """
    with session_scope() as session:
        rows = session.execute(
            select(Record.at_uri, Record.did, Record.ap_activity_json).where(
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.ap_object_json.is_(None),
                Record.ap_activity_json.is_not(None),
            )
        ).all()
    for at_uri, did, activity_json in rows:
        activity = None
        with contextlib.suppress(TypeError, ValueError):
            activity = json.loads(activity_json)
        if not isinstance(activity, dict) or activity.get("type") != "Delete":
            continue
        report.resent += 1
        await _broadcast(worker, report, at_uri=at_uri, did=did, activity=activity)


# Timestamps that legitimately differ between a stored Note and a fresh
# derivation of the same state (Updates are stamped at derivation time).
_VOLATILE_NOTE_KEYS = frozenset({"published", "updated"})


def _comparable(node):
    """A Note stripped of volatile timestamps, for change detection."""
    if isinstance(node, dict):
        return {k: _comparable(v) for k, v in node.items() if k not in _VOLATILE_NOTE_KEYS}
    if isinstance(node, list):
        return [_comparable(v) for v in node]
    return node


async def _resync_pairs(worker: DeliveryWorker | None, report: RepairReport) -> None:
    """Re-derive every active (author, work) pair; broadcast what changed.

    Stateless on purpose: driven by comparing each pair's fresh derivation
    against the Note stored on its anchor, not by which records this run
    happened to remap — so a run interrupted between the rebuild and this
    phase still converges on the next run. Covers both damage patterns: a
    published Note that referenced the wrong work or still folds a moved
    partner's status (Update), and a pair left Note-less because its records
    moved to a new work (Create, anchored on a contributing row). Episode
    works are never re-published.
    """
    with session_scope() as session:
        pairs = session.execute(
            select(Record.did, Record.work_key)
            .where(
                Record.collection.in_(_PAIRED_COLLECTIONS),
                Record.work_key.is_not(None),
                Record.deleted_at.is_(None),
            )
            .distinct()
            .order_by(Record.did.asc(), Record.work_key.asc())
        ).all()

    for did, work_key in pairs:
        if works.is_episode_key(work_key):
            continue
        trigger_uri = _pair_trigger(did, work_key)
        if trigger_uri is None:
            continue
        handle = _handle_for(did)
        if handle is None:
            continue
        derived = _derive_pair(did=did, work_key=work_key, handle=handle, trigger_uri=trigger_uri)
        if derived is None:
            continue
        stored = None
        if derived.stored_note_json:
            with contextlib.suppress(TypeError, ValueError):
                stored = json.loads(derived.stored_note_json)
        if stored is not None and _comparable(stored) == _comparable(derived.note):
            continue
        # Enqueue before persisting: once the corrected Note is stored, a
        # later run derives identical content and skips the pair, so a crash
        # in between must cost a duplicate Update on the rerun — never a
        # correction peers silently miss. (An enqueued delivery that exhausts
        # its retries is logged in the delivery table; like every skybridge
        # broadcast, that last mile is best-effort.)
        await _broadcast(
            worker, report, at_uri=derived.anchor_uri, did=did, activity=derived.activity
        )
        _update_ap(derived.anchor_uri, derived.note, derived.activity)
        report.resynced += 1


async def repair(worker: DeliveryWorker | None = None, *, dry_run: bool = False) -> RepairReport:
    """Run the full repair. ``dry_run`` reports phase 1 and stops (phases 2-3
    can't be previewed without mutating the works tables).

    A mutating run destroys the state its broadcasts derive from (stored Note
    ids, episode-typed work_keys), so ``worker=None`` — which mutates without
    broadcasting — is for tests only; the CLI always passes a worker.
    """
    report = RepairReport(dry_run=dry_run)
    if dry_run:
        await _preview_retractions(report)
        return report
    # Pending first, so a Delete persisted later this run isn't immediately
    # re-found as "pending" and sent twice; retractions after the rebuild, so
    # listItems converted to seasons are already off the episode keys and
    # keep their Note for the in-place Update.
    await _resend_pending_retractions(worker, report)
    _rebuild_works(report)
    await _retract_episode_notes(worker, report)
    await _retract_duplicate_holders(worker, report)
    await _resync_pairs(worker, report)
    log.info(
        "repair: retracted=%d resent=%d works %d->%d remapped=%d resynced=%d deliveries=%d",
        report.retracted,
        report.resent,
        report.works_before,
        report.works_after,
        report.remapped,
        report.resynced,
        report.deliveries,
    )
    return report
