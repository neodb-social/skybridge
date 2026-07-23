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

RUN WITH THE INGEST PROCESS STOPPED. The rebuild transaction serializes
against concurrent SQLite writers, but a long write lock can make a
concurrent ingest transaction fail with SQLITE_BUSY after Jetstream has
already advanced its cursor — permanently skipping that event. Repair is a
brief one-shot; pausing ingestion for its duration is the safe procedure.
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
from skybridge.models import (
    BridgedActor,
    Delivery,
    Follow,
    Record,
    Relay,
    Work,
    WorkIdentifier,
    utcnow,
)
from skybridge.pipeline import (
    _LIST_ITEM_COLLECTION,
    _PAIRED_COLLECTIONS,
    _contributes_row,
    _derive_pair,
    _pair_rows,
    _pair_trigger,
    _source_dict,
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


def _pair_historical_targets(did: str, work_key: str) -> list[str]:
    """Historical recipients of THIS pair's Note, minus the current audience.

    Correction (Update/Create) deliveries are scoped to inboxes that were
    sent one of the pair's own *active* records — peers that plausibly hold
    the damaged Note. Tombstoned records are excluded (their Notes were
    already Deleted; those recipients don't hold the pair's current Note),
    and the author-wide superset used for Deletes would push new public
    content to former followers who never had this Note.
    """
    current = set(relay_inboxes()) | set(follower_targets(did))
    with session_scope() as session:
        past = session.scalars(
            select(Delivery.target_inbox)
            .join(Record, Record.at_uri == Delivery.record_uri)
            .where(
                Record.did == did,
                Record.work_key == work_key,
                Record.deleted_at.is_(None),
            )
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
    historical: list[str],
) -> None:
    """Fan a repair activity out to the current audience AND the given
    historical inboxes — peers that received the damaged Note but have since
    left the audience must still get its retraction/correction."""
    if worker is None:
        return
    report.deliveries += await fanout(worker, record_uri=at_uri, did=did, activity=activity)
    report.deliveries += await deliver_to(
        worker, record_uri=at_uri, did=did, activity=activity, inboxes=historical
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
        await _broadcast(
            worker,
            report,
            at_uri=at_uri,
            did=did,
            activity=activity,
            historical=_historical_targets(did),
        )


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


async def _retract_remapped_nonpaired(
    worker: DeliveryWorker | None,
    report: RepairReport,
    remapped: list[tuple[str, str, str | None, str | None]],
) -> None:
    """Retract published Notes of remapped records outside the pair machinery.

    Only the paired collections publish work-bearing Notes today, so this is
    a safety net for future work-bearing collections (and hand-seeded
    archives): such a Note can't be re-derived by _resync_pairs, and after
    the rebuild moved its record to a different work it references a catalog
    entry that no longer exists — a stale federated Note is worse than none.
    """
    for did, at_uri, _old_key, _new_key in remapped:
        with session_scope() as session:
            row = session.get(Record, at_uri)
            if (
                row is None
                or row.collection in _PAIRED_COLLECTIONS
                or row.ap_object_json is None
                or row.deleted_at is not None
            ):
                continue
            collection, rkey, ap_object_json = row.collection, row.rkey, row.ap_object_json
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
    """Re-mint every archived record source through the fixed logic.

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
            # Every archived record, not just the paired collections: the
            # wipe above removed works minted from ANY collection, so any
            # source that can mint must be replayed or its catalog entry
            # (and its records' work_keys) would dangle. Sources with no
            # resolvable work (lists, ...) are no-ops here.
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
        await _broadcast(
            worker,
            report,
            at_uri=at_uri,
            did=did,
            activity=activity,
            historical=_historical_targets(did),
        )


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


async def _resync_pairs(
    worker: DeliveryWorker | None, report: RepairReport
) -> list[tuple[str, str | None, dict, dict]]:
    """Re-derive every active (author, work) pair; broadcast what changed.

    Stateless on purpose: driven by comparing each pair's fresh derivation
    against the Note stored on its anchor, not by which records this run
    happened to remap — so a run interrupted between the rebuild and this
    phase still converges on the next run. Covers both damage patterns: a
    published Note that referenced the wrong work or still folds a moved
    partner's status (Update), and a pair left Note-less because its records
    moved to a new work (Create, anchored on a contributing row). Episode
    works are never re-published.

    Corrections are enqueued here but NOT persisted: the un-updated stored
    Note is the durable pending marker. Returns ``(anchor_uri,
    observed_note_json, note, activity)`` for the caller to persist only
    after the delivery queue has drained — a process death anywhere before
    that leaves the divergence detectable, so the next run re-derives and
    re-sends instead of skipping a correction peers never received.
    """
    pending: list[tuple[str, str | None, dict, dict]] = []
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
        correction = await _resync_pair(worker, report, did, work_key)
        if correction is not None:
            pending.append(correction)
    return pending


async def _resync_pair(
    worker: DeliveryWorker | None, report: RepairReport, did: str, work_key: str
) -> tuple[str, str | None, dict, dict] | None:
    """Derive one (author, work) pair; broadcast and return the correction if
    it differs from the stored Note, ``None`` when already in sync."""
    if works.is_episode_key(work_key):
        return None
    trigger_uri = _pair_trigger(did, work_key)
    if trigger_uri is None:
        return None
    handle = _handle_for(did)
    if handle is None:
        return None
    derived = _derive_pair(did=did, work_key=work_key, handle=handle, trigger_uri=trigger_uri)
    if derived is None:
        return None
    stored = None
    if derived.stored_note_json:
        with contextlib.suppress(TypeError, ValueError):
            stored = json.loads(derived.stored_note_json)
    if stored is not None and _comparable(stored) == _comparable(derived.note):
        return None
    await _broadcast(
        worker,
        report,
        at_uri=derived.anchor_uri,
        did=did,
        activity=derived.activity,
        historical=_pair_historical_targets(did, work_key),
    )
    report.resynced += 1
    return (derived.anchor_uri, derived.stored_note_json, derived.note, derived.activity)


async def _finalize_corrections(
    worker: DeliveryWorker | None,
    corrections: list[tuple[str, str | None, dict, dict]],
    started_at,
) -> None:
    """Persist corrected Notes only after every enqueued delivery has had its
    bounded retries: the stale stored Note is the durable pending marker, so
    dying before this point leaves each correction re-derivable (and
    re-sendable) by the next run, at worst as a duplicate Update."""
    if worker is not None:
        await worker.drain()
    for anchor_uri, observed, note, activity in corrections:
        if worker is not None and _delivery_failed_since(anchor_uri, started_at):
            # Every bounded retry to at least one inbox failed (temporary
            # outage): keep the stored Note stale so the next run re-derives
            # and re-sends this correction instead of considering it done.
            continue
        _persist_correction(anchor_uri, observed, note, activity)


def _delivery_failed_since(record_uri: str, since) -> bool:
    """Did any delivery for *record_uri* end in failure during this run?

    _record_attempt keeps one row per (record_uri, inbox) whose status
    reflects the LAST attempt, so after drain() a ``failed`` row newer than
    the run start means the bounded retry schedule was exhausted.
    """
    with session_scope() as session:
        failed = session.scalar(
            select(func.count())
            .select_from(Delivery)
            .where(
                Delivery.record_uri == record_uri,
                Delivery.status == "failed",
                Delivery.last_attempt >= since,
            )
        )
    return bool(failed)


def _persist_correction(
    anchor_uri: str, observed_note_json: str | None, note: dict, activity: dict
) -> None:
    """Store the corrected Note — unless the anchor changed while we drained.

    A live event may have re-derived the same anchor during the delivery
    window; its state is newer than this correction (and was broadcast by
    its own pipeline flow), so an unconditional write would roll it back to
    the pre-drain derivation.
    """
    with session_scope() as session:
        row = session.get(Record, anchor_uri)
        if row is None or row.ap_object_json != observed_note_json:
            return
        row.ap_object_json = json.dumps(note)
        row.ap_activity_json = json.dumps(activity)
        row.updated_at = utcnow()


async def repair(worker: DeliveryWorker | None = None, *, dry_run: bool = False) -> RepairReport:
    """Run the full repair. ``dry_run`` reports phase 1 and stops (phases 2-3
    can't be previewed without mutating the works tables).

    A mutating run destroys the state its broadcasts derive from (stored Note
    ids, episode-typed work_keys), so ``worker=None`` — which mutates without
    broadcasting — is for tests only; the CLI always passes a worker.
    """
    report = RepairReport(dry_run=dry_run)
    started_at = utcnow()
    if dry_run:
        await _preview_retractions(report)
        return report
    # Pending first, so a Delete persisted later this run isn't immediately
    # re-found as "pending" and sent twice; retractions after the rebuild, so
    # listItems converted to seasons are already off the episode keys and
    # keep their Note for the in-place Update.
    await _resend_pending_retractions(worker, report)
    remapped = _rebuild_works(report)
    await _retract_episode_notes(worker, report)
    await _retract_remapped_nonpaired(worker, report, remapped)
    await _retract_duplicate_holders(worker, report)
    corrections = await _resync_pairs(worker, report)
    await _finalize_corrections(worker, corrections, started_at)
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


# --------------------------------------------------------------------------- #
# repair3: normalize episode-shaped titles on historical show/season works
# --------------------------------------------------------------------------- #


@dataclass
class TitleRepairReport:
    dry_run: bool = True
    # (work_key, old_title, new_title) for every work whose title changed
    retitled: list[tuple[str, str | None, str | None]] = field(default_factory=list)
    resynced: int = 0  # pair Notes re-broadcast with the corrected title
    deliveries: int = 0


async def repair_titles(
    worker: DeliveryWorker | None = None, *, dry_run: bool = False
) -> TitleRepairReport:
    """Normalize historical tv_show / tv_season work titles.

    popfeed labels show- and season-typed records with the watched episode's
    title ("Baron Noir - S1E5 - Grenelle"); works minted before
    works.normalize_title existed carry it verbatim. This re-runs the
    normalization over the stored catalog — a tv_show keeps just the show
    name, a tv_season keeps its season ("<show> - Season <n>") — and
    re-broadcasts the pair Notes on retitled works so federated copies pick
    up the corrected name too (same drain-then-persist guarantees as
    ``repair``'s re-sync).

    Title-only: no catalog rebuild, no retractions. Safe while serving —
    the title write is one quick transaction and Note persists are
    compare-and-swap guarded against concurrent live updates.
    """
    report = TitleRepairReport(dry_run=dry_run)
    started_at = utcnow()
    with session_scope() as session:
        rows = session.execute(
            select(Work.work_key, Work.title, Work.identifiers_json).where(
                Work.creative_work_type.in_(("tv_show", works.SEASON_TYPE))
            )
        ).all()
    for work_key, title, identifiers_json in rows:
        identifiers = _source_dict(identifiers_json) or {}
        work_type = work_key.partition(":")[0]
        new_title = works.normalize_title(work_type, title, identifiers)
        if new_title != title:
            report.retitled.append((work_key, title, new_title))
    if dry_run:
        return report

    with session_scope() as session:
        for work_key, _old, new_title in report.retitled:
            row = session.get(Work, work_key)
            if row is not None:
                row.title = new_title

    # Re-broadcast the pair Notes that embed the old title. Scoped to the
    # retitled works — unlike repair's full-catalog re-sync — so this stays
    # cheap enough to run while serving.
    inner = RepairReport()
    corrections: list[tuple[str, str | None, dict, dict]] = []
    for work_key, _old, _new in report.retitled:
        with session_scope() as session:
            dids = session.scalars(
                select(Record.did)
                .where(
                    Record.work_key == work_key,
                    Record.collection.in_(_PAIRED_COLLECTIONS),
                    Record.deleted_at.is_(None),
                )
                .distinct()
            ).all()
        for did in dids:
            correction = await _resync_pair(worker, inner, did, work_key)
            if correction is not None:
                corrections.append(correction)
    await _finalize_corrections(worker, corrections, started_at)
    report.resynced = inner.resynced
    report.deliveries = inner.deliveries
    log.info(
        "repair3: retitled=%d resynced=%d deliveries=%d",
        len(report.retitled),
        report.resynced,
        report.deliveries,
    )
    return report


# --------------------------------------------------------------------------- #
# repair2: prune stale delivery-log rows
# --------------------------------------------------------------------------- #


@dataclass
class DeliveryPruneReport:
    dry_run: bool = True
    kept: list[tuple[str, int]] = field(default_factory=list)  # (inbox, rows) still current
    stale: list[tuple[str, int]] = field(default_factory=list)  # (inbox, rows) pruned
    deleted: int = 0


def _current_audience_inboxes() -> set[str]:
    """Every inbox we could still deliver to.

    Every *accepted* relay (read straight from the Relay table, NOT via
    relay_inboxes() — that intersects with the SKYBRIDGE_RELAYS env var, so a
    run without it set would wrongly treat the live relay as stale and prune
    its rows) plus every accepted follower's inbox, both plain and shared
    forms since the log records whichever was used at send time. A delivery
    row whose target is absent here can never receive traffic again — a relay
    removed from the Relay table, or an unfollowed peer.
    """
    inboxes: set[str] = set()
    with session_scope() as session:
        for inbox in session.scalars(select(Relay.inbox).where(Relay.state == "accepted")):
            inboxes.add(inbox)
        for inbox, shared in session.execute(
            select(Follow.follower_inbox, Follow.follower_shared_inbox).where(
                Follow.state == "accepted"
            )
        ).all():
            if inbox:
                inboxes.add(inbox)
            if shared:
                inboxes.add(shared)
    return inboxes


def prune_stale_deliveries(*, delete: bool = False) -> DeliveryPruneReport:
    """Delete delivery-log rows whose target inbox left the current audience.

    The ``delivery`` table is an append-only-ish log: retries/stats read it,
    and repair's historical-target discovery re-reaches past recipients
    through it. Rows for inboxes we can no longer deliver to (a removed relay
    such as a decommissioned ``eggplant.place``, an unfollowed peer) are dead
    weight — they bloat the log and make ``repair`` waste time trying to
    re-reach a dead host. Current relay + follower inboxes are always kept.

    ``delete=False`` (default) only reports; pass ``delete=True`` to apply.
    Safe to run while serving — it is a single quick DELETE, not the heavy
    catalog rebuild ``repair`` performs.

    Trade-off: pruning an inbox also drops repair's ability to send it a
    later retraction/correction. That's the intended effect for dead relays;
    for a genuinely unfollowed human peer it means a future repair won't
    reach them (acceptable — they left, and a re-follow re-logs deliveries).
    """
    report = DeliveryPruneReport(dry_run=not delete)
    current = _current_audience_inboxes()
    with session_scope() as session:
        counts = session.execute(
            select(Delivery.target_inbox, func.count()).group_by(Delivery.target_inbox)
        ).all()
    for inbox, count in counts:
        (report.kept if inbox in current else report.stale).append((inbox, count))
    if delete and report.stale:
        stale_inboxes = [inbox for inbox, _ in report.stale]
        with session_scope() as session:
            result = session.execute(
                sql_delete(Delivery).where(Delivery.target_inbox.in_(stale_inboxes))
            )
            report.deleted = getattr(result, "rowcount", 0) or 0
    return report
