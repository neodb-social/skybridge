"""Historical import from the Jetstream v2 archive (Network Replay).

The live tail only reaches back a bounded window (36 hours on Bluesky-hosted
instances). To seed the bridge with history, Jetstream v2 serves its whole
archive over HTTP: ``planSnapshot`` says which segments and blocks can hold
matching events, then ``getBlock``/``getSegment`` deliver them. Those calls
are authenticated and metered — the live WebSocket is neither — so this module
is the only place ``SKYBRIDGE_JETSTREAM_API_KEY`` is used.

Three properties matter, and each is enforced here rather than trusted:

*Never overwrites newer data.* An import is bounded above by the live ingest
cursor at request time, and every event is additionally checked against the
record's ``last_seq`` high-water mark in :func:`skybridge.pipeline.process_event`.
An archived event can therefore never regress a record the live tail already
advanced, whichever order they happen to interleave in.

*Honours opt-out.* Opted-out DIDs are dropped before decode, and the pipeline
re-checks per event, so an opt-out takes effect mid-import. A *delivering*
import is additionally interrupted by ``optout.opt_out`` (see
:func:`cancel_if_delivering`), because that is the only mode that could
enqueue a ``Create`` behind the purge's ``Delete``. This is only possible
because the import runs as a task inside the server process — an
out-of-process import could not be stopped, which is the hazard
``optout.purge_did`` documents for ``backfill --deliver``.

*Coexists with live ingest.* It shares the event loop with the live tail, so
network and decode work happen off-thread and the loop is yielded between
blocks. Progress is persisted per block, so a metering 429 or a redeploy
resumes instead of re-downloading (and re-paying for) what it already has.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Any

import httpx

from skybridge import optout
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto import jss
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Cursor, ImportJob, utcnow
from skybridge.pipeline import process_event

log = logging.getLogger("skybridge.archive")

_PLAN = "network.bsky.jetstream.planSnapshot"
_GET_BLOCK = "network.bsky.jetstream.getBlock"
_GET_SEGMENT = "network.bsky.jetstream.getSegment"

# Whole-segment downloads stream in chunks so a 256 MB file never lands in
# memory at once and a 429 mid-download only costs the current chunk.
_SEGMENT_CHUNK = 8 * 1024 * 1024

# Blocks fetched concurrently within a segment. Bounded deliberately: the
# archive is metered by bytes, so this trades latency for parallelism without
# increasing what a run downloads, and keeps the in-flight window small enough
# that a cancellation or 429 wastes at most this many requests.
_BLOCK_PREFETCH = 8

# Cap on how long we honour a Retry-After before giving up on the run; the
# quota refills continuously, so a longer wait than this means the budget is
# exhausted rather than momentarily tight.
_MAX_RETRY_AFTER = 3600

# Sizing constants for the dry-run estimate. Block sizes are not in the plan;
# these are the measured averages (~250 KB compressed per 4096-event block,
# ~250 MB per sealed segment).
_AVG_BLOCK_BYTES = 250_000
_AVG_SEGMENT_BYTES = 250 * 1024 * 1024

# Transient transport failures (DNS hiccup, dropped connection) retried per
# request before the run gives up. Long imports make tens of thousands of
# requests, so the odds of hitting at least one blip approach certainty.
_TRANSPORT_RETRIES = 5

# How long a job may sit in `running` without its row being touched before we
# assume the process owning it died. A live import writes progress after every
# segment, and the slowest single segment (a whole-file fallback: download plus
# decode) is well inside this, so a heartbeat older than it means nobody is
# working the job.
_HEARTBEAT_STALE_AFTER = timedelta(minutes=5)

# Seconds before the first transport retry, doubling to _MAX_RETRY_BACKOFF.
# Module-level so tests can collapse it to 0 without patching asyncio itself.
_RETRY_BACKOFF_BASE = 1.0
_MAX_RETRY_BACKOFF = 30.0


class ArchiveError(RuntimeError):
    """Unrecoverable problem talking to the archive (auth, config, protocol)."""


@dataclass(frozen=True)
class PlanEstimate:
    """Dry-run summary: what a full import would cost before committing to it."""

    segments: int
    whole_segments: int
    block_ranges: int
    blocks: int
    # The planner's own `stats.entries`: work units it accounted for, NOT a
    # count of matching records. Measured against a real full BookHive replay
    # it understates the records recovered by more than an order of magnitude,
    # so it must never be presented as "how much data you will get".
    planner_entries: int
    estimated_bytes: int
    after_seq: int
    before_seq: int


def _client() -> httpx.AsyncClient:
    settings = get_settings()
    if not settings.jetstream_api_key:
        raise ArchiveError(
            "SKYBRIDGE_JETSTREAM_API_KEY is required for archive replay "
            "(the live WebSocket needs no key, but the HTTP archive does)"
        )
    if not settings.jetstream_is_v2:
        raise ArchiveError(
            f"archive replay needs a Jetstream v2 endpoint; SKYBRIDGE_JETSTREAM "
            f"is {settings.jetstream_url}"
        )
    return httpx.AsyncClient(
        base_url=f"{settings.jetstream_http_base}/xrpc",
        headers={
            "Authorization": f"Bearer {settings.jetstream_api_key}",
            "User-Agent": settings.user_agent,
        },
        timeout=httpx.Timeout(120.0, connect=15.0),
    )


async def _retry_after(response: httpx.Response) -> bool:
    """Handle a metering 429; ``True`` if the caller should retry.

    Metering is by response bytes and the quota refills continuously, so the
    documented recovery is simply to wait out ``Retry-After``.
    """
    if response.status_code != 429:
        return False
    delay = 60
    with suppress(TypeError, ValueError):
        delay = int(response.headers.get("Retry-After", 60))
    if delay > _MAX_RETRY_AFTER:
        raise ArchiveError(f"archive byte budget exhausted (Retry-After: {delay}s)")
    log.info("archive rate limited; waiting %ss", delay)
    await asyncio.sleep(delay)
    return True


def _raise_for_auth(response: httpx.Response) -> None:
    if response.status_code in (401, 403):
        raise ArchiveError(
            f"archive rejected the API key ({response.status_code}): {response.text[:200]}"
        )


async def _send(send: Callable[[], Awaitable[httpx.Response]], *, what: str) -> httpx.Response:
    """Issue one archive request, absorbing rate limits and network blips.

    A full-history import is tens of thousands of requests over the better
    part of an hour, so a single transient DNS or connection failure must not
    end it — one such failure did abort a popfeed run at 79%. Transport errors
    are retried with exponential backoff; status errors are not, since they
    would only repeat.
    """
    delay = _RETRY_BACKOFF_BASE
    attempts = 0
    while True:
        try:
            response = await send()
        except httpx.TransportError as exc:
            attempts += 1
            if attempts > _TRANSPORT_RETRIES:
                raise
            log.warning(
                "archive %s: %s (%s); retry %d/%d in %.0fs",
                what,
                type(exc).__name__,
                exc,
                attempts,
                _TRANSPORT_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RETRY_BACKOFF)
            continue
        if await _retry_after(response):
            continue
        _raise_for_auth(response)
        response.raise_for_status()
        return response


async def _plan_page(
    client: httpx.AsyncClient, *, after_seq: int, before_seq: int | None
) -> dict[str, Any]:
    """One ``planSnapshot`` page for the bridged collections."""
    body: dict[str, Any] = {
        "collections": list(get_settings().wanted_collections),
        "afterSeq": after_seq,
    }
    if before_seq:
        body["beforeSeq"] = before_seq
    response = await _send(lambda: client.post(_PLAN, json=body), what="planSnapshot")
    return response.json()


async def estimate(*, after_seq: int = 0, before_seq: int | None = None) -> PlanEstimate:
    """Plan the import without downloading anything.

    Worth running first: the planner works from bloom filters and per-block
    summaries, so for a collection as rare as popfeed's it matches far more of
    the archive than actually holds matching rows, and a full-history import
    can run to tens of gigabytes for a few thousand events.
    """
    async with _client() as client:
        if before_seq is None:
            before_seq = _live_cursor()
        segments = 0
        whole = 0
        ranges = 0
        blocks = 0
        planner_entries = 0
        est_bytes = 0

        cursor = after_seq
        sealed_tip = 0
        while True:
            plan = await _plan_page(client, after_seq=cursor, before_seq=before_seq)
            sealed_tip = plan.get("sealedTipSeq", 0)
            for segment in plan.get("segments", []):
                segments += 1
                if segment.get("mode") == "segment":
                    whole += 1
                    # Resolve it the way the import will, rather than charging
                    # the whole 250 MB file: the importer reads the collection
                    # index and fetches only the blocks it names, so charging
                    # the file size overstated a real popfeed estimate ~6x —
                    # and the estimate is the gate before spending quota.
                    indices, index_cost = await _blocks_from_index(client, segment["name"])
                    est_bytes += index_cost
                    if indices is None:
                        est_bytes += _AVG_SEGMENT_BYTES  # whole-file fallback
                    else:
                        blocks += len(indices)
                        est_bytes += len(indices) * _AVG_BLOCK_BYTES
                else:
                    spans = segment.get("blocks") or []
                    ranges += len(spans)
                    n = sum(span["last"] - span["first"] + 1 for span in spans)
                    blocks += n
                    est_bytes += n * _AVG_BLOCK_BYTES
            planner_entries += plan.get("stats", {}).get("entries", 0)
            planned = plan.get("plannedThroughSeq", 0)
            target = before_seq or sealed_tip
            if planned >= target or planned <= cursor:
                break
            cursor = planned

        return PlanEstimate(
            segments=segments,
            whole_segments=whole,
            block_ranges=ranges,
            blocks=blocks,
            planner_entries=planner_entries,
            estimated_bytes=est_bytes,
            after_seq=after_seq,
            before_seq=before_seq or sealed_tip,
        )


def _live_cursor() -> int:
    """The live tail's current ``seq``, used as the import's upper bound.

    Bounding the import strictly below the live cursor is what lets the
    staleness rule treat a pre-v2 record row (no ``last_seq``) as newer than
    anything the archive can offer.
    """
    with session_scope() as session:
        row = session.get(Cursor, 1)
        return (row.seq or 0) if row is not None else 0


async def _fetch_block(client: httpx.AsyncClient, segment: str, index: int) -> bytes:
    response = await _send(
        lambda: client.get(_GET_BLOCK, params={"segment": segment, "blockIndex": index}),
        what=f"getBlock {segment}#{index}",
    )
    return response.content


async def _fetch_segment(client: httpx.AsyncClient, name: str) -> bytes:
    """Download a whole segment, resuming with Range after a 429.

    Bytes already received are never re-requested, so an interrupted download
    is not re-charged against the quota.

    Note the parameter is ``name``, where ``getBlock`` takes ``segment`` — the
    two endpoints spell it differently.
    """
    chunks: list[bytes] = []
    received = 0
    while True:
        headers = {"Range": f"bytes={received}-"} if received else {}
        try:
            async with client.stream(
                "GET", _GET_SEGMENT, params={"name": name}, headers=headers
            ) as response:
                if response.status_code == 429:
                    await response.aread()
                    if await _retry_after(response):
                        continue
                _raise_for_auth(response)
                response.raise_for_status()
                async for chunk in response.aiter_bytes(_SEGMENT_CHUNK):
                    chunks.append(chunk)
                    received += len(chunk)
            return b"".join(chunks)
        except httpx.TransportError:
            # Only a dropped connection is worth resuming. A status error
            # (bad request, revoked key) would repeat identically, so it must
            # propagate rather than spin here forever.
            if not received:
                raise
            log.info("segment %s interrupted at %d bytes; resuming", name, received)


def _wanted(event: dict[str, Any]) -> bool:
    """Exact filter over what the planner returned.

    The planner guarantees no false negatives but does include blocks holding
    nothing we want (it plans from bloom filters without opening files), so
    every decoded row is re-checked against the real collection list.
    """
    return event.get("collection") in get_settings().wanted_collections


async def _apply(events: list[dict[str, Any]], *, worker: DeliveryWorker | None) -> int:
    """Feed decoded events through the pipeline, exactly as live ingest does.

    ``allow_network`` stays on: identity is resolved once per *new* DID (see
    identity.ensure_actor), and without it every account the import discovers
    would be minted under the synthetic ``<did-tail>.did`` fallback handle.
    That handle is the actor's public URL and is only ever resolved at
    creation time, so it would then stick for good.
    """
    applied = 0
    for event in events:
        if not _wanted(event):
            continue
        if optout.is_opted_out(event.get("did", "")):
            # Cheap pre-check; process_event re-checks authoritatively.
            continue
        result = await process_event(event, worker=worker, from_archive=True)
        if result is not None:
            applied += 1
        # Yield so live ingest is not starved by a long import.
        await asyncio.sleep(0)
    return applied


def _update_job(job_id: int, **fields: Any) -> None:
    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        if job is None:
            return
        for key, value in fields.items():
            setattr(job, key, value)
        job.updated_at = utcnow()


def _claim_job(job_id: int) -> bool:
    """Take ownership of a job. ``False`` if someone else already has it.

    One conditional UPDATE, so the transition out of ``pending``/``paused`` is
    atomic: if ``python -m skybridge import --run`` and the server's
    :func:`watch_jobs` both go for the same row, exactly one wins and the other
    backs off. Without this both would download the same metered blocks and
    race on the same progress counters.
    """
    with session_scope() as session:
        claimed = (
            session.query(ImportJob)
            .filter(ImportJob.id == job_id, ImportJob.state.in_(("pending", "paused")))
            .update(
                {"state": "running", "error": None, "updated_at": utcnow()},
                synchronize_session=False,
            )
        )
        return claimed == 1


def _job_state(job_id: int) -> str:
    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        return job.state if job is not None else "missing"


async def run_import(job_id: int, *, worker: DeliveryWorker | None = None) -> int:
    """Execute an :class:`ImportJob` to completion; returns events applied.

    Resumable at two granularities. ``planned_through_seq`` skips whole plan
    pages already consumed — but a single page can cover the entire archive,
    so on its own that is no resume at all (a real popfeed run died at 1288 of
    1525 segments with ``planned_through_seq`` still 0). ``last_segment``
    therefore records the last segment applied, and the re-planned page is
    fast-forwarded past it.
    """
    if not _claim_job(job_id):
        with session_scope() as session:
            state = session.get(ImportJob, job_id)
            if state is None:
                raise ArchiveError(f"import job {job_id} not found")
            current = state.state
        # Another runner owns it (the CLI's --run and the server's watcher can
        # both reach for the same row), or it is already finished/cancelled.
        log.warning("archive import %s is %s and not claimable here; skipping", job_id, current)
        return 0

    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        assert job is not None  # _claim_job just updated it
        after_seq = job.planned_through_seq or job.after_seq
        before_seq = job.before_seq
        deliver = job.deliver
        applied_total = job.events_applied
        resume_after = job.last_segment

    target_worker = worker if deliver else None

    try:
        async with _client() as client:
            cursor = after_seq
            while True:
                plan = await _plan_page(client, after_seq=cursor, before_seq=before_seq)
                sealed_tip = plan.get("sealedTipSeq", 0)
                segments = plan.get("segments", [])

                # Fast-forward past what a previous attempt already applied.
                # Matched by name rather than position: segments are rewritten
                # by compaction, so an index from the old plan could point at
                # different data. An unrecognised name means the plan changed
                # under us, and re-applying is harmless (the staleness guard
                # drops what is already stored) where skipping would not be.
                skipped = 0
                if resume_after:
                    names = [s["name"] for s in segments]
                    if resume_after in names:
                        skipped = names.index(resume_after) + 1
                        segments = segments[skipped:]
                        log.info(
                            "archive import %s resuming after %s (%d segment(s) skipped)",
                            job_id,
                            resume_after,
                            skipped,
                        )
                    else:
                        log.warning(
                            "archive import %s: last segment %s not in the re-planned page; "
                            "restarting this page",
                            job_id,
                            resume_after,
                        )
                    resume_after = None

                _update_job(
                    job_id,
                    sealed_tip_seq=sealed_tip,
                    segments_total=len(segments) + skipped,
                    segments_done=skipped,
                )

                for done, segment in enumerate(segments, start=skipped + 1):
                    if _job_state(job_id) == "cancelled":
                        return applied_total
                    applied_total += await _apply_segment(
                        client, segment, job_id, worker=target_worker
                    )
                    _update_job(
                        job_id,
                        segments_done=done,
                        events_applied=applied_total,
                        last_segment=segment["name"],
                    )

                planned = plan.get("plannedThroughSeq", 0)
                # The page is fully applied, so the segment marker has served
                # its purpose; leaving it set would fast-forward the *next*
                # page against a name it cannot contain.
                _update_job(job_id, planned_through_seq=planned, last_segment=None)
                target = before_seq or sealed_tip
                if planned >= target or planned <= cursor:
                    break
                cursor = planned

        _update_job(job_id, state="done", events_applied=applied_total)
        log.info("archive import %s finished: %d event(s) applied", job_id, applied_total)
        return applied_total
    except asyncio.CancelledError:
        # Do not clobber an explicit stop: cancel() writes "cancelled" and
        # *then* cancels the task, so seeing that state here means an operator
        # asked to stop. Overwriting it with "paused" would make watch_jobs
        # restart the import within one poll.
        if _job_state(job_id) == "cancelled":
            _update_job(job_id, events_applied=applied_total)
        else:
            _update_job(job_id, state="paused", events_applied=applied_total)
        raise
    except httpx.TransportError as exc:
        # The network went away for longer than the per-request retries cover.
        # Nothing is wrong with the job itself, so leave it resumable:
        # watch_jobs picks it back up and the plan loop restarts from the last
        # completed page rather than the beginning.
        log.warning("archive import %s interrupted by %s; will resume", job_id, type(exc).__name__)
        _update_job(
            job_id,
            state="paused",
            events_applied=applied_total,
            error=f"{type(exc).__name__}: {exc}"[:500],
        )
        raise
    except Exception as exc:
        log.exception("archive import %s failed", job_id)
        _update_job(job_id, state="failed", error=f"{type(exc).__name__}: {exc}"[:500])
        raise


async def _range(client: httpx.AsyncClient, name: str, start: int, end: int | None) -> bytes:
    """Fetch a byte range of a segment file."""
    span = f"bytes={start}-{'' if end is None else end}"
    response = await _send(
        lambda: client.get(_GET_SEGMENT, params={"name": name}, headers={"Range": span}),
        what=f"getSegment {name} {span}",
    )
    return response.content


async def _blocks_from_index(client: httpx.AsyncClient, name: str) -> tuple[list[int] | None, int]:
    """Blocks of *name* holding a wanted collection, and the bytes this cost.

    Returns ``(None, bytes)`` if the index can't be read, so the caller falls
    back to downloading the whole segment rather than silently importing
    nothing.
    """
    try:
        header = await _range(client, name, 0, jss.HEADER_SIZE - 1)
        offset = jss.collection_index_offset(header)
        region = await _range(client, name, offset, None)
        wanted = set(get_settings().wanted_collections)
        blocks = await asyncio.to_thread(jss.blocks_for_collections, region, wanted)
        cost = len(header) + len(region)
        log.info("segment %s: %d block(s) match, index cost %d B", name, len(blocks), cost)
        return blocks, cost
    except Exception as exc:
        log.warning(
            "segment %s: collection index unreadable (%s); downloading in full",
            name,
            type(exc).__name__,
        )
        return None, 0


async def _apply_segment(
    client: httpx.AsyncClient,
    segment: dict[str, Any],
    job_id: int,
    *,
    worker: DeliveryWorker | None,
) -> int:
    """Download and apply one planned segment.

    A ``mode: "segment"`` entry nominally means "fetch the whole file", but
    doing so is ruinous for a rare collection: one measured segment cost
    242 MiB to yield 11 BookHive records. The segment's own collection index
    names the blocks holding each NSID, so we read that first (a few KB over
    two Range requests) and fetch only the blocks that matter — the same 11
    records for 3 MiB. Whole-file download stays as the fallback for a
    segment whose index can't be read.
    """
    name = segment["name"]
    applied = 0
    downloaded = 0

    if segment.get("mode") == "segment":
        indices, header_bytes = await _blocks_from_index(client, name)
        downloaded += header_bytes
        if indices is None:
            data = await _fetch_segment(client, name)
            downloaded += len(data)
            # zstd + CBOR decode is CPU-bound: keep it off the event loop so
            # live ingest keeps reading while a 250 MB segment is unpacked.
            events = await asyncio.to_thread(lambda: list(jss.iter_segment(data)))
            applied += await _apply(events, worker=worker)
            indices = []
    else:
        indices = [
            index
            for span in segment.get("blocks") or []
            for index in range(span["first"], span["last"] + 1)
        ]

    # Shared by both branches: a segment-mode entry resolves to a block list
    # via its collection index, and a blocks-mode entry gets one from the plan.
    # (This loop lived inside the `else` above, which meant every segment-mode
    # entry resolved its blocks and then silently fetched none of them.)
    #
    # Fetching one block at a time makes the whole import a chain of round
    # trips — ~14k of them for BookHive's history — which dominates wall-clock
    # while leaving the link idle. Fetch a window concurrently, but *apply
    # strictly in index order*: the archive's per-DID sequence ordering is a
    # guarantee the pipeline relies on.
    for start in range(0, len(indices), _BLOCK_PREFETCH):
        if _job_state(job_id) == "cancelled":
            break
        window = indices[start : start + _BLOCK_PREFETCH]
        tasks = [asyncio.create_task(_fetch_block(client, name, i)) for i in window]
        try:
            frames = await asyncio.gather(*tasks)
        except BaseException:
            # gather re-raises the first failure but leaves the siblings
            # running; their responses would still be metered and their
            # exceptions never retrieved.
            for task in tasks:
                task.cancel()
            raise
        for frame in frames:
            downloaded += len(frame)
            events = await asyncio.to_thread(jss.decode_block, frame)
            applied += await _apply(events, worker=worker)

    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        if job is not None:
            job.bytes_downloaded += downloaded
            job.updated_at = utcnow()
    return applied


# --------------------------------------------------------------------------- #
# In-process job registry
#
# The import runs inside the server process, next to live ingest, for the same
# reason per-DID backfills do: only an in-process task can be cancelled by
# optout.opt_out before it re-publishes records behind their own Delete.
# --------------------------------------------------------------------------- #
_TASK: asyncio.Task | None = None


def create_job(*, after_seq: int = 0, before_seq: int | None = None, deliver: bool = False) -> int:
    """Record a requested import and return its id.

    ``before_seq`` defaults to the live cursor, so the import covers exactly
    the history the live tail has not already seen.
    """
    if before_seq is None:
        before_seq = _live_cursor()
    with session_scope() as session:
        job = ImportJob(
            state="pending",
            after_seq=after_seq,
            before_seq=before_seq,
            planned_through_seq=after_seq,
            deliver=deliver,
        )
        session.add(job)
        session.flush()
        return job.id


def start(job_id: int, *, worker: DeliveryWorker | None = None) -> bool:
    """Run ``job_id`` in the background. ``False`` if one is already running."""
    global _TASK
    if _TASK is not None and not _TASK.done():
        return False

    async def _run() -> None:
        try:
            await run_import(job_id, worker=worker)
        except asyncio.CancelledError:
            log.info("archive import %s cancelled", job_id)
            raise
        except Exception:
            log.exception("archive import %s failed", job_id)

    _TASK = asyncio.create_task(_run(), name=f"archive-import-{job_id}")
    _TASK.add_done_callback(_clear_task)
    return True


def _clear_task(task: asyncio.Task) -> None:
    """Drop the registration, unless a newer run already replaced it.

    The callback is delivered via call_soon, so another start() can install a
    fresh task first; clearing unconditionally would report is_running() False
    while that one runs and let a second import start alongside it.
    """
    global _TASK
    if _TASK is task:
        _TASK = None


def is_running() -> bool:
    return _TASK is not None and not _TASK.done()


async def cancel(*, resume: bool = False) -> bool:
    """Cancel the running import and wait for it to stop.

    ``resume=True`` leaves the job ``paused`` instead of ``cancelled``, so
    :func:`watch_jobs` picks it up again from its persisted progress. Used
    when an opt-out must interrupt an import rather than abandon it.
    """
    global _TASK
    task = _TASK
    if task is None or task.done():
        return False
    if not resume:
        # run_import polls for this to stop between blocks; it also stops the
        # task being restarted by watch_jobs.
        with session_scope() as session:
            for job in session.query(ImportJob).filter(ImportJob.state == "running"):
                job.state = "cancelled"
                job.updated_at = utcnow()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    return True


async def cancel_if_delivering() -> bool:
    """Cancel a running import only if it federates what it imports.

    Called by ``optout.opt_out``: a delivering import could otherwise enqueue
    a ``Create`` behind the purge's ``Delete`` and leave opted-out content
    live on peers. A non-delivering import writes nothing to peers and its
    per-event opt-out check already stops it, so it is left running.
    """
    if not is_running():
        return False
    with session_scope() as session:
        # .first(), not .one_or_none(): a hard kill can strand an older row in
        # "running", and this runs inside opt_out — it must never raise there.
        job = (
            session.query(ImportJob)
            .filter(ImportJob.state == "running")
            .order_by(ImportJob.id.desc())
            .first()
        )
        delivering = job is not None and job.deliver
    if not delivering:
        return False
    log.info("cancelling delivering archive import for an opt-out; it will resume")
    return await cancel(resume=True)


def current_job() -> ImportJob | None:
    """The most recent job, for the admin view."""
    with session_scope() as session:
        return session.query(ImportJob).order_by(ImportJob.id.desc()).first()


def _claimable_job() -> int | None:
    """The oldest job waiting to run.

    ``paused`` is included so a job interrupted by a restart (or a shutdown
    mid-download) resumes on the next boot from its persisted
    ``planned_through_seq`` rather than needing to be re-requested.
    """
    with session_scope() as session:
        job = (
            session.query(ImportJob)
            .filter(ImportJob.state.in_(("pending", "paused")))
            .order_by(ImportJob.id.asc())
            .first()
        )
        return job.id if job is not None else None


def reclaim_orphaned_jobs(*, stale_after: timedelta = _HEARTBEAT_STALE_AFTER) -> int:
    """Return jobs stranded in ``running`` to a resumable state.

    A graceful shutdown leaves ``paused`` via the cancellation path, but a
    SIGKILL or OOM leaves ``running`` — a state nothing reclaims, which would
    strand exactly the interrupted job the resume machinery exists for.

    Staleness is judged by the row's heartbeat rather than assuming this is
    the only process: ``python -m skybridge import --run`` can legitimately be
    working a job from another process, and stealing it would mean two runners
    downloading the same metered blocks.
    """
    cutoff = utcnow() - stale_after
    reclaimed = 0
    with session_scope() as session:
        for job in session.query(ImportJob).filter(ImportJob.state == "running").all():
            beat = job.updated_at
            if beat is not None:
                # SQLite may hand back a naive datetime for a tz-aware column.
                if beat.tzinfo is None:
                    beat = beat.replace(tzinfo=UTC)
                if beat > cutoff:
                    continue  # still heartbeating; someone owns it
            job.state = "paused"
            job.updated_at = utcnow()
            reclaimed += 1
    return reclaimed


async def watch_jobs(*, worker: DeliveryWorker | None = None, interval: float = 15.0) -> None:
    """Run queued imports on the server process.

    ``python -m skybridge import`` only records the request; the work happens
    here so it shares the process with live ingest and stays cancellable by
    ``optout.opt_out``. Also resumes an unfinished job after a restart.
    """
    if reclaimed := reclaim_orphaned_jobs():
        log.info("reclaimed %d archive import(s) left running by a hard stop", reclaimed)
    while True:
        try:
            if not is_running() and (job_id := _claimable_job()) is not None:
                log.info("picking up archive import %s", job_id)
                start(job_id, worker=worker)
        except Exception:
            log.exception("archive job watcher failed")
        await asyncio.sleep(interval)
