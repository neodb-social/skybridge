"""Backfill: seed the bridge from a DID's existing popfeed records.

Uses ``com.atproto.repo.listRecords`` against the author's PDS and feeds each
record through the pipeline as a synthetic ``create`` commit. Two callers:
the ``backfill`` CLI subcommand (operator seeding) and :func:`start_import`,
behind the "Import recent activity" button on /optout (user-triggered,
windowed to the last ``backfill_days`` days, capped at ``backfill_limit``
records, one run per DID at a time).

The window and replay order are keyed on each record's *write time* (its TID
rkey, falling back to ``createdAt``/``addedAt`` for non-TID rkeys): an import
replays what live ingestion would have bridged in that period, in the same
order, so review/listItem pairs anchor their combined Note identically.
Archive-only collections (feed.list) are exempt from the window and replay
first: they never federate, and having them archived before their listItems
translate lets ``_list_label`` resolve locally instead of fetching over the
network mid-replay.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from skybridge import optout
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.pipeline import ARCHIVE_ONLY_COLLECTIONS, Processed, process_event
from skybridge.translate import bookhive

log = logging.getLogger("skybridge.backfill")

_PROFILE_COLLECTION = "social.popfeed.actor.profile"
_PAGE_SIZE = 100  # listRecords hard cap per request

# Fetch priority under the shared cap: collections that emit AP activities
# first, so archive-only records (feed.list) can never starve them of budget.
# Anything wanted-but-unlisted keeps its settings order, after these.
_FETCH_PRIORITY = (
    "social.popfeed.feed.review",
    "social.popfeed.feed.listItem",
    bookhive.BOOK_COLLECTION,
)

# TID encoding: 13 chars of base32-sortable, 1 zero bit + 53-bit microseconds
# since the UNIX epoch + 10-bit clock id (https://atproto.com/specs/tid).
_TID_CHARS = "234567abcdefghijklmnopqrstuvwxyz"

# How often the replay loop re-checks the opt-out flag: process_event guards
# every record anyway, so this only bounds the wasted no-op tail.
_OPTOUT_CHECK_EVERY = 25


def _content_collections() -> tuple[str, ...]:
    """Collections worth importing, in fetch-priority order: everything we
    bridge except profile edits (identity metadata — refreshed separately at
    the end of :func:`backfill_did`)."""
    wanted = [c for c in get_settings().wanted_collections if c != _PROFILE_COLLECTION]
    prioritized = [c for c in _FETCH_PRIORITY if c in wanted]
    return tuple(prioritized + [c for c in wanted if c not in prioritized])


def _tid_datetime(rkey: str) -> datetime | None:
    """Decode a TID rkey into the record's write time (None for non-TID rkeys).

    The first char must encode a zero top bit (the spec restricts it to
    ``234567abcdefghij``); without that check, a crafted 13-char rkey of
    charset letters decodes to a far-future time and bypasses any age window.
    """
    if len(rkey) != 13 or rkey[0] not in _TID_CHARS[:16]:
        return None
    value = 0
    for char in rkey:
        idx = _TID_CHARS.find(char)
        if idx < 0:
            return None
        value = (value << 5) | idx
    try:
        return datetime.fromtimestamp((value >> 10) / 1_000_000, tz=UTC)
    except (OSError, OverflowError, ValueError):
        return None


def _created_at(record: dict[str, Any]) -> datetime | None:
    """The record's own creation time (``createdAt``/``addedAt``), or None.

    popfeed sometimes writes ``createdAt`` as an empty object — mirror
    ``translate.neodb._published`` and treat anything non-string as absent.
    """
    raw = record.get("createdAt") or record.get("addedAt")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _rkey(rec: dict[str, Any]) -> str:
    uri = rec.get("uri", "")
    return uri.rsplit("/", 1)[-1] if uri else ""


def _record_time(rec: dict[str, Any]) -> datetime | None:
    """When the record was written: TID rkey first (authoritative, matches
    firehose order), content timestamp as the fallback for non-TID rkeys."""
    return _tid_datetime(_rkey(rec)) or _created_at(rec.get("value") or {})


def _commit_event(
    did: str, collection: str, rkey: str, record: dict[str, Any], cid: str | None = None
) -> dict[str, Any]:
    """A Jetstream-shaped synthetic ``create`` commit for process_event."""
    return {
        "did": did,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": collection,
            "rkey": rkey,
            "record": record,
            "cid": cid,
        },
    }


def _list_records(
    pds: str, did: str, collection: str, limit: int, since: datetime | None
) -> list[dict[str, Any]]:
    """Newest-first records of one collection, cursor-paginated.

    Stops at ``limit`` records, the end of the collection, or — when
    ``since`` is set — once a page's oldest rkey TID predates it: listRecords
    pages in descending rkey order (verified against bsky.network PDSes), and
    TIDs are write times, so later pages can only be older. Content
    timestamps are deliberately NOT used here: they are user-authored and not
    monotone with pagination order.
    """
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    while len(out) < limit:
        url = (
            f"{pds}/xrpc/com.atproto.repo.listRecords"
            f"?repo={did}&collection={collection}&limit={min(_PAGE_SIZE, limit - len(out))}"
        )
        if cursor:
            url += f"&cursor={quote(cursor)}"
        data = identity._http_json(url) or {}
        records = data.get("records", [])
        if not records:
            break
        out.extend(records)
        cursor = data.get("cursor")
        if not cursor:
            break
        if since is not None:
            oldest = _tid_datetime(_rkey(records[-1]))
            if oldest is not None and oldest < since:
                break
    return out


def _fetch_records(
    pds: str, did: str, *, limit: int, since: datetime | None
) -> list[tuple[datetime | None, str, dict[str, Any]]]:
    """(write time, collection, listRecords entry) triples, window-filtered,
    at most ``limit`` in total, budgeted in :data:`_FETCH_PRIORITY` order.

    Only records that survive the window count against the budget, so a
    collection whose history straddles the window cannot starve later
    collections (each collection's raw fetch is still capped at the
    remaining budget, and the TID early-stop bounds the overshoot to about
    one page). Archive-only collections are exempt from the window — they
    never federate, and archiving them is what keeps listItem translation
    off the network during replay. Blocking urllib I/O — call via
    ``asyncio.to_thread``.
    """
    fetched: list[tuple[datetime | None, str, dict[str, Any]]] = []
    for collection in _content_collections():
        room = limit - len(fetched)
        if room <= 0:
            break
        window = None if collection in ARCHIVE_ONLY_COLLECTIONS else since
        unknown_age = 0
        for rec in _list_records(pds, did, collection, room, window):
            when = _record_time(rec)
            if window is not None and (when is None or when < window):
                if when is None:
                    unknown_age += 1
                continue
            fetched.append((when, collection, rec))
        if unknown_age:
            log.info("%s: skipped %d record(s) of unknown age for %s", collection, unknown_age, did)
    return fetched


async def backfill_did(
    did: str,
    *,
    worker: DeliveryWorker | None = None,
    limit: int | None = None,
    since: datetime | None = None,
) -> list[Processed]:
    """Pull a DID's popfeed records and run them through the pipeline.

    Fetches at most ``limit`` records total (``None`` = the configured
    ``backfill_limit``), newest first, reviews and shelf items budgeted
    before archive-only lists. With ``since`` set, only records written
    at/after it are replayed (write time = TID rkey, falling back to
    ``createdAt``/``addedAt``; unknown-age records are skipped); without it
    everything fetched replays. Lists replay first (see module docstring),
    then everything else oldest-first by write time — the order live
    ingestion would have used — and the run finishes by refreshing the
    actor's identity from their popfeed profile record, which
    ``ensure_actor`` alone never does for existing actors.
    """
    if optout.is_opted_out(did):
        log.info("skipping import for %s: opted out", did)
        return []
    if limit is None:
        limit = get_settings().backfill_limit
    pds = await asyncio.to_thread(identity.resolve_pds, did)
    if pds is None:
        log.warning("could not resolve PDS for %s", did)
        return []
    fetched = await asyncio.to_thread(_fetch_records, pds, did, limit=limit, since=since)
    if not fetched:
        # Nothing to replay: don't mint an actor row (or fetch profiles) for
        # a repo with no importable popfeed presence.
        log.info("nothing to import for %s", did)
        return []
    # Mint/refresh the bridged actor once up front (network resolution
    # included) instead of on the first replayed event.
    await asyncio.to_thread(identity.ensure_actor, did)

    epoch = datetime.fromtimestamp(0, tz=UTC)
    fetched.sort(key=lambda e: (e[1] not in ARCHIVE_ONLY_COLLECTIONS, e[0] or epoch))

    results: list[Processed] = []
    for i, (_, collection, rec) in enumerate(fetched):
        # An opt-out completed mid-import wins. opt_out cancels the import
        # task and process_event re-checks per record; this periodic check
        # just cuts the no-op tail short for direct (CLI) callers.
        if i % _OPTOUT_CHECK_EVERY == 0 and optout.is_opted_out(did):
            log.info("import aborted for %s: opted out", did)
            return results
        # Yield so a long replay burst cannot starve concurrent requests:
        # process_event's DB and translate work is synchronous on the loop.
        await asyncio.sleep(0)
        processed = await process_event(
            _commit_event(did, collection, _rkey(rec), rec.get("value", {}), rec.get("cid")),
            worker=worker,
        )
        if processed is not None:
            results.append(processed)

    # Refresh display name/avatar from the popfeed profile record and emit
    # Update(Person): ensure_actor resolves the network only for NEW actors,
    # so an existing actor's stale identity would otherwise never update.
    # The network round trips run off-loop; the process_event that emits the
    # Update stays offline (allow_network=False) so nothing blocks the loop.
    if not optout.is_opted_out(did):
        profile = await asyncio.to_thread(identity._profile_record, pds, did, _PROFILE_COLLECTION)
        if profile:
            await asyncio.to_thread(identity.refresh_actor, did, profile)
            await process_event(
                _commit_event(did, _PROFILE_COLLECTION, "self", profile),
                worker=worker,
                allow_network=False,
            )

    log.info("backfilled %d record(s) for %s", len(results), did)
    return results


# One import per DID at a time: key = DID with a run in flight, value = the
# task (a strong reference — bare create_task results may be GC'd). Only ever
# touched from the event loop thread, so no lock is needed.
_IMPORTS: dict[str, asyncio.Task] = {}


def start_import(did: str, *, worker: DeliveryWorker | None = None) -> bool:
    """Schedule a background windowed import for ``did``.

    Returns ``False`` without starting anything when an import for this DID
    is already running or the DID is opted out. Window and cap come from
    settings (``SKYBRIDGE_BACKFILL_DAYS`` / ``SKYBRIDGE_BACKFILL_LIMIT``).
    """
    if did in _IMPORTS or optout.is_opted_out(did):
        return False
    settings = get_settings()
    since = datetime.now(UTC) - timedelta(days=settings.backfill_days)

    async def _run() -> None:
        try:
            await backfill_did(did, worker=worker, limit=settings.backfill_limit, since=since)
        except asyncio.CancelledError:
            log.info("import cancelled for %s", did)
            raise
        except Exception:
            log.exception("import failed for %s", did)

    task = asyncio.create_task(_run(), name=f"import-{did}")
    _IMPORTS[did] = task
    # start_import refuses while the DID is still in _IMPORTS, so this pop
    # can only ever remove this very task.
    task.add_done_callback(lambda _t: _IMPORTS.pop(did, None))
    return True


async def cancel_import(did: str) -> bool:
    """Cancel ``did``'s running import (if any) and wait for it to stop.

    Called by ``optout.opt_out`` before purging, so a half-done replay can
    never re-publish records behind their own Delete activities.
    """
    task = _IMPORTS.get(did)
    if task is None:
        return False
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    current = asyncio.current_task()
    if current is not None and current.cancelling():
        # The suppress above cannot tell the import's cancellation from OUR
        # OWN, delivered while we waited (e.g. uvicorn cancelling an opt-out
        # request during shutdown) — propagate it instead of letting the
        # caller carry on.
        raise asyncio.CancelledError
    return True


async def cancel_all_imports() -> None:
    """Cancel every running import (server shutdown): imports must stop
    enqueueing before the delivery worker drains its queue."""
    for did in list(_IMPORTS):
        await cancel_import(did)
