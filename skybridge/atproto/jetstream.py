"""Live Jetstream WebSocket client: stream popfeed commits into the pipeline.

Connects to a Jetstream endpoint requesting only the popfeed collections,
persists a cursor for resume, and reconnects with exponential backoff. Each
event is handed to :func:`process_event`.

A stored cursor can fall behind the host's lookback window while the bridge is
down. The host then refuses the upgrade, and rebuilding the URL from the same
cursor would retry that refusal forever, so the cursor itself has to move: see
``_CURSOR_TOO_OLD`` for the recovery and the gap it reports.

Speaks both dialects (see :mod:`skybridge.atproto.events`): v2 hosts take
``collections``/``kinds`` and order by ``seq``; v1 hosts take
``wantedCollections`` and order by ``time_us``. The dialect follows from the
configured URL.

Why v1 is still supported: **rollback**, not migration. Upgrading needs no v1
code at all — a v2 host accepts a ``time_us`` cursor and resolves it by
magnitude, so :func:`load_cursor` alone carries a v1 deployment across (see
its docstring). What the v1 transport buys is the ability to point
``SKYBRIDGE_JETSTREAM`` back at a v1 host if v2 misbehaves in production. That
is a post-cutover safety net, not a permanent feature.

Retiring v1 touches: the ``jetstream_is_v2`` guards (9 in this package plus one
in ``web/manage.html``), :func:`skybridge.atproto.events._from_v1`, the dual
cursor read/write here, and two things easy to miss —
``backfill._commit_event`` still *produces* the v1 shape for the user-facing
"Import recent activity" path, and ``fixtures/jetstream_sample.jsonl`` is
v1-shaped and drives most of the test suite via ``conftest.fixture_path``.
Keep ``micros_to_iso`` regardless: the ``.jss`` archive stores epoch
microseconds and needs it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import UTC, datetime
from random import uniform
from time import monotonic
from typing import Any
from urllib.parse import urlencode

import websockets
from websockets.exceptions import InvalidStatus
from websockets.typing import Subprotocol

from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Cursor
from skybridge.pipeline import process_event

log = logging.getLogger("skybridge.jetstream")

_MAX_BACKOFF = 60

# How long (seconds) a connection has to stay up before it counts as healthy
# and the backoff resets. Resetting on the handshake instead would defeat the
# backoff against a host that accepts the upgrade and then drops the stream
# straight away ("no close frame received or sent"): every cycle would reset to
# one second, so a struggling host would be hammered once a second forever.
_HEALTHY_AFTER = 30

# v2 frames are JSON under this XRPC subprotocol; the server rejects the
# upgrade without it.
_V2_SUBPROTOCOL = Subprotocol("xrpc.v1.json")

# A v2 `seq` counts events (~1e10); a v1 `time_us` counts microseconds since
# the epoch (~1.7e15). v2 disambiguates a cursor by magnitude, which is what
# lets a v1 deployment's stored time_us resume against a v2 host. Anything at
# or above this bound is a timestamp, not a sequence number.
_TIME_US_FLOOR = 1_000_000_000_000_000

# Events between cursor writes. The cursor only bounds how far a reconnect
# rewinds, and re-processing is idempotent (pipeline._is_stale drops anything
# at or below a record's high-water mark), so persisting every frame buys
# nothing and costs a commit per event — including the identity/account
# traffic v2 delivers for the whole network.
_CURSOR_FLUSH_EVERY = 100

# Error code a v2 host returns, with HTTP 400 and no WebSocket upgrade, when
# the cursor predates its lookback window (36 hours on the Bluesky-hosted
# instances). Nothing about this is transient: the URL is rebuilt from the
# stored cursor on every attempt, so an unhandled refusal loops at the backoff
# cap until an operator moves the cursor by hand.
_CURSOR_TOO_OLD = "CursorTooOld"

# The floor rides in the human-readable message; the host publishes it nowhere
# else (there is no describe endpoint, and the archive calls need an API key).
# Reading it is what keeps the whole lookback window on the free live socket
# instead of the metered archive, and a reworded message costs nothing: an
# unparsed floor falls back to the live tip.
_FLOOR_IN_MESSAGE = re.compile(r"lookback floor (\d+)")

# Consecutive refusals of a cursor before giving up on resuming near the floor.
# The floor advances a sealed segment at a time, so the first retry lands
# inside the window in the normal case; a second refusal means it moved in
# between, and only the live tip is certain not to be too old.
_MAX_CURSOR_RESETS = 2

# When the host last delivered an event. Read by the liveness line on /stats
# and the admin panel: the HTTP healthcheck says nothing about the ingest
# loop, which can sit in a reconnect cycle while the web app answers normally.
# In-process only — `serve` runs the loop beside the web app (see
# ``main.lifespan``), and a standalone `ingest` process has no page to report.
_last_event_at: datetime | None = None


def last_event_at() -> datetime | None:
    """When an event last arrived, or ``None`` if none has since startup."""
    return _last_event_at


def _build_url(cursor: int | None = None) -> str:
    """The subscribe URL. ``cursor`` overrides the stored one; 0 omits it."""
    settings = get_settings()
    if cursor is None:
        cursor = load_cursor()
    if settings.jetstream_is_v2:
        params: list[tuple[str, str]] = [("collections", c) for c in settings.wanted_collections]
        params += [("kinds", k) for k in settings.wanted_kinds]
    else:
        params = [("wantedCollections", c) for c in settings.wanted_collections]
    if cursor:
        params.append(("cursor", str(cursor)))
    return f"{settings.jetstream_url}?{urlencode(params)}"


def load_cursor() -> int:
    """The resume point for the configured host.

    A v2 host is given the stored ``seq`` when we have one, else the legacy
    ``time_us`` — which it resolves by magnitude, so upgrading a running
    deployment resumes where it left off instead of restarting from the tip.
    A v1 host only understands ``time_us``, and a ``seq`` means nothing to it,
    so it gets 0 (start from now) rather than a timestamp of 1970.
    """
    with session_scope() as session:
        row = session.get(Cursor, 1)
        if row is None:
            return 0
        if get_settings().jetstream_is_v2:
            return row.seq or row.time_us or 0
        return row.time_us or 0


def save_cursor(value: int) -> None:
    """Record the resume point, in whichever unit this host reports."""
    is_seq = value < _TIME_US_FLOOR
    with session_scope() as session:
        row = session.get(Cursor, 1)
        if row is None:
            row = Cursor(id=1)
            session.add(row)
        if is_seq:
            row.seq = value
        else:
            row.time_us = value


def _describe(body: Any) -> str:
    """A one-line handle on an event for the log: enough to find the record
    (did, collection, rkey, seq), never the record itself."""
    if not isinstance(body, dict):
        return repr(body)[:120]
    return " ".join(
        f"{key}={body.get(key)}" for key in ("seq", "did", "collection", "rkey") if key in body
    )


def _cursor_of(event: dict) -> int | None:
    """The resume value carried by an event, in this host's unit."""
    for key in ("seq", "time_us"):
        if (value := event.get(key)) is not None:
            return int(value)
    return None


def _stale_cursor_floor(exc: InvalidStatus) -> int | None:
    """The lookback floor named by a "cursor too old" refusal.

    ``None`` when the host refused the upgrade for any other reason, so the
    caller leaves the cursor alone. ``0`` when it refused the cursor but named
    no floor, which leaves the live tip as the only certain resume point.
    """
    try:
        body = json.loads(bytes(exc.response.body or b""))
    except ValueError:
        return None
    if not isinstance(body, dict) or body.get("error") != _CURSOR_TOO_OLD:
        return None
    match = _FLOOR_IN_MESSAGE.search(str(body.get("message", "")))
    return int(match.group(1)) if match else 0


def _report_gap(after_seq: int, before_seq: int) -> bool:
    """Log what a cursor reset skipped, and queue the import that recovers it.

    Both ends are seqs the live tail actually held, so the archive can resolve
    the range. Delivery stays off: the gap is history by the time it imports,
    and fanning it out would push a burst of Creates at every peer.

    ``False`` means the caller must keep the gap and try again: the request to
    record it failed, and the cursor is about to move past the range for good.
    Nothing but the log would remember it. A gap this deployment can never
    import (a v1 host, no key, no real range) returns ``True`` — trying again
    on the next event would only repeat the same refusal forever.
    """
    if not 0 < after_seq < before_seq:
        # Nothing was missed, or the old cursor was a legacy `time_us`
        # (~1.7e15, above every seq) which the seq-keyed archive cannot use.
        log.error("ingest resumed at seq %d after a cursor reset from %d", before_seq, after_seq)
        return True
    settings = get_settings()
    if not settings.jetstream_is_v2 or not settings.jetstream_api_key:
        log.error(
            "ingest gap: seq %d..%d was skipped and cannot be imported here (%s)",
            after_seq,
            before_seq,
            "v1 endpoint" if not settings.jetstream_is_v2 else "no SKYBRIDGE_JETSTREAM_API_KEY",
        )
        return True
    from skybridge.atproto import archive

    try:
        job_id = archive.create_job(after_seq=after_seq, before_seq=before_seq)
    except Exception:
        log.exception(
            "ingest gap: seq %d..%d was skipped; queueing the import failed. Recover it with "
            "`python -m skybridge import --after-seq %d --before-seq %d`",
            after_seq,
            before_seq,
            after_seq,
            before_seq,
        )
        return False
    log.error(
        "ingest gap: seq %d..%d was skipped; queued archive import %d to recover it",
        after_seq,
        before_seq,
        job_id,
    )
    return True


async def run(worker: DeliveryWorker, *, stop_after: int | None = None) -> int:
    """Consume the firehose until cancelled. Returns count of events processed.

    ``stop_after`` (events) bounds the loop for smoke tests.
    """
    global _last_event_at
    settings = get_settings()
    subprotocols = [_V2_SUBPROTOCOL] if settings.jetstream_is_v2 else None
    processed = 0
    backoff = 1
    pending_cursor: int | None = None
    since_flush = 0
    # Live only while a refused cursor is being recovered: the override for
    # the next URL (0 = ask for the live tip), how many refusals came in a row
    # without a connection in between, and where the gap starts — its far end
    # is only known once events flow again.
    resume_from: int | None = None
    cursor_resets = 0
    gap_after: int | None = None
    gap_before: int | None = None
    while True:
        opened: float | None = None
        try:
            url = _build_url(resume_from)
            log.info("connecting to jetstream: %s", url)
            async with websockets.connect(url, max_size=None, subprotocols=subprotocols) as ws:
                opened = monotonic()
                cursor_resets = 0
                async for raw in ws:
                    _last_event_at = datetime.now(UTC)
                    event = json.loads(raw)
                    # v2 nests everything under `payload`; the cursor lives on
                    # the payload, so read it post-unwrap but pre-normalise
                    # (normalise drops kinds we don't bridge, and the cursor
                    # must still advance across those).
                    body = event.get("payload") if event.get("$type") == "message" else event
                    if isinstance(body, dict) and (value := _cursor_of(body)) is not None:
                        resumed = gap_after is not None
                        if resumed:
                            # Where ingestion came back, pinned on the first
                            # event so a retry below reports the same range
                            # rather than a wider one.
                            gap_before = gap_before or value
                            if _report_gap(gap_after, gap_before):
                                gap_after, gap_before = None, None
                            resume_from = None
                        pending_cursor = value
                        since_flush += 1
                        # The first event after a reset is flushed at once:
                        # until the new position is stored, a reconnect would
                        # rebuild the URL from the refused cursor and be
                        # refused again.
                        if since_flush >= _CURSOR_FLUSH_EVERY or resumed:
                            save_cursor(pending_cursor)
                            pending_cursor, since_flush = None, 0
                    try:
                        result = await process_event(event, worker=worker)
                    except Exception:
                        # One record must never take the stream down. Anyone
                        # can write a lexicon-invalid record into a wanted
                        # collection, and Jetstream does not validate shapes;
                        # letting the error escape would drop the socket and,
                        # since the cursor names this very event, replay it
                        # on reconnect — for good. Log it, keep the cursor
                        # moving, and read on.
                        log.exception(
                            "skipping event that the pipeline could not process: %s",
                            _describe(body),
                        )
                        result = None
                    if result is not None:
                        processed += 1
                        log.info("bridged %s (%s)", result.at_uri, result.operation)
                    if stop_after is not None and processed >= stop_after:
                        if pending_cursor is not None:
                            save_cursor(pending_cursor)
                        return processed
        except Exception as exc:  # reconnect on any transport/parse error
            if pending_cursor is not None:
                # Persist progress before backing off; the next connect should
                # resume from what we actually processed, not the last flush.
                save_cursor(pending_cursor)
                pending_cursor, since_flush = None, 0
            if isinstance(exc, InvalidStatus):
                # The body carries the reason and the library never shows it.
                log.warning(
                    "jetstream refused the upgrade: HTTP %d %s",
                    exc.response.status_code,
                    bytes(exc.response.body or b"").decode("utf-8", "replace").strip(),
                )
                floor = _stale_cursor_floor(exc)
                if floor is not None and cursor_resets < _MAX_CURSOR_RESETS:
                    cursor_resets += 1
                    stale = load_cursor()
                    if gap_after is None:
                        gap_after = stale
                    # Where this attempt will land is not known yet, so the
                    # far end has to be measured again once events flow.
                    gap_before = None
                    # Resume at the floor: the lookback window still holds
                    # those events and the socket serves them for free, where
                    # the archive is metered by the byte. A second refusal means
                    # the floor moved between the refusal and the retry, and the
                    # host may have named no floor at all — the live tip is then
                    # the only resume point that can never be too old.
                    resume_from = floor if floor and cursor_resets == 1 else 0
                    # Both resume points stay in memory until an event lands.
                    # Storing one would strand the gap: a restart in between
                    # would resume from it, having forgotten where ingestion
                    # really stopped, and nothing would report the loss. Left
                    # alone, the same refusal simply repeats the recovery. (A
                    # stored 0 would be worse still, falling through to the
                    # legacy time_us, which is just as stale.)
                    log.error(
                        "jetstream refused cursor %d as too old; resuming from %s",
                        stale,
                        "the live tip" if resume_from == 0 else floor,
                    )
                    continue  # the cause is gone, so do not back off
            # A handshake that fails outright never opened, so it counts as
            # zero uptime and keeps the backoff growing.
            uptime = 0.0 if opened is None else monotonic() - opened
            if uptime >= _HEALTHY_AFTER:
                backoff = 1
            # Jitter, so instances that restart together do not resynchronise
            # on the host at every retry.
            delay = uniform(backoff / 2, backoff)
            log.warning(
                "jetstream connection lost after %.1fs: %s; reconnecting in %.1fs",
                uptime,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, _MAX_BACKOFF)
