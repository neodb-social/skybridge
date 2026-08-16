"""Live Jetstream WebSocket client: stream popfeed commits into the pipeline.

Connects to a Jetstream endpoint requesting only the popfeed collections,
persists a cursor for resume, and reconnects with exponential backoff. Each
event is handed to :func:`process_event`.

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
in ``web/optout.html``), :func:`skybridge.atproto.events._from_v1`, the dual
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
from random import uniform
from time import monotonic
from urllib.parse import urlencode

import websockets
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


def _build_url() -> str:
    settings = get_settings()
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


def _cursor_of(event: dict) -> int | None:
    """The resume value carried by an event, in this host's unit."""
    for key in ("seq", "time_us"):
        if (value := event.get(key)) is not None:
            return int(value)
    return None


async def run(worker: DeliveryWorker, *, stop_after: int | None = None) -> int:
    """Consume the firehose until cancelled. Returns count of events processed.

    ``stop_after`` (events) bounds the loop for smoke tests.
    """
    settings = get_settings()
    subprotocols = [_V2_SUBPROTOCOL] if settings.jetstream_is_v2 else None
    processed = 0
    backoff = 1
    pending_cursor: int | None = None
    since_flush = 0
    while True:
        opened: float | None = None
        try:
            url = _build_url()
            log.info("connecting to jetstream: %s", url)
            async with websockets.connect(url, max_size=None, subprotocols=subprotocols) as ws:
                opened = monotonic()
                async for raw in ws:
                    event = json.loads(raw)
                    # v2 nests everything under `payload`; the cursor lives on
                    # the payload, so read it post-unwrap but pre-normalise
                    # (normalise drops kinds we don't bridge, and the cursor
                    # must still advance across those).
                    body = event.get("payload") if event.get("$type") == "message" else event
                    if isinstance(body, dict) and (value := _cursor_of(body)) is not None:
                        pending_cursor = value
                        since_flush += 1
                        if since_flush >= _CURSOR_FLUSH_EVERY:
                            save_cursor(pending_cursor)
                            pending_cursor, since_flush = None, 0
                    result = await process_event(event, worker=worker)
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
