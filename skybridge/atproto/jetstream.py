"""Live Jetstream WebSocket client: stream popfeed commits into the pipeline.

Connects to a public Jetstream endpoint requesting only the popfeed
collections, persists a ``time_us`` cursor for resume, and reconnects with
exponential backoff. Each event is handed to :func:`process_event`.
"""

from __future__ import annotations

import json
import logging
from urllib.parse import urlencode

import websockets

from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Cursor
from skybridge.pipeline import process_event

log = logging.getLogger("skybridge.jetstream")

_MAX_BACKOFF = 60


def _build_url() -> str:
    settings = get_settings()
    params = [("wantedCollections", c) for c in settings.wanted_collections]
    cursor = load_cursor()
    if cursor:
        params.append(("cursor", str(cursor)))
    return f"{settings.jetstream_url}?{urlencode(params)}"


def load_cursor() -> int:
    with session_scope() as session:
        row = session.get(Cursor, 1)
        return row.time_us if row else 0


def save_cursor(time_us: int) -> None:
    with session_scope() as session:
        row = session.get(Cursor, 1)
        if row is None:
            row = Cursor(id=1, time_us=time_us)
            session.add(row)
        else:
            row.time_us = time_us


async def run(worker: DeliveryWorker, *, stop_after: int | None = None) -> int:
    """Consume the firehose until cancelled. Returns count of events processed.

    ``stop_after`` (events) bounds the loop for smoke tests.
    """
    processed = 0
    backoff = 1
    while True:
        try:
            url = _build_url()
            log.info("connecting to jetstream: %s", url)
            async with websockets.connect(url, max_size=None) as ws:
                backoff = 1
                async for raw in ws:
                    event = json.loads(raw)
                    if (time_us := event.get("time_us")) is not None:
                        save_cursor(int(time_us))
                    result = await process_event(event, worker=worker)
                    if result is not None:
                        processed += 1
                        log.info("bridged %s (%s)", result.at_uri, result.operation)
                    if stop_after is not None and processed >= stop_after:
                        return processed
        except Exception as exc:  # reconnect on any transport/parse error
            log.warning("jetstream connection error: %s; reconnecting in %ss", exc, backoff)
            import asyncio

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
