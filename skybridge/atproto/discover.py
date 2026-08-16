"""Discover atproto collections published under the namespaces we bridge.

``WANTED_COLLECTIONS`` is a hand-maintained list, and the commentary beside it
records collections that were noticed by hand and judged not worth bridging.
Jetstream v2 accepts namespace wildcards, so that survey can be automated:
subscribe to ``social.popfeed.*``/``buzz.bookhive.*`` and record every NSID
that appears, with a sample record to judge the shape from.

Deliberately a separate subcommand rather than part of ingestion. A wildcard
subscription also carries collections the bridge has no use for — BookHive's
``catalogBook`` alone streams multi-KB author biographies — so the live path
keeps asking for exactly the collections it wants.
"""

from __future__ import annotations

import asyncio
import json
import logging
from urllib.parse import urlencode

import websockets
from websockets.typing import Subprotocol

from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import SeenCollection, utcnow

log = logging.getLogger("skybridge.discover")

_SUBPROTOCOL = Subprotocol("xrpc.v1.json")
_SAMPLE_CHARS = 4000


def _build_url() -> str:
    settings = get_settings()
    params = [("collections", pattern) for pattern in settings.discovery_collections]
    params.append(("kinds", "commit"))
    return f"{settings.jetstream_url}?{urlencode(params)}"


def record_collection(nsid: str, record: dict | None) -> None:
    """Upsert one observed NSID, keeping the first record seen as the sample."""
    with session_scope() as session:
        row = session.get(SeenCollection, nsid)
        if row is None:
            sample = json.dumps(record)[:_SAMPLE_CHARS] if record else None
            session.add(SeenCollection(nsid=nsid, event_count=1, sample_json=sample))
            return
        row.event_count += 1
        row.last_seen = utcnow()
        if row.sample_json is None and record:
            # The first sighting may have been a delete, which carries no
            # record; take the first real one so the report can show a shape.
            row.sample_json = json.dumps(record)[:_SAMPLE_CHARS]


def known_collections() -> set[str]:
    """NSIDs already accounted for, so the report only shows what is new."""
    return set(get_settings().wanted_collections)


def report() -> list[SeenCollection]:
    """Every observed collection, busiest first."""
    with session_scope() as session:
        return list(session.query(SeenCollection).order_by(SeenCollection.event_count.desc()).all())


async def run(*, seconds: float = 60.0, limit: int | None = None) -> int:
    """Watch the wildcard subscription for a bounded time; returns events seen.

    Bounded by design: this is a survey run from the CLI, not a daemon.
    """
    settings = get_settings()
    if not settings.jetstream_is_v2:
        raise RuntimeError(
            "collection discovery needs a Jetstream v2 endpoint (wildcards are v2-only); "
            f"SKYBRIDGE_JETSTREAM is {settings.jetstream_url}"
        )

    url = _build_url()
    log.info("discovering collections: %s", url)
    seen = 0
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds

    async with websockets.connect(url, max_size=None, subprotocols=[_SUBPROTOCOL]) as ws:
        while (remaining := deadline - loop.time()) > 0:
            if limit is not None and seen >= limit:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                break
            payload = (json.loads(raw) or {}).get("payload") or {}
            nsid = payload.get("collection")
            if nsid:
                record_collection(nsid, payload.get("record"))
                seen += 1
    return seen
