"""Aggregate counts for the dashboard, ``/stats`` and NodeInfo."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select

from skybridge.activitypub.actors import RELAY_DID
from skybridge.atproto.jetstream import last_event_at
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Follow, Like, Record, Relay, Work, utcnow

log = logging.getLogger("skybridge.stats")

# NodeInfo's counts are recomputed on this cadence and answered from memory, so
# a scrape costs no queries and nothing ever waits on the counting.
USAGE_INTERVAL = 6 * 3600.0

# "Not counted yet": the document leaves such a number out rather than
# reporting a zero a peer would believe.
UNKNOWN = -1

# NodeInfo's activeMonth window.
_ACTIVE_WINDOW = timedelta(days=30)

_usage: dict[str, int] = {
    "total": UNKNOWN,
    "active_month": UNKNOWN,
    "local_posts": UNKNOWN,
    "relays_accepted": UNKNOWN,
    "works": UNKNOWN,
}


def collect_stats() -> dict[str, Any]:
    last_event = last_event_at()
    with session_scope() as session:
        bridged = session.scalar(
            select(func.count()).select_from(BridgedActor).where(BridgedActor.did != RELAY_DID)
        )
        records_total = session.scalar(select(func.count()).select_from(Record))
        records_active = session.scalar(
            select(func.count()).select_from(Record).where(Record.deleted_at.is_(None))
        )
        relays_accepted = session.scalar(
            select(func.count()).select_from(Relay).where(Relay.state == "accepted")
        )
        follows = session.scalar(select(func.count()).select_from(Follow))
        works = session.scalar(select(func.count()).select_from(Work))
        likes = session.scalar(select(func.count()).select_from(Like))

        by_collection = dict(
            session.execute(select(Record.collection, func.count()).group_by(Record.collection))
            .tuples()
            .all()
        )
        delivery_by_status = dict(
            session.execute(select(Delivery.status, func.count()).group_by(Delivery.status))
            .tuples()
            .all()
        )

        return {
            "bridged_actors": bridged or 0,
            "records_total": records_total or 0,
            "records_active": records_active or 0,
            "relays_configured": len(get_settings().relays),
            "relays_accepted": relays_accepted or 0,
            "follows": follows or 0,
            "works": works or 0,
            "likes": likes or 0,
            "records_by_collection": by_collection,
            "deliveries_by_status": delivery_by_status,
            # Ingest liveness. The HTTP healthcheck only proves the web app
            # answers, and the ingest loop can sit in a reconnect cycle
            # beside it; this is the reading that shows that. Null in a
            # process that does not run the loop, and until the first event.
            "last_event_at": last_event.isoformat() if last_event else None,
        }


def usage_counts() -> dict[str, int]:
    """Count what NodeInfo reports.

    Blocking (five aggregate queries over the whole archive): run it through
    :func:`refresh_usage`, never inline in a request.
    """
    cutoff = utcnow() - _ACTIVE_WINDOW
    with session_scope() as session:
        # An author who opted out is no longer bridged, so they are no longer
        # one of this node's users.
        total = session.scalar(
            select(func.count())
            .select_from(BridgedActor)
            .where(BridgedActor.did != RELAY_DID, BridgedActor.opted_out.is_(False))
        )
        # Bridged activity is the only "activity" an author has here. Skipping
        # the tombstones matters: an opt-out stamps `updated_at` on every
        # record it retracts, which would otherwise read as a month of activity.
        active_month = session.scalar(
            select(func.count(func.distinct(Record.did))).where(
                Record.updated_at >= cutoff, Record.deleted_at.is_(None)
            )
        )
        # Archived-only records (lists, collection membership, merged-away
        # pairs) never became a Note, so they are not posts this node serves.
        local_posts = session.scalar(
            select(func.count())
            .select_from(Record)
            .where(Record.ap_object_json.isnot(None), Record.deleted_at.is_(None))
        )
        relays_accepted = session.scalar(
            select(func.count()).select_from(Relay).where(Relay.state == "accepted")
        )
        works = session.scalar(select(func.count()).select_from(Work))

    return {
        "total": total or 0,
        "active_month": active_month or 0,
        "local_posts": local_posts or 0,
        "relays_accepted": relays_accepted or 0,
        "works": works or 0,
    }


def cached_usage() -> dict[str, int]:
    """The counts as of the last refresh, for a caller that must not block.

    Every entry is :data:`UNKNOWN` until the first refresh lands.
    """
    return dict(_usage)


async def refresh_usage() -> dict[str, int]:
    """Recount off-thread and publish the result to :func:`cached_usage`."""
    global _usage
    _usage = await asyncio.to_thread(usage_counts)
    return _usage


def reset_usage() -> None:
    """Forget the counts (tests, and after the database is swapped)."""
    global _usage
    _usage = dict.fromkeys(_usage, UNKNOWN)


async def usage_refresh_loop(interval: float = USAGE_INTERVAL) -> None:
    """Keep the NodeInfo counts warm; started by the app lifespan.

    Deliberately approximate: the counts age for up to `interval`, and a failed
    pass just leaves the previous numbers standing until the next one.
    """
    while True:
        try:
            await refresh_usage()
        except Exception:
            log.exception("usage refresh failed")
        await asyncio.sleep(interval)
