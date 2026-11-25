"""Aggregate counts for the dashboard, ``/stats`` and NodeInfo.

Every count is a whole-table aggregate, and on a large archive some of them
take minutes. No request ever runs one: two background loops recount them
off-thread and requests read the last result from memory.

- :func:`stats_refresh_loop` recounts the dashboard figures every few minutes.
  Each of its queries reads an index or a small table, never all of ``record``.
- :func:`usage_refresh_loop` recounts what NodeInfo reports, plus the active
  record count, every few hours. These need one full pass over ``record``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import case, func, select

from skybridge.activitypub.actors import RELAY_DID
from skybridge.atproto.jetstream import last_event_at
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Follow, Like, Record, Relay, Work, utcnow

log = logging.getLogger("skybridge.stats")

# NodeInfo's counts are recomputed on this cadence and answered from memory, so
# a scrape costs no queries and nothing ever waits on the counting.
USAGE_INTERVAL = 6 * 3600.0

# The dashboard and /stats figures, likewise. Short, because each pass is
# cheap; see count_stats for why.
STATS_INTERVAL = 5 * 60.0

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
    # Not part of NodeInfo: the dashboard's "active records". It needs the
    # same full pass as active_month, so it is counted there.
    "records_active": UNKNOWN,
}

# The figures count_stats returns, by name. Before the first refresh lands each
# count reads None (JSON null), never a zero a monitor would believe.
_COUNTS = ("bridged_actors", "records_total", "relays_accepted", "follows", "works", "likes")
_BREAKDOWNS = ("records_by_collection", "deliveries_by_status")


def _cold_stats() -> dict[str, Any]:
    return {**dict.fromkeys(_COUNTS), **{name: {} for name in _BREAKDOWNS}}


_stats: dict[str, Any] = _cold_stats()
_stats_at: datetime | None = None


def count_stats() -> dict[str, Any]:
    """Count the dashboard figures.

    Blocking: run it through :func:`refresh_stats`, never inline in a request.
    Every query here reads an index or a small table: the record count comes
    from the narrowest index on ``record`` and the per-collection breakdown
    from ``ix_record_collection``. A count that has to read the ``record``
    table itself, such as the live (not tombstoned) records, belongs in
    :func:`usage_counts`.
    """
    with session_scope() as session:
        bridged = session.scalar(
            select(func.count()).select_from(BridgedActor).where(BridgedActor.did != RELAY_DID)
        )
        records_total = session.scalar(select(func.count()).select_from(Record))
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
        "relays_accepted": relays_accepted or 0,
        "follows": follows or 0,
        "works": works or 0,
        "likes": likes or 0,
        "records_by_collection": by_collection,
        "deliveries_by_status": delivery_by_status,
    }


def _document(counts: dict[str, Any], records_active: int | None) -> dict[str, Any]:
    """The ``/stats`` document around a set of counts.

    The keys are the ones ``/stats`` has always served, so a monitor that
    reads it keeps working.
    """
    last_event = last_event_at()
    return {
        "bridged_actors": counts["bridged_actors"],
        "records_total": counts["records_total"],
        "records_active": records_active,
        "relays_configured": len(get_settings().relays),
        "relays_accepted": counts["relays_accepted"],
        "follows": counts["follows"],
        "works": counts["works"],
        "likes": counts["likes"],
        "records_by_collection": counts["records_by_collection"],
        "deliveries_by_status": counts["deliveries_by_status"],
        # Ingest liveness. The HTTP healthcheck only proves the web app
        # answers, and the ingest loop can sit in a reconnect cycle beside
        # it; this is the reading that shows that. Null in a process that
        # does not run the loop, and until the first event. Read live on
        # every call, since it costs no query.
        "last_event_at": last_event.isoformat() if last_event else None,
    }


def collect_stats() -> dict[str, Any]:
    """Count everything now, including the full-table active record count.

    Blocking, and slow on a large archive: for the CLI and tests. Requests
    read :func:`cached_stats` instead.
    """
    with session_scope() as session:
        records_active = session.scalar(
            select(func.count()).select_from(Record).where(Record.deleted_at.is_(None))
        )
    return _document(count_stats(), records_active or 0)


def cached_stats() -> dict[str, Any]:
    """The ``/stats`` document as of the last refresh, for a caller that must
    not block.

    The counts read None until the first refresh lands. ``records_active``
    comes from the slower usage pass, so it can be up to
    :data:`USAGE_INTERVAL` old. ``stats_updated_at`` says when the other
    counts were taken.
    """
    active = _usage["records_active"]
    doc = _document(_stats, None if active == UNKNOWN else active)
    doc["stats_updated_at"] = _stats_at.isoformat() if _stats_at else None
    return doc


async def refresh_stats() -> dict[str, Any]:
    """Recount off-thread and publish the result to :func:`cached_stats`."""
    global _stats, _stats_at
    counts = await asyncio.to_thread(count_stats)
    _stats, _stats_at = counts, utcnow()
    return counts


def reset_stats() -> None:
    """Forget the counts (tests, and after the database is swapped)."""
    global _stats, _stats_at
    _stats, _stats_at = _cold_stats(), None


async def stats_refresh_loop(interval: float = STATS_INTERVAL) -> None:
    """Keep the dashboard counts warm; started by the app lifespan.

    A failed pass leaves the previous numbers standing until the next one.
    """
    while True:
        try:
            await refresh_stats()
        except Exception:
            log.exception("stats refresh failed")
        await asyncio.sleep(interval)


def usage_counts() -> dict[str, int]:
    """Count what NodeInfo reports, and the active record count.

    Blocking, and one full pass over ``record``: run it through
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
        # The three record counts share one pass over the table, since no
        # index serves `deleted_at IS NULL` on its own.
        #
        # active_month: bridged activity is the only "activity" an author has
        # here. Skipping the tombstones matters: an opt-out stamps
        # `updated_at` on every record it retracts, which would otherwise read
        # as a month of activity.
        #
        # local_posts: archived-only records (lists, collection membership,
        # merged-away pairs) never became a Note, so they are not posts this
        # node serves. count(column) skips the NULLs.
        records_active, active_month, local_posts = session.execute(
            select(
                func.count(),
                func.count(func.distinct(case((Record.updated_at >= cutoff, Record.did)))),
                func.count(Record.ap_object_json),
            ).where(Record.deleted_at.is_(None))
        ).one()
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
        "records_active": records_active or 0,
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
