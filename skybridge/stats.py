"""Aggregate counts for the dashboard, ``/stats`` and NodeInfo."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select

from skybridge.activitypub.actors import RELAY_DID
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Follow, Record, Subscriber, Work


def collect_stats() -> dict[str, Any]:
    with session_scope() as session:
        bridged = session.scalar(
            select(func.count()).select_from(BridgedActor).where(BridgedActor.did != RELAY_DID)
        )
        records_total = session.scalar(select(func.count()).select_from(Record))
        records_active = session.scalar(
            select(func.count()).select_from(Record).where(Record.deleted_at.is_(None))
        )
        subs_accepted = session.scalar(
            select(func.count()).select_from(Subscriber).where(Subscriber.state == "accepted")
        )
        subs_total = session.scalar(select(func.count()).select_from(Subscriber))
        follows = session.scalar(select(func.count()).select_from(Follow))
        works = session.scalar(select(func.count()).select_from(Work))

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
            "subscribers_accepted": subs_accepted or 0,
            "subscribers_total": subs_total or 0,
            "follows": follows or 0,
            "works": works or 0,
            "records_by_collection": by_collection,
            "deliveries_by_status": delivery_by_status,
        }
