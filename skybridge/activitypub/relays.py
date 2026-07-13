"""Relay subscriptions (client role): ``Follow`` ``as:Public`` at configured relays.

Skybridge is a normal ActivityPub server that additionally subscribes,
Mastodon-relay-style, to external relays listed in ``SKYBRIDGE_RELAYS``. On
startup (:func:`reconcile_relays`) it Follows ``as:Public`` at each configured
inbox, signed by the service actor, and Undoes the Follow for any relay
dropped from config. Once a relay ``Accept``s, :mod:`.delivery` forwards every
author-signed post and every inbound ``Like`` to it.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from skybridge.activitypub.actors import get_relay_keys
from skybridge.activitypub.delivery import DeliveryWorker, Task
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Relay
from skybridge.translate.neodb import PUBLIC

log = logging.getLogger("skybridge.relays")


def build_follow(follow_id: str) -> dict[str, Any]:
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": follow_id,
        "type": "Follow",
        "actor": get_settings().relay_actor_id,
        "object": PUBLIC,
    }


def build_undo_follow(follow_id: str) -> dict[str, Any]:
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{follow_id}#undo",
        "type": "Undo",
        "actor": get_settings().relay_actor_id,
        "object": build_follow(follow_id),
    }


async def reconcile_relays(worker: DeliveryWorker) -> None:
    """Sync ``Relay`` rows with ``SKYBRIDGE_RELAYS``.

    Enqueues (or re-enqueues) ``Follow`` for every configured relay not yet
    accepted, and ``Undo(Follow)`` for any relay removed from config, onto
    ``worker`` (retry/backoff + ``Delivery`` audit rows come for free). Called
    at startup and from the ``ingest`` CLI; best-effort — logs and swallows
    any failure rather than crashing the caller.
    """
    try:
        await _sync_relays(worker)
    except Exception:
        log.exception("relay reconciliation failed")


def _plan_sync(session: Session) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Update ``Relay`` rows in ``session``; return (to_follow, to_undo) as (inbox, follow_id)."""
    settings = get_settings()
    configured = set(settings.relays)
    existing = {row.inbox: row for row in session.scalars(select(Relay))}

    to_follow: list[tuple[str, str]] = []
    for inbox in settings.relays:
        row = existing.get(inbox)
        if row is None:
            row = Relay(inbox=inbox)
            session.add(row)
            existing[inbox] = row
        if row.state == "accepted":
            continue
        if row.state != "pending" or row.follow_activity_id is None:
            row.follow_activity_id = settings.url(f"activities/{uuid4()}")
            row.state = "pending"
        follow_id = row.follow_activity_id
        assert follow_id is not None  # just (re)set above, or was already non-None
        to_follow.append((inbox, follow_id))

    to_undo: list[tuple[str, str]] = []
    for inbox, row in existing.items():
        if inbox in configured or row.state not in ("pending", "accepted"):
            continue
        if row.follow_activity_id is not None:
            to_undo.append((inbox, row.follow_activity_id))
        # Clear the follow id so a late/replayed Accept for it can't match
        # this row again and resurrect an unsubscribed relay.
        row.follow_activity_id = None
        row.state = "unsubscribed"

    return to_follow, to_undo


async def _sync_relays(worker: DeliveryWorker) -> None:
    with session_scope() as session:
        to_follow, to_undo = _plan_sync(session)

    priv, _ = get_relay_keys()
    key_id = f"{get_settings().relay_actor_id}#main-key"
    for inbox, follow_id in to_follow:
        await worker.enqueue(Task(follow_id, inbox, key_id, priv, build_follow(follow_id)))
    for inbox, follow_id in to_undo:
        await worker.enqueue(
            Task(f"{follow_id}#undo", inbox, key_id, priv, build_undo_follow(follow_id))
        )


def _follow_id_of(activity: dict[str, Any]) -> str | None:
    obj = activity.get("object")
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        follow_id = obj.get("id")
        return follow_id if isinstance(follow_id, str) else None
    return None


def _set_relay_state(activity: dict[str, Any], state: str) -> int:
    """Match the Accept/Reject to a ``Relay`` row by exact follow id.

    neodb-relay echoes our Follow id back in the Accept/Reject ``object``, so
    an exact match is sufficient and avoids the spoofable host-based
    fallback this used to fall back to.
    """
    follow_id = _follow_id_of(activity)
    with session_scope() as session:
        row = None
        if follow_id is not None:
            row = session.scalar(select(Relay).where(Relay.follow_activity_id == follow_id))
        if row is None:
            return 202  # unknown Follow: nothing to update
        row.state = state
    return 202


def handle_accept(activity: dict[str, Any]) -> int:
    return _set_relay_state(activity, "accepted")


def handle_reject(activity: dict[str, Any]) -> int:
    return _set_relay_state(activity, "rejected")
