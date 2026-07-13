"""Inbox handling: per-user follows, relay Accept/Reject, and Like storage.

* A **remote user** Follows a bridged ``Person`` actor → we store a
  :class:`Follow` and reply ``Accept``; thereafter that author's activities are
  delivered to the follower's inbox.
* A Follow of the service actor (or ``as:Public``) gets a polite ``Reject``:
  this server doesn't relay for other instances — see
  :mod:`skybridge.activitypub.relays` for the client-role subscription we
  maintain to *external* relays instead.
* ``Accept``/``Reject`` of one of our own relay subscriptions is dispatched to
  :mod:`skybridge.activitypub.relays`.
* ``Like`` (and its ``Undo``) on one of our local posts is stored in
  :class:`Like` for dedup and forwarded, signed by the service actor, to every
  accepted relay.

Inbound HTTP signature verification is not implemented yet — a future
hardening item; activities are trusted at face value.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx
from sqlalchemy import delete, select

from skybridge.activitypub import objects, relays
from skybridge.activitypub.actors import RELAY_DID, get_relay_keys
from skybridge.activitypub.delivery import DeliveryWorker, Task, forward_to_relays
from skybridge.config import get_settings
from skybridge.crypto import sign_request
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Follow, Like, Record

log = logging.getLogger("skybridge.inbox")


async def fetch_actor(actor_id: str) -> dict[str, Any] | None:
    headers = {
        "Accept": "application/activity+json",
        "User-Agent": get_settings().user_agent,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(actor_id, headers=headers)
            if resp.status_code == 200:
                return resp.json()
    except (httpx.HTTPError, json.JSONDecodeError):
        log.warning("could not fetch remote actor %s", actor_id)
    return None


def _inboxes(actor_doc: dict[str, Any]) -> tuple[str, str | None]:
    inbox = actor_doc.get("inbox", "")
    shared = (actor_doc.get("endpoints") or {}).get("sharedInbox")
    return inbox, shared


def _target_username(target_actor_id: str | None) -> str | None:
    """Extract the local username from a ``/users/<name>`` actor URL."""
    if not target_actor_id:
        return None
    settings = get_settings()
    prefix = settings.url("users/")
    if target_actor_id.startswith(prefix):
        return target_actor_id[len(prefix) :].split("/")[0]
    return None


def _local_post_record(object_id: str) -> Record | None:
    """Resolve a local ``.../users/<ident>/posts/<rkey>`` object id to its live Record."""
    prefix = get_settings().url("users/")
    if not object_id.startswith(prefix):
        return None
    ident, sep, rkey = object_id[len(prefix) :].partition("/posts/")
    if not sep or not ident or not rkey:
        return None
    record = objects._record_for(ident, rkey)
    if record is None or record.deleted_at is not None or record.ap_object_json is None:
        return None
    return record


async def handle_inbox(
    activity: dict[str, Any],
    *,
    target_actor_id: str | None = None,
    worker: DeliveryWorker | None = None,
) -> int:
    """Process an inbound activity. Returns an HTTP status code to reply with.

    ``target_actor_id`` is the actor whose inbox received it (``None`` for the
    shared inbox); it disambiguates a per-user Follow from one aimed at the
    service actor. ``worker``, when supplied, lets Like/Undo(Like) forward to
    configured relays, and lets the Follow Accept/Reject be enqueued instead
    of sent inline.
    """
    kind = activity.get("type")
    if kind == "Follow":
        return await _handle_follow(activity, target_actor_id, worker)
    if kind == "Undo":
        return await _handle_undo(activity, target_actor_id, worker)
    if kind == "Accept":
        return relays.handle_accept(activity)
    if kind == "Reject":
        return relays.handle_reject(activity)
    if kind == "Like":
        return await _handle_like(activity, worker)
    # Delete/Create/Update/Announce etc: nothing actionable inbound; ack.
    return 202


async def _handle_follow(
    activity: dict[str, Any], target_actor_id: str | None, worker: DeliveryWorker | None
) -> int:
    actor_id = activity.get("actor")
    if not isinstance(actor_id, str):
        return 400

    remote = await fetch_actor(actor_id)
    if remote is None:
        return 202  # accept-and-forget; we can't deliver a response without an inbox
    inbox, shared = _inboxes(remote)

    username = _target_username(target_actor_id)
    if username and username != get_settings().relay_username:
        return await _follow_person(username, actor_id, inbox, shared, activity, worker)

    # A Follow of the service actor / as:Public: we're a normal server now,
    # not a relay for other instances — politely decline.
    priv, _ = get_relay_keys()
    await _send_follow_response(
        kind="Reject",
        signer_actor=get_settings().relay_actor_id,
        private_pem=priv,
        follow=activity,
        inbox=inbox,
        worker=worker,
    )
    return 202


async def _follow_person(
    username: str,
    actor_id: str,
    inbox: str,
    shared: str | None,
    activity: dict[str, Any],
    worker: DeliveryWorker | None,
) -> int:
    with session_scope() as session:
        author = session.scalar(
            select(BridgedActor).where(
                BridgedActor.handle == username, BridgedActor.did != RELAY_DID
            )
        )
        if author is None:
            return 404
        existing = session.scalar(
            select(Follow).where(
                Follow.local_did == author.did, Follow.follower_actor_id == actor_id
            )
        )
        if existing is None:
            session.add(
                Follow(
                    local_did=author.did,
                    follower_actor_id=actor_id,
                    follower_inbox=inbox,
                    follower_shared_inbox=shared,
                    state="accepted",
                )
            )
        else:
            existing.follower_inbox, existing.follower_shared_inbox = inbox, shared
            existing.state = "accepted"
        signer_id = get_settings().actor_id(author.handle)
        priv = author.private_key_pem
    await _send_follow_response(
        kind="Accept",
        signer_actor=signer_id,
        private_pem=priv,
        follow=activity,
        inbox=inbox,
        worker=worker,
    )
    return 202


async def _handle_undo(
    activity: dict[str, Any], target_actor_id: str | None, worker: DeliveryWorker | None
) -> int:
    obj = activity.get("object")
    actor_id = activity.get("actor")
    if not isinstance(actor_id, str):
        return 400
    if isinstance(obj, dict) and obj.get("type") == "Follow":
        return _handle_undo_follow(obj, actor_id, target_actor_id)
    if isinstance(obj, dict) and obj.get("type") == "Like":
        return await _handle_undo_like(obj.get("id"), actor_id, activity, worker)
    if isinstance(obj, str):
        return await _handle_undo_like(obj, actor_id, activity, worker)
    return 202


def _handle_undo_follow(follow: dict[str, Any], actor_id: str, target_actor_id: str | None) -> int:
    username = _target_username(target_actor_id)
    if username and username != get_settings().relay_username:
        with session_scope() as session:
            author = session.scalar(select(BridgedActor).where(BridgedActor.handle == username))
            if author is not None:
                session.execute(
                    delete(Follow).where(
                        Follow.local_did == author.did,
                        Follow.follower_actor_id == actor_id,
                    )
                )
    # An Undo(Follow) aimed at the service actor (formerly a relay subscriber
    # unsubscribing) is now a no-op: we no longer track service-actor followers.
    return 202


async def _handle_undo_like(
    like_id: Any, actor_id: str, activity: dict[str, Any], worker: DeliveryWorker | None
) -> int:
    if not isinstance(like_id, str):
        return 202
    with session_scope() as session:
        row = session.scalar(select(Like).where(Like.activity_id == like_id))
        if row is None or row.actor_id != actor_id:
            return 202  # unknown Like, or actor mismatch (no signature verification): no-op
        session.delete(row)
    if worker is not None:
        await forward_to_relays(worker, record_uri=activity.get("id") or like_id, activity=activity)
    return 202


async def _handle_like(activity: dict[str, Any], worker: DeliveryWorker | None) -> int:
    like_id = activity.get("id")
    actor_id = activity.get("actor")
    obj = activity.get("object")
    object_id = obj if isinstance(obj, str) else obj.get("id") if isinstance(obj, dict) else None
    if not isinstance(like_id, str) or not isinstance(actor_id, str):
        return 400
    if not isinstance(object_id, str):
        return 400
    if _local_post_record(object_id) is None:
        return 202  # not a live local post: no-op

    with session_scope() as session:
        dup = session.scalar(
            select(Like).where(
                (Like.activity_id == like_id)
                | ((Like.actor_id == actor_id) & (Like.object_id == object_id))
            )
        )
        if dup is not None:
            return 202
        session.add(
            Like(
                activity_id=like_id,
                actor_id=actor_id,
                object_id=object_id,
                raw_json=json.dumps(activity),
            )
        )

    if worker is not None:
        await forward_to_relays(worker, record_uri=like_id, activity=activity)
    return 202


async def _send_follow_response(
    *,
    kind: str,
    signer_actor: str,
    private_pem: str,
    follow: dict[str, Any],
    inbox: str,
    worker: DeliveryWorker | None = None,
) -> None:
    """Send an Accept/Reject for a Follow.

    Enqueued on the ``DeliveryWorker`` when one is available so a slow remote
    inbox can't stall the request that triggered it; falls back to an inline
    signed POST when no worker is running.
    """
    settings = get_settings()
    response = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": settings.url(f"activities/{uuid.uuid4()}"),
        "type": kind,
        "actor": signer_actor,
        "object": follow,
    }
    if worker is not None:
        await worker.enqueue(
            Task(response["id"], inbox, f"{signer_actor}#main-key", private_pem, response)
        )
        return
    body = json.dumps(response).encode()
    headers = sign_request(
        private_pem=private_pem,
        key_id=f"{signer_actor}#main-key",
        method="POST",
        url=inbox,
        body=body,
    )
    headers["Accept"] = "application/activity+json"
    headers["User-Agent"] = settings.user_agent
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(inbox, content=body, headers=headers)
    except httpx.HTTPError as exc:
        log.warning("failed to deliver %s to %s: %s", kind, inbox, exc)
