"""Inbox handling: the relay/Follow handshake and per-author follows.

* A peer **instance** Follows our relay ``Application`` actor (object =
  ``as:Public`` or the relay actor id) → we store a :class:`Subscriber` and
  reply ``Accept``; thereafter it receives ``Announce``d activities.
* A **remote user** Follows a bridged ``Person`` actor → we store a
  :class:`Follow` and reply ``Accept``; thereafter that author's activities are
  delivered to the follower's inbox.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx
from sqlalchemy import delete, select

from skybridge.activitypub.actors import RELAY_DID, get_relay_keys
from skybridge.config import get_settings
from skybridge.crypto import sign_request
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Follow, Subscriber
from skybridge.translate.neodb import PUBLIC

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


def _is_relay_target(obj: Any) -> bool:
    targets = (PUBLIC, get_settings().relay_actor_id)
    if obj in targets:
        return True
    return isinstance(obj, dict) and obj.get("id") in targets


def _target_username(target_actor_id: str | None) -> str | None:
    """Extract the local username from a ``/users/<name>`` actor URL."""
    if not target_actor_id:
        return None
    settings = get_settings()
    prefix = settings.url("users/")
    if target_actor_id.startswith(prefix):
        return target_actor_id[len(prefix) :].split("/")[0]
    return None


async def handle_inbox(activity: dict[str, Any], *, target_actor_id: str | None = None) -> int:
    """Process an inbound activity. Returns an HTTP status code to reply with.

    ``target_actor_id`` is the actor whose inbox received it (``None`` for the
    shared inbox); it disambiguates relay vs per-user Follows.
    """
    kind = activity.get("type")
    if kind == "Follow":
        return await _handle_follow(activity, target_actor_id)
    if kind == "Undo":
        return _handle_undo(activity, target_actor_id)
    if kind in ("Accept", "Reject", "Delete", "Create", "Update", "Announce", "Like"):
        # Nothing actionable inbound for a one-way bridge; acknowledge.
        return 202
    return 202


async def _handle_follow(activity: dict[str, Any], target_actor_id: str | None) -> int:
    actor_id = activity.get("actor")
    if not isinstance(actor_id, str):
        return 400
    obj = activity.get("object")

    remote = await fetch_actor(actor_id)
    if remote is None:
        return 202  # accept-and-forget; we can't deliver Accept without an inbox
    inbox, shared = _inboxes(remote)

    username = _target_username(target_actor_id)
    if username and username != get_settings().relay_username:
        return await _follow_person(username, actor_id, inbox, shared, activity)
    if _is_relay_target(obj) or target_actor_id == get_settings().relay_actor_id:
        return await _subscribe_relay(actor_id, inbox, shared, activity)
    # A Follow of an unknown / non-relay object on the shared inbox: treat as
    # a relay subscription if it pointed at us, else ignore.
    return await _subscribe_relay(actor_id, inbox, shared, activity)


async def _subscribe_relay(
    actor_id: str, inbox: str, shared: str | None, activity: dict[str, Any]
) -> int:
    with session_scope() as session:
        row = session.scalar(select(Subscriber).where(Subscriber.actor_id == actor_id))
        if row is None:
            row = Subscriber(actor_id=actor_id, inbox=inbox, shared_inbox=shared)
            session.add(row)
        row.inbox, row.shared_inbox, row.state = inbox, shared, "accepted"
    priv, _ = get_relay_keys()
    await _send_accept(
        signer_actor=get_settings().relay_actor_id,
        private_pem=priv,
        follow=activity,
        inbox=inbox,
    )
    return 202


async def _follow_person(
    username: str, actor_id: str, inbox: str, shared: str | None, activity: dict[str, Any]
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
    await _send_accept(signer_actor=signer_id, private_pem=priv, follow=activity, inbox=inbox)
    return 202


def _handle_undo(activity: dict[str, Any], target_actor_id: str | None) -> int:
    obj = activity.get("object")
    actor_id = activity.get("actor")
    if not isinstance(actor_id, str):
        return 400
    inner_type = obj.get("type") if isinstance(obj, dict) else None
    if inner_type != "Follow":
        return 202
    username = _target_username(target_actor_id)
    with session_scope() as session:
        if username and username != get_settings().relay_username:
            author = session.scalar(select(BridgedActor).where(BridgedActor.handle == username))
            if author is not None:
                session.execute(
                    delete(Follow).where(
                        Follow.local_did == author.did,
                        Follow.follower_actor_id == actor_id,
                    )
                )
        else:
            session.execute(delete(Subscriber).where(Subscriber.actor_id == actor_id))
    return 202


async def _send_accept(
    *, signer_actor: str, private_pem: str, follow: dict[str, Any], inbox: str
) -> None:
    settings = get_settings()
    accept = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": settings.url(f"activities/{uuid.uuid4()}"),
        "type": "Accept",
        "actor": signer_actor,
        "object": follow,
    }
    body = json.dumps(accept).encode()
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
        log.warning("failed to deliver Accept to %s: %s", inbox, exc)
