"""Outbound delivery: signed POSTs, fanout, and a retry/backoff worker.

Two delivery paths (see the pipeline):

* **relay path** — the relay ``Application`` actor ``Announce``s every
  translated activity to all accepted instance subscribers' inboxes.
* **direct path** — each bridged author signs and delivers its own
  ``Create``/``Update``/``Delete`` to that author's followers' inboxes.

Delivery is in-process and best-effort: failures are logged to the ``delivery``
table and retried with exponential backoff, never blocking ingestion.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy import select

from skybridge.activitypub.actors import get_relay_keys
from skybridge.config import get_settings
from skybridge.crypto import sign_request
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Follow, Subscriber, utcnow

log = logging.getLogger("skybridge.delivery")


@dataclass
class Task:
    record_uri: str
    target_inbox: str
    key_id: str
    private_pem: str
    activity: dict[str, Any]
    attempt: int = 0


@dataclass
class DeliveryWorker:
    """In-process async delivery queue with bounded exponential-backoff retry."""

    queue: asyncio.Queue[Task] = field(default_factory=asyncio.Queue)
    _task: asyncio.Task | None = None
    _stopping: bool = False
    # Strong refs to in-flight retry timers so they aren't GC'd mid-flight.
    _pending: set[asyncio.Task] = field(default_factory=set)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="delivery-worker")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            await self.queue.join()
            self._task.cancel()
            self._task = None

    async def enqueue(self, task: Task) -> None:
        await self.queue.put(task)

    async def _run(self) -> None:
        async with httpx.AsyncClient(timeout=15.0) as client:
            while True:
                task = await self.queue.get()
                try:
                    await self._deliver(client, task)
                except Exception:  # never let the worker die
                    log.exception("delivery task crashed")
                finally:
                    self.queue.task_done()

    async def _deliver(self, client: httpx.AsyncClient, task: Task) -> None:
        settings = get_settings()
        body = json.dumps(task.activity).encode()
        ok, code = await post_signed(
            client,
            inbox=task.target_inbox,
            key_id=task.key_id,
            private_pem=task.private_pem,
            body=body,
        )
        _record_attempt(task, status="sent" if ok else "failed", code=code)
        if ok or task.attempt + 1 >= len(settings.retry_backoff):
            return
        delay = settings.retry_backoff[task.attempt]
        task.attempt += 1
        retry = asyncio.create_task(self._requeue_after(delay, task))
        self._pending.add(retry)
        retry.add_done_callback(self._pending.discard)

    async def _requeue_after(self, delay: int, task: Task) -> None:
        await asyncio.sleep(delay)
        if not self._stopping:
            await self.queue.put(task)


async def post_signed(
    client: httpx.AsyncClient,
    *,
    inbox: str,
    key_id: str,
    private_pem: str,
    body: bytes,
) -> tuple[bool, int | None]:
    """POST a signed activity to an inbox. Returns ``(ok, status_code)``."""
    headers = sign_request(
        private_pem=private_pem,
        key_id=key_id,
        method="POST",
        url=inbox,
        body=body,
    )
    headers["Accept"] = "application/activity+json"
    headers["User-Agent"] = get_settings().user_agent
    try:
        resp = await client.post(inbox, content=body, headers=headers)
        return (200 <= resp.status_code < 300, resp.status_code)
    except httpx.HTTPError as exc:
        log.warning("delivery to %s failed: %s", inbox, exc)
        return (False, None)


def _record_attempt(task: Task, *, status: str, code: int | None) -> None:
    with session_scope() as session:
        row = session.scalar(
            select(Delivery).where(
                Delivery.record_uri == task.record_uri,
                Delivery.target_inbox == task.target_inbox,
            )
        )
        if row is None:
            row = Delivery(
                record_uri=task.record_uri,
                target_inbox=task.target_inbox,
                activity_type=task.activity.get("type", "Activity"),
            )
            session.add(row)
        row.status = status
        row.attempts = task.attempt + 1
        row.response_code = code
        row.last_attempt = utcnow()


def _dedup_inboxes(targets: list[tuple[str, str | None]]) -> list[str]:
    """Collapse (inbox, shared_inbox) pairs, preferring shared inboxes."""
    seen: set[str] = set()
    out: list[str] = []
    for inbox, shared in targets:
        chosen = shared or inbox
        if chosen and chosen not in seen:
            seen.add(chosen)
            out.append(chosen)
    return out


def relay_targets() -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Subscriber.inbox, Subscriber.shared_inbox).where(Subscriber.state == "accepted")
        ).all()
    return _dedup_inboxes([(r[0], r[1]) for r in rows])


def follower_targets(local_did: str) -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Follow.follower_inbox, Follow.follower_shared_inbox).where(
                Follow.local_did == local_did, Follow.state == "accepted"
            )
        ).all()
    return _dedup_inboxes([(r[0], r[1]) for r in rows])


def announce(activity: dict[str, Any]) -> dict[str, Any]:
    """Wrap an activity in an ``Announce`` by the relay actor (by reference)."""
    settings = get_settings()
    actor = settings.relay_actor_id
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{activity.get('id', settings.relay_actor_id)}#announce",
        "type": "Announce",
        "actor": actor,
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
        "cc": [f"{actor}/followers"],
        "object": activity.get("id") or activity,
    }


async def fanout(
    worker: DeliveryWorker,
    *,
    record_uri: str,
    did: str,
    activity: dict[str, Any],
) -> int:
    """Enqueue relay ``Announce`` + direct author delivery. Returns task count."""
    settings = get_settings()
    count = 0

    # Relay path: Announce to accepted instance subscribers.
    relay_inboxes = relay_targets()
    if relay_inboxes:
        relay_priv, _ = get_relay_keys()
        relay_key = f"{settings.relay_actor_id}#main-key"
        ann = announce(activity)
        for inbox in relay_inboxes:
            await worker.enqueue(Task(record_uri, inbox, relay_key, relay_priv, ann))
            count += 1

    # Direct path: the bridged author delivers to its own followers.
    follower_inboxes = follower_targets(did)
    if follower_inboxes:
        with session_scope() as session:
            actor_row = session.get(BridgedActor, did)
            if actor_row is not None:
                priv = actor_row.private_key_pem
                key_id = f"{settings.actor_id(actor_row.handle)}#main-key"
            else:
                priv = key_id = None
        if priv and key_id:
            for inbox in follower_inboxes:
                await worker.enqueue(Task(record_uri, inbox, key_id, priv, activity))
                count += 1

    return count
