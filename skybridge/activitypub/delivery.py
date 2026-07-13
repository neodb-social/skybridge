"""Outbound delivery: signed POSTs, fanout, and a retry/backoff worker.

Two delivery paths (see the pipeline):

* **direct path** — each bridged author signs and delivers its own
  ``Create``/``Update``/``Delete`` to that author's followers' inboxes.
* **relay path** — the same author-signed activity also goes to every
  accepted relay inbox (external relays we subscribe to as a client; see
  :mod:`skybridge.activitypub.relays`), with no ``Announce`` wrapper.

Delivery is in-process and best-effort: failures are logged to the ``delivery``
table and retried with exponential backoff, never blocking ingestion.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy import select

from skybridge.activitypub.actors import get_relay_keys
from skybridge.config import get_settings
from skybridge.crypto import sign_request
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, Follow, Relay, utcnow

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


def relay_inboxes() -> list[str]:
    """Accepted relay inboxes, intersected with the currently configured set.

    Defense-in-depth: a stale or resurrected ``accepted`` row can't receive
    traffic once its inbox is no longer in ``SKYBRIDGE_RELAYS``.
    """
    configured = set(get_settings().relays)
    with session_scope() as session:
        accepted = session.scalars(select(Relay.inbox).where(Relay.state == "accepted"))
        return [inbox for inbox in accepted if inbox in configured]


def follower_targets(local_did: str) -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Follow.follower_inbox, Follow.follower_shared_inbox).where(
                Follow.local_did == local_did, Follow.state == "accepted"
            )
        ).all()
    return _dedup_inboxes([(r[0], r[1]) for r in rows])


def _author_key(did: str) -> tuple[str, str] | None:
    """``(private_pem, key_id)`` for a bridged author, or ``None`` if unknown."""
    with session_scope() as session:
        actor_row = session.get(BridgedActor, did)
        if actor_row is None:
            return None
        key_id = f"{get_settings().actor_id(actor_row.handle)}#main-key"
        return actor_row.private_key_pem, key_id


async def _deliver_direct(
    worker: DeliveryWorker,
    *,
    record_uri: str,
    did: str,
    activity: dict[str, Any],
    key: tuple[str, str] | None = None,
) -> int:
    """Direct path: the bridged author delivers ``activity`` to its own followers.

    ``key`` is the ``(private_pem, key_id)`` pair for ``did``, when the caller
    already resolved it (e.g. :func:`fanout`); otherwise it's looked up here.
    """
    count = 0
    follower_inboxes = follower_targets(did)
    if follower_inboxes:
        if key is None:
            key = _author_key(did)
        if key is not None:
            priv, key_id = key
            for inbox in follower_inboxes:
                await worker.enqueue(Task(record_uri, inbox, key_id, priv, activity))
                count += 1
    return count


async def fanout(
    worker: DeliveryWorker,
    *,
    record_uri: str,
    did: str,
    activity: dict[str, Any],
) -> int:
    """Enqueue the author-signed activity to accepted relays + the author's
    followers. Returns the number of delivery tasks enqueued.
    """
    # Resolved once up front and threaded through both paths below, so an
    # unknown author is a single query and skips both paths gracefully.
    key = _author_key(did)
    if key is None:
        return 0
    priv, key_id = key

    count = 0

    # Relay path: the same author-signed activity, no Announce wrapper.
    for inbox in relay_inboxes():
        await worker.enqueue(Task(record_uri, inbox, key_id, priv, activity))
        count += 1

    # Direct path: the bridged author delivers to its own followers.
    count += await _deliver_direct(
        worker, record_uri=record_uri, did=did, activity=activity, key=key
    )

    return count


async def fanout_actor_update(
    worker: DeliveryWorker,
    *,
    did: str,
    activity: dict[str, Any],
) -> int:
    """Direct-deliver an actor ``Update`` to just that author's followers.

    Profile updates are never forwarded to configured relays (that's the
    ``fanout`` relay path, for per-work content) — this is just a direct
    refresh of the actor document itself.
    """
    return await _deliver_direct(worker, record_uri=activity["id"], did=did, activity=activity)


async def forward_to_relays(
    worker: DeliveryWorker, *, record_uri: str, activity: dict[str, Any]
) -> int:
    """Wrap ``activity`` in an ``Announce`` by the service actor and enqueue it
    to every accepted relay, signed by the service actor.

    neodb-relay only redistributes ``Create``/``Update``/``Delete``/``Move``
    and ``Announce`` addressed to ``as:Public`` (it ignores ``Like``
    entirely), so a forwarded ``Like`` is embedded as the ``Announce``'s
    ``object`` rather than sent raw. This also fixes a signer/actor mismatch:
    the ``Announce`` actor is the service actor that signs it, whereas the raw
    activity's own actor is the remote peer that authored it.
    """
    inboxes = relay_inboxes()
    if not inboxes:
        return 0
    settings = get_settings()
    priv, _ = get_relay_keys()
    key_id = f"{settings.relay_actor_id}#main-key"
    announce = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{settings.relay_actor_id}/activities/{uuid.uuid4()}",
        "type": "Announce",
        "actor": settings.relay_actor_id,
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
        "object": activity,
    }
    for inbox in inboxes:
        await worker.enqueue(Task(record_uri, inbox, key_id, priv, announce))
    return len(inboxes)
