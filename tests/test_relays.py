"""Relay subscription client: reconciliation + Accept/Reject handling."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from skybridge.activitypub import relays
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.config import set_settings
from skybridge.db import session_scope
from skybridge.models import Relay
from skybridge.translate.neodb import PUBLIC
from sqlalchemy import select

RELAY_INBOX = "https://relay.example/inbox"


def _drain(worker: DeliveryWorker) -> list:
    """Pop every currently-queued Task off ``worker`` (never started)."""
    tasks = []
    while not worker.queue.empty():
        tasks.append(worker.queue.get_nowait())
    return tasks


def _relay_state(inbox: str) -> str:
    with session_scope() as session:
        row = session.scalar(select(Relay).where(Relay.inbox == inbox))
        assert row is not None
        return row.state


def _relay_row(inbox: str) -> Relay:
    with session_scope() as session:
        row = session.scalar(select(Relay).where(Relay.inbox == inbox))
        assert row is not None
        return row


def test_reconcile_creates_pending_row_and_sends_follow(settings):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    worker = DeliveryWorker()  # never started: inspect the queue directly
    asyncio.run(relays.reconcile_relays(worker))

    tasks = _drain(worker)
    assert len(tasks) == 1
    task = tasks[0]
    assert task.target_inbox == RELAY_INBOX
    assert task.key_id == f"{settings.relay_actor_id}#main-key"
    assert task.activity["type"] == "Follow"
    assert task.activity["actor"] == settings.relay_actor_id
    assert task.activity["object"] == PUBLIC

    assert _relay_state(RELAY_INBOX) == "pending"
    row = _relay_row(RELAY_INBOX)
    assert row.follow_activity_id == task.activity["id"]


def test_reconcile_resends_same_follow_id_while_pending(settings):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    worker = DeliveryWorker()
    asyncio.run(relays.reconcile_relays(worker))
    first_id = _drain(worker)[0].activity["id"]

    asyncio.run(relays.reconcile_relays(worker))

    tasks = _drain(worker)
    assert len(tasks) == 1
    assert tasks[0].activity["type"] == "Follow"
    assert tasks[0].activity["id"] == first_id


def test_reconcile_undoes_relay_removed_from_config(settings):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    worker = DeliveryWorker()
    asyncio.run(relays.reconcile_relays(worker))
    follow_id = _drain(worker)[0].activity["id"]

    set_settings(replace(settings, relays=()))
    asyncio.run(relays.reconcile_relays(worker))

    tasks = _drain(worker)
    assert len(tasks) == 1
    task = tasks[0]
    assert task.target_inbox == RELAY_INBOX
    assert task.activity["type"] == "Undo"
    assert task.activity["object"]["id"] == follow_id

    row = _relay_row(RELAY_INBOX)
    assert row.state == "unsubscribed"
    assert row.follow_activity_id is None


def test_reconcile_empty_config_and_no_rows_is_noop(settings):
    set_settings(replace(settings, relays=()))
    worker = DeliveryWorker()
    asyncio.run(relays.reconcile_relays(worker))
    assert _drain(worker) == []


def test_handle_accept_with_id_string_object(settings):
    follow_id = settings.url("activities/abc")
    with session_scope() as session:
        session.add(Relay(inbox=RELAY_INBOX, follow_activity_id=follow_id, state="pending"))

    status = relays.handle_accept(
        {"type": "Accept", "actor": "https://relay.example/actor", "object": follow_id}
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "accepted"


def test_handle_accept_with_embedded_follow_dict(settings):
    follow_id = settings.url("activities/abc")
    with session_scope() as session:
        session.add(Relay(inbox=RELAY_INBOX, follow_activity_id=follow_id, state="pending"))

    status = relays.handle_accept(
        {
            "type": "Accept",
            "actor": "https://relay.example/actor",
            "object": relays.build_follow(follow_id),
        }
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "accepted"


def test_handle_accept_unknown_id_is_noop(settings):
    with session_scope() as session:
        session.add(Relay(inbox=RELAY_INBOX, follow_activity_id="known-id", state="pending"))

    status = relays.handle_accept(
        {"type": "Accept", "actor": "https://relay.example/actor", "object": "unknown-id"}
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "pending"


def test_handle_accept_after_unsubscribe_does_not_resurrect(settings):
    """A late/replayed Accept for a cleared follow id must not flip back to accepted."""
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    worker = DeliveryWorker()
    asyncio.run(relays.reconcile_relays(worker))
    follow_id = _drain(worker)[0].activity["id"]

    set_settings(replace(settings, relays=()))
    asyncio.run(relays.reconcile_relays(worker))
    _drain(worker)
    assert _relay_row(RELAY_INBOX).follow_activity_id is None

    status = relays.handle_accept(
        {"type": "Accept", "actor": "https://relay.example/actor", "object": follow_id}
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "unsubscribed"


def test_handle_reject_sets_rejected(settings):
    follow_id = settings.url("activities/xyz")
    with session_scope() as session:
        session.add(Relay(inbox=RELAY_INBOX, follow_activity_id=follow_id, state="pending"))

    status = relays.handle_reject(
        {"type": "Reject", "actor": "https://relay.example/actor", "object": follow_id}
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "rejected"
