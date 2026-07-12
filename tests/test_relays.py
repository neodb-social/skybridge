"""Relay subscription client: reconciliation + Accept/Reject handling."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from skybridge.activitypub import relays
from skybridge.config import set_settings
from skybridge.db import session_scope
from skybridge.models import Relay
from skybridge.translate.neodb import PUBLIC
from sqlalchemy import select

RELAY_INBOX = "https://relay.example/inbox"


@pytest.fixture
def capture_posts(monkeypatch):
    """Capture (inbox, activity, key_id) for every ``post_signed`` call."""
    calls: list[tuple[str, dict, str]] = []

    async def fake_post_signed(client, *, inbox, key_id, private_pem, body):
        calls.append((inbox, json.loads(body), key_id))
        return True, 202

    monkeypatch.setattr(relays, "post_signed", fake_post_signed)
    return calls


def _relay_state(inbox: str) -> str:
    with session_scope() as session:
        row = session.scalar(select(Relay).where(Relay.inbox == inbox))
        assert row is not None
        return row.state


def test_reconcile_creates_pending_row_and_sends_follow(settings, capture_posts):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    asyncio.run(relays.reconcile_relays())

    assert len(capture_posts) == 1
    inbox, activity, key_id = capture_posts[0]
    assert inbox == RELAY_INBOX
    assert activity["type"] == "Follow"
    assert activity["actor"] == settings.relay_actor_id
    assert activity["object"] == PUBLIC
    assert key_id == f"{settings.relay_actor_id}#main-key"

    assert _relay_state(RELAY_INBOX) == "pending"
    with session_scope() as session:
        row = session.scalar(select(Relay).where(Relay.inbox == RELAY_INBOX))
        assert row is not None
        assert row.follow_activity_id == activity["id"]


def test_reconcile_resends_same_follow_id_while_pending(settings, capture_posts):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    asyncio.run(relays.reconcile_relays())
    first_id = capture_posts[0][1]["id"]

    asyncio.run(relays.reconcile_relays())

    assert len(capture_posts) == 2
    assert capture_posts[1][1]["type"] == "Follow"
    assert capture_posts[1][1]["id"] == first_id


def test_reconcile_undoes_relay_removed_from_config(settings, capture_posts):
    set_settings(replace(settings, relays=(RELAY_INBOX,)))
    asyncio.run(relays.reconcile_relays())
    follow_id = capture_posts[0][1]["id"]

    set_settings(replace(settings, relays=()))
    asyncio.run(relays.reconcile_relays())

    assert len(capture_posts) == 2
    inbox, activity, _ = capture_posts[1]
    assert inbox == RELAY_INBOX
    assert activity["type"] == "Undo"
    assert activity["object"]["id"] == follow_id

    assert _relay_state(RELAY_INBOX) == "unsubscribed"


def test_reconcile_empty_config_and_no_rows_is_noop(settings, capture_posts):
    set_settings(replace(settings, relays=()))
    asyncio.run(relays.reconcile_relays())
    assert capture_posts == []


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


def test_handle_accept_falls_back_to_actor_host_when_id_stripped(settings):
    with session_scope() as session:
        session.add(
            Relay(
                inbox=RELAY_INBOX,
                follow_activity_id=settings.url("activities/stripped"),
                state="pending",
            )
        )

    accept = {
        "type": "Accept",
        "actor": "https://relay.example/actor",
        # The relay echoed the Follow back without its "id".
        "object": {"type": "Follow", "actor": settings.relay_actor_id, "object": PUBLIC},
    }
    status = relays.handle_accept(accept)

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "accepted"


def test_handle_reject_sets_rejected(settings):
    follow_id = settings.url("activities/xyz")
    with session_scope() as session:
        session.add(Relay(inbox=RELAY_INBOX, follow_activity_id=follow_id, state="pending"))

    status = relays.handle_reject(
        {"type": "Reject", "actor": "https://relay.example/actor", "object": follow_id}
    )

    assert status == 202
    assert _relay_state(RELAY_INBOX) == "rejected"
