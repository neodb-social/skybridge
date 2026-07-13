"""Inbound Like storage/dedup/forwarding, its Undo, and the relay-Follow Reject."""

from __future__ import annotations

import asyncio

import pytest
from skybridge.activitypub import inbox
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto.replay import replay_file
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Follow, Like, Record, Relay
from skybridge.pipeline import process_event
from sqlalchemy import select

REMOTE_ACTOR = "https://remote.example/actor"


def _like(actor_id: str, object_id: str, like_id: str) -> dict:
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": like_id,
        "type": "Like",
        "actor": actor_id,
        "object": object_id,
    }


@pytest.fixture
def local_post(settings, fixture_path) -> tuple[str, str, str]:
    """Seed the fixture and return (handle, did, rkey) of a published review post."""
    asyncio.run(replay_file(fixture_path, allow_network=False))
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.review",
                Record.ap_object_json.isnot(None),
            )
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        return actor.handle, rec.did, rec.rkey


def _likes() -> list[Like]:
    with session_scope() as session:
        return list(session.scalars(select(Like)))


def test_like_valid_creates_one_row(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    like = _like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/1")
    status = asyncio.run(inbox.handle_inbox(like))
    assert status == 202
    rows = _likes()
    assert len(rows) == 1
    assert rows[0].object_id == object_id
    assert rows[0].actor_id == REMOTE_ACTOR


def test_like_duplicate_activity_id_is_single_row(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    like = _like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/1")
    asyncio.run(inbox.handle_inbox(like))
    asyncio.run(inbox.handle_inbox(like))
    assert len(_likes()) == 1


def test_like_same_actor_and_object_new_id_is_single_row(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    asyncio.run(inbox.handle_inbox(_like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/1")))
    asyncio.run(inbox.handle_inbox(_like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/2")))
    assert len(_likes()) == 1


def test_like_on_unknown_object_not_stored(settings, local_post):
    status = asyncio.run(
        inbox.handle_inbox(
            _like(REMOTE_ACTOR, "https://elsewhere.example/posts/x", f"{REMOTE_ACTOR}/likes/1")
        )
    )
    assert status == 202
    assert _likes() == []


def test_like_on_deleted_post_not_stored(settings, local_post):
    handle, did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    event = {
        "did": did,
        "kind": "commit",
        "commit": {"operation": "delete", "collection": "social.popfeed.feed.review", "rkey": rkey},
    }
    asyncio.run(process_event(event, allow_network=False))

    status = asyncio.run(
        inbox.handle_inbox(_like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/1"))
    )
    assert status == 202
    assert _likes() == []


def test_undo_like_dict_form_deletes_row(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    like_id = f"{REMOTE_ACTOR}/likes/1"
    asyncio.run(inbox.handle_inbox(_like(REMOTE_ACTOR, object_id, like_id)))

    undo = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{like_id}#undo",
        "type": "Undo",
        "actor": REMOTE_ACTOR,
        "object": {"id": like_id, "type": "Like", "actor": REMOTE_ACTOR, "object": object_id},
    }
    assert asyncio.run(inbox.handle_inbox(undo)) == 202
    assert _likes() == []


def test_undo_like_bare_string_form_deletes_row(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    like_id = f"{REMOTE_ACTOR}/likes/1"
    asyncio.run(inbox.handle_inbox(_like(REMOTE_ACTOR, object_id, like_id)))

    undo = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{like_id}#undo",
        "type": "Undo",
        "actor": REMOTE_ACTOR,
        "object": like_id,
    }
    assert asyncio.run(inbox.handle_inbox(undo)) == 202
    assert _likes() == []


def test_undo_unknown_like_is_noop(settings):
    undo = {
        "type": "Undo",
        "actor": REMOTE_ACTOR,
        "object": f"{REMOTE_ACTOR}/likes/does-not-exist",
    }
    assert asyncio.run(inbox.handle_inbox(undo)) == 202
    assert _likes() == []


def test_like_forwarded_to_accepted_relay(settings, local_post):
    handle, _did, rkey = local_post
    object_id = settings.post_id(handle, rkey)
    with session_scope() as session:
        session.add(Relay(inbox="https://relay.example/inbox", state="accepted"))

    like = _like(REMOTE_ACTOR, object_id, f"{REMOTE_ACTOR}/likes/1")
    worker = DeliveryWorker()  # never started: inspect the queue directly
    assert asyncio.run(inbox.handle_inbox(like, worker=worker)) == 202

    assert worker.queue.qsize() == 1
    task = worker.queue.get_nowait()
    assert task.target_inbox == "https://relay.example/inbox"
    assert task.key_id == f"{settings.relay_actor_id}#main-key"
    assert task.activity == like


def test_follow_of_service_actor_now_rejects(settings, monkeypatch):
    stub_actor = {"id": REMOTE_ACTOR, "inbox": f"{REMOTE_ACTOR.rsplit('/', 1)[0]}/inbox"}
    captured: dict = {}

    async def fake_fetch_actor(actor_id):
        return stub_actor

    async def fake_send_follow_response(
        *, kind, signer_actor, private_pem, follow, inbox, worker=None
    ):
        captured["kind"] = kind
        captured["signer_actor"] = signer_actor

    monkeypatch.setattr(inbox, "fetch_actor", fake_fetch_actor)
    monkeypatch.setattr(inbox, "_send_follow_response", fake_send_follow_response)

    follow = {
        "id": f"{REMOTE_ACTOR}/activities/1",
        "type": "Follow",
        "actor": REMOTE_ACTOR,
        "object": settings.relay_actor_id,
    }
    status = asyncio.run(inbox.handle_inbox(follow, target_actor_id=settings.relay_actor_id))

    assert status == 202
    assert captured["kind"] == "Reject"
    assert captured["signer_actor"] == settings.relay_actor_id
    with session_scope() as session:
        assert session.scalar(select(Relay)) is None
        assert session.scalar(select(Follow)) is None
