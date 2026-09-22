"""Regressions for the review fixes: poison records, retraction ids after a
rename, deletes that must not mint, blocking I/O kept off the loop, SSRF
guards, inbox authentication, and the retry schedule."""

from __future__ import annotations

import asyncio
import json
import threading

import pytest
from fastapi.testclient import TestClient
from skybridge import optout, pipeline
from skybridge.activitypub import delivery, inbox
from skybridge.activitypub.delivery import DeliveryWorker, Task
from skybridge.atproto import archive, identity, jetstream
from skybridge.atproto.replay import replay_file
from skybridge.crypto import generate_keypair, sign_request
from skybridge.db import session_scope
from skybridge.main import app
from skybridge.models import BridgedActor, Delivery, Follow, Record
from sqlalchemy import select

from tests.test_identity import _fake_http_json

DID = "did:plc:hardeningtestauthor00000"
REVIEW = "social.popfeed.feed.review"
LIST_ITEM = "social.popfeed.feed.listItem"
FIXTURE_DID = "did:plc:i6k6scfcdaup4e2va33nkprb"  # the author in the fixture

GOOD_REVIEW = {
    "$type": REVIEW,
    "title": "Heat",
    "rating": 8,
    "text": "great",
    "creativeWorkType": "movie",
    "identifiers": {"imdbId": "tt0113277"},
    "createdAt": "2026-09-01T00:00:00Z",
}


def _event(rkey: str, record: dict | None, *, seq: int, op: str = "create", coll: str = REVIEW):
    event = {
        "kind": "commit",
        "did": DID,
        "seq": seq,
        "time": "2026-09-01T00:00:00Z",
        "collection": coll,
        "rkey": rkey,
        "operation": op,
    }
    if record is not None:
        event["record"] = record
    return event


def _run(event, **kw):
    return asyncio.run(pipeline.process_event(event, allow_network=False, **kw))


# --------------------------------------------------------------------------- #
# 1. A record the pipeline cannot process never takes ingestion down
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "label, record, coll",
    [
        ("facets is a string", {**GOOD_REVIEW, "facets": "oops"}, REVIEW),
        ("facet is not an object", {**GOOD_REVIEW, "facets": ["oops", 3]}, REVIEW),
        ("identifiers is a list", {**GOOD_REVIEW, "identifiers": ["tt1"]}, REVIEW),
        ("text is a number", {**GOOD_REVIEW, "text": 5}, REVIEW),
        ("title is an object", {**GOOD_REVIEW, "title": {"x": 1}}, REVIEW),
        ("tags is an object", {**GOOD_REVIEW, "tags": {"a": 1}}, REVIEW),
        (
            "listType is a list",
            {
                "$type": LIST_ITEM,
                "listType": ["watched"],
                "creativeWorkType": "movie",
                "identifiers": {"imdbId": "tt0113277"},
            },
            LIST_ITEM,
        ),
        ("record is not an object", ["not", "a", "record"], REVIEW),
    ],
)
def test_lexicon_invalid_records_are_processed_without_raising(settings, label, record, coll):
    """Anyone can write these into a wanted collection; none may raise."""
    _run(_event("3aaaaaaaaaaaa", record, seq=1, coll=coll))


class _StopLoop(Exception):
    pass


def _v2_commit(seq: int, rkey: str) -> str:
    return json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "did": DID,
                "seq": seq,
                "time": "2026-09-01T00:00:00Z",
                "collection": REVIEW,
                "rkey": rkey,
                "operation": "create",
                "record": GOOD_REVIEW,
            },
        }
    )


def test_live_loop_keeps_reading_past_a_record_the_pipeline_rejects(settings, monkeypatch):
    """The poison event is logged and skipped on the SAME connection; the next
    event is processed and the cursor moves past both."""
    frames = iter([_v2_commit(10, "3poisonpoison"), _v2_commit(11, "3healthyheal1")])
    connections = {"n": 0}
    seen: list[int] = []

    class _Host:
        def __init__(self, url, **kwargs) -> None:
            connections["n"] += 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info) -> bool:
            return False

        def __aiter__(self):
            return self

        async def __anext__(self) -> str:
            try:
                return next(frames)
            except StopIteration:
                raise ConnectionError("no close frame received or sent") from None

    async def fake_process_event(event, **kwargs):
        seq = event["payload"]["seq"]
        seen.append(seq)
        if seq == 10:
            raise AttributeError("'list' object has no attribute 'items'")
        return None

    def stop(low: float, high: float) -> float:
        raise _StopLoop

    monkeypatch.setattr(jetstream.websockets, "connect", _Host)
    monkeypatch.setattr(jetstream, "process_event", fake_process_event)
    monkeypatch.setattr(jetstream, "uniform", stop)
    with pytest.raises(_StopLoop):
        asyncio.run(jetstream.run(DeliveryWorker()))

    assert seen == [10, 11]
    assert connections["n"] == 1  # no reconnect between the two events
    assert jetstream.load_cursor() == 11


def test_archive_apply_skips_a_record_the_pipeline_rejects(settings, monkeypatch):
    calls: list[str] = []

    async def fake_process_event(event, **kwargs):
        calls.append(event["rkey"])
        if event["rkey"] == "bad":
            raise TypeError("boom")
        return pipeline.Processed(f"at://{DID}/{REVIEW}/{event['rkey']}", "create", REVIEW, {})

    monkeypatch.setattr(archive, "process_event", fake_process_event)
    events = [_event("bad", GOOD_REVIEW, seq=1), _event("good", GOOD_REVIEW, seq=2)]
    applied = asyncio.run(archive._apply(events, worker=None, deliver=False))
    assert calls == ["bad", "good"]
    assert applied == 1


# --------------------------------------------------------------------------- #
# 2. Retractions name the id peers received, whatever the handle is now
# --------------------------------------------------------------------------- #
def _published_ids(did: str) -> dict[str, str]:
    """at_uri -> stored Note id for every published, active record of ``did``."""
    with session_scope() as session:
        rows = session.scalars(
            select(Record).where(
                Record.did == did, Record.ap_object_json.isnot(None), Record.deleted_at.is_(None)
            )
        )
        return {r.at_uri: json.loads(r.ap_object_json or "")["id"] for r in rows}


def _retracted_ids(did: str) -> dict[str, str]:
    """at_uri -> the Tombstone id each published record's Delete names."""
    with session_scope() as session:
        rows = session.scalars(
            select(Record).where(Record.did == did, Record.ap_object_json.isnot(None))
        )
        return {
            r.at_uri: json.loads(r.ap_activity_json)["object"]["id"]
            for r in rows
            if r.ap_activity_json and json.loads(r.ap_activity_json)["type"] == "Delete"
        }


def test_opt_out_after_a_rename_retracts_the_published_ids(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    published = _published_ids(FIXTURE_DID)
    assert published

    assert identity.rename_actor(FIXTURE_DID, "renamed.example") is not None
    purged = asyncio.run(optout.opt_out(FIXTURE_DID))

    assert purged == len(published)
    retracted = _retracted_ids(FIXTURE_DID)
    assert retracted == published
    assert not any("renamed.example" in target for target in retracted.values())


def test_account_deletion_after_a_rename_retracts_the_published_ids(settings, fixture_path):
    """Same path as an opt-out (purge_did), same guarantee."""
    asyncio.run(replay_file(fixture_path, allow_network=False))
    published = _published_ids(FIXTURE_DID)
    assert identity.rename_actor(FIXTURE_DID, "renamed.example") is not None

    asyncio.run(optout.purge_did(FIXTURE_DID, mark_opt_out=False))

    assert _retracted_ids(FIXTURE_DID) == published


# --------------------------------------------------------------------------- #
# 3. A delete never mints an actor
# --------------------------------------------------------------------------- #
def _actor(did: str) -> BridgedActor | None:
    with session_scope() as session:
        return session.get(BridgedActor, did)


def test_delete_of_an_unknown_record_from_an_unknown_author_is_ignored(settings):
    result = _run(_event("3neverseen000", None, seq=5, op="delete"))
    assert result is None
    assert _actor(DID) is None


def test_delete_of_an_unknown_record_from_a_known_author_still_retracts(settings):
    """The best-effort Delete for a record we lost track of is kept for an
    author we do bridge — only the minting is gone."""
    _run(_event("3aaaaaaaaaaaa", GOOD_REVIEW, seq=1))
    assert _actor(DID) is not None
    result = _run(_event("3neverseen000", None, seq=5, op="delete"))
    assert result is not None
    assert result.activity["type"] == "Delete"


def test_delete_of_an_archived_list_from_an_unbridged_author_still_tombstones(settings):
    """neodb._fetch_and_archive_list archives another author's list without
    minting them; deleting that list must still tombstone the row."""
    list_uri = f"at://{DID}/social.popfeed.feed.list/3listlistlist"
    with session_scope() as session:
        session.add(
            Record(
                at_uri=list_uri,
                did=DID,
                collection="social.popfeed.feed.list",
                rkey="3listlistlist",
                source_json='{"name": "x"}',
            )
        )
    result = _run(
        _event("3listlistlist", None, seq=5, op="delete", coll="social.popfeed.feed.list")
    )
    assert result is not None
    with session_scope() as session:
        row = session.get(Record, list_uri)
        assert row is not None and row.deleted_at is not None


# --------------------------------------------------------------------------- #
# 4. Identity resolution stays off the event loop
# --------------------------------------------------------------------------- #
def test_new_actor_resolution_runs_off_the_event_loop(settings, monkeypatch):
    seen: dict[str, threading.Thread] = {}
    real = identity.ensure_actor

    def spy(did: str, *, allow_network: bool = True):
        seen["thread"] = threading.current_thread()
        return real(did, allow_network=False)

    monkeypatch.setattr(identity, "ensure_actor", spy)
    asyncio.run(pipeline.process_event(_event("3aaaaaaaaaaaa", GOOD_REVIEW, seq=1)))
    assert seen["thread"] is not threading.main_thread()


def test_profile_refresh_runs_off_the_event_loop(settings, monkeypatch):
    seen: dict[str, threading.Thread] = {}
    _run(_event("3aaaaaaaaaaaa", GOOD_REVIEW, seq=1))
    real = identity.refresh_actor

    def spy(did, record, *, allow_network=True):
        seen["thread"] = threading.current_thread()
        return real(did, record, allow_network=False)

    monkeypatch.setattr(identity, "refresh_actor", spy)
    profile = _event("self", {"displayName": "New"}, seq=2, coll="social.popfeed.actor.profile")
    asyncio.run(pipeline.process_event(profile))
    assert seen["thread"] is not threading.main_thread()


# --------------------------------------------------------------------------- #
# 5. URLs taken from documents we did not write are vetted
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "endpoint",
    [
        "http://pds.example",  # not https
        "https://127.0.0.1:8443",
        "https://169.254.169.254",
        "https://metadata.google.internal",
        "https://pds.localhost",
        "https://localhost",
    ],
)
def test_a_pds_endpoint_must_be_a_public_https_url(monkeypatch, endpoint):
    calls: list[str] = []
    doc = {
        "alsoKnownAs": ["at://alice.example"],
        "service": [{"id": "#atproto_pds", "serviceEndpoint": endpoint}],
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json({"plc.directory": doc}, calls))

    assert identity.resolve_pds("did:plc:x") is None
    ident = identity.resolve_remote("did:plc:x")

    assert ident.handle == "alice.example"  # the handle still resolves
    assert all("plc.directory" in url for url in calls)  # the PDS was never asked


def test_redirects_off_to_a_private_host_are_refused():
    handler = identity._VettedRedirects()
    with pytest.raises(ValueError):
        handler.redirect_request(None, None, 302, "Found", {}, "http://127.0.0.1/x")


def test_fetch_actor_refuses_a_non_public_url(settings):
    for url in ("http://127.0.0.1/actor", "https://10.0.0.1/actor", "https://evil.internal/a"):
        assert asyncio.run(inbox.fetch_actor(url)) is None


def test_private_inbox_urls_are_dropped(settings):
    doc = {"inbox": "http://10.0.0.1/inbox", "endpoints": {"sharedInbox": "https://ok.example/i"}}
    assert inbox._inboxes(doc) == ("", "https://ok.example/i")


# --------------------------------------------------------------------------- #
# 6. Inbox authentication
# --------------------------------------------------------------------------- #
REMOTE = "https://remote.example/users/bob"
REMOTE_KEY_ID = f"{REMOTE}#main-key"


@pytest.fixture
def remote(monkeypatch):
    """A remote actor with a keypair; fetch_actor serves its document."""
    inbox.reset_key_cache()
    private_pem, public_pem = generate_keypair()
    doc = {
        "id": REMOTE,
        "type": "Person",
        "inbox": f"{REMOTE}/inbox",
        "publicKey": {"id": REMOTE_KEY_ID, "owner": REMOTE, "publicKeyPem": public_pem},
    }

    async def fake_fetch_actor(actor_id: str):
        return doc if actor_id == REMOTE else None

    monkeypatch.setattr(inbox, "fetch_actor", fake_fetch_actor)
    yield {"private_pem": private_pem, "doc": doc}
    inbox.reset_key_cache()


@pytest.fixture
def followed(settings, fixture_path) -> tuple[TestClient, str, str]:
    """The fixture author, followed by REMOTE; returns (client, handle, did)."""
    asyncio.run(replay_file(fixture_path, allow_network=False))
    with session_scope() as session:
        actor = session.get(BridgedActor, FIXTURE_DID)
        assert actor is not None
        handle = actor.handle
        session.add(
            Follow(
                local_did=FIXTURE_DID, follower_actor_id=REMOTE, follower_inbox=f"{REMOTE}/inbox"
            )
        )
    return TestClient(app), handle, FIXTURE_DID


def _follow_row(did: str) -> Follow | None:
    with session_scope() as session:
        return session.scalar(
            select(Follow).where(Follow.local_did == did, Follow.follower_actor_id == REMOTE)
        )


def _undo_follow(handle: str) -> dict:
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{REMOTE}/undo/1",
        "type": "Undo",
        "actor": REMOTE,
        "object": {
            "type": "Follow",
            "actor": REMOTE,
            "object": f"https://bridge.test/users/{handle}",
        },
    }


def _signed(private_pem: str, key_id: str, path: str, activity: dict) -> tuple[bytes, dict]:
    body = json.dumps(activity).encode()
    headers = sign_request(
        private_pem=private_pem,
        key_id=key_id,
        method="POST",
        url=f"http://testserver{path}",
        body=body,
    )
    return body, headers


def test_an_unsigned_undo_follow_is_refused(followed, remote):
    client, handle, did = followed
    resp = client.post(f"/users/{handle}/inbox", json=_undo_follow(handle))
    assert resp.status_code == 401
    assert _follow_row(did) is not None


def test_a_signed_undo_follow_is_honoured(followed, remote):
    client, handle, did = followed
    path = f"/users/{handle}/inbox"
    body, headers = _signed(remote["private_pem"], REMOTE_KEY_ID, path, _undo_follow(handle))
    resp = client.post(path, content=body, headers=headers)
    assert resp.status_code == 202
    assert _follow_row(did) is None


def test_a_signature_by_a_key_the_actor_does_not_own_is_refused(followed, remote):
    """A valid signature from some other account must not act as the victim."""
    client, handle, did = followed
    path = f"/users/{handle}/inbox"
    undo = {**_undo_follow(handle), "actor": "https://remote.example/users/victim"}
    body, headers = _signed(remote["private_pem"], REMOTE_KEY_ID, path, undo)
    resp = client.post(path, content=body, headers=headers)
    assert resp.status_code == 401
    assert _follow_row(did) is not None


def test_a_signature_over_a_different_body_is_refused(followed, remote):
    client, handle, did = followed
    path = f"/users/{handle}/inbox"
    _, headers = _signed(remote["private_pem"], REMOTE_KEY_ID, path, {"type": "Like"})
    body = json.dumps(_undo_follow(handle)).encode()
    resp = client.post(path, content=body, headers=headers)
    assert resp.status_code == 401
    assert _follow_row(did) is not None


def test_an_unsigned_create_is_still_acknowledged(followed, remote):
    """Nothing is done with a Create, so a relay forwarding one under its own
    key must not be 401ed."""
    client, _handle, _did = followed
    create = {"type": "Create", "actor": "https://elsewhere.example/u", "object": {"type": "Note"}}
    assert client.post("/inbox", json=create).status_code == 202


def test_malformed_inbox_bodies_are_400(followed):
    client, handle, _did = followed
    assert client.post("/inbox", content=b"{not json").status_code == 400
    assert client.post("/inbox", json=["not", "an", "object"]).status_code == 400
    assert client.post(f"/users/{handle}/inbox", content=b"").status_code == 400


def test_key_document_shapes(remote):
    doc = remote["doc"]
    assert inbox._key_from_doc(doc, REMOTE_KEY_ID) == (REMOTE, doc["publicKey"]["publicKeyPem"])
    bare = {"id": REMOTE_KEY_ID, "owner": REMOTE, "publicKeyPem": "PEM"}
    assert inbox._key_from_doc(bare, REMOTE_KEY_ID) == (REMOTE, "PEM")
    many = {"id": REMOTE, "publicKey": [{"id": "x#k"}, {"id": REMOTE_KEY_ID, "publicKeyPem": "P"}]}
    assert inbox._key_from_doc(many, REMOTE_KEY_ID) == (REMOTE, "P")
    assert inbox._key_from_doc({"id": REMOTE}, REMOTE_KEY_ID) is None


def test_follow_at_an_opted_out_actors_own_inbox_is_gone(settings, fixture_path, monkeypatch):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    asyncio.run(optout.opt_out(FIXTURE_DID))
    actor = _actor(FIXTURE_DID)
    assert actor is not None
    handle = actor.handle

    async def fake_fetch_actor(actor_id):
        return {"id": REMOTE, "inbox": f"{REMOTE}/inbox"}

    monkeypatch.setattr(inbox, "fetch_actor", fake_fetch_actor)
    follow = {"type": "Follow", "actor": REMOTE, "object": f"https://bridge.test/users/{handle}"}
    status = asyncio.run(
        inbox.handle_inbox(follow, target_actor_id=f"https://bridge.test/users/{handle}")
    )
    assert status == 410
    assert _follow_row(FIXTURE_DID) is None


# --------------------------------------------------------------------------- #
# 7. Every entry of the retry schedule is used
# --------------------------------------------------------------------------- #
def test_every_backoff_entry_buys_one_retry(settings, monkeypatch):
    async def failing_post(client, *, inbox, key_id, private_pem, body):
        return False, 503

    async def no_sleep(delay):
        return None

    monkeypatch.setattr(delivery, "post_signed", failing_post)
    monkeypatch.setattr(delivery.asyncio, "sleep", no_sleep)
    private_pem, _ = generate_keypair()

    async def go() -> None:
        worker = DeliveryWorker()
        worker.start()
        await worker.enqueue(
            Task(
                "at://x/y/z",
                "https://peer.example/inbox",
                "k#main-key",
                private_pem,
                {"type": "Create"},
            )
        )
        await worker.drain()
        await worker.stop()

    asyncio.run(go())
    with session_scope() as session:
        row = session.scalar(select(Delivery))
        assert row is not None
        assert row.attempts == len(settings.retry_backoff) + 1
        assert row.status == "failed"
