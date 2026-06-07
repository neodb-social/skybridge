"""Opt-out: authentication-gated self-service to leave the bridge."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from skybridge import optout
from skybridge.atproto import auth
from skybridge.atproto.replay import replay_file
from skybridge.db import session_scope
from skybridge.main import app
from skybridge.models import BridgedActor, OptOut, Record
from sqlalchemy import func, select

DID = "did:plc:i6k6scfcdaup4e2va33nkprb"  # the author in the fixture


def _active_records(did: str) -> int:
    with session_scope() as session:
        return (
            session.scalar(
                select(func.count())
                .select_from(Record)
                .where(Record.did == did, Record.deleted_at.is_(None))
            )
            or 0
        )


def test_opt_out_tombstones_existing_records(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    assert _active_records(DID) > 0

    purged = asyncio.run(optout.opt_out(DID))
    assert purged > 0
    assert _active_records(DID) == 0
    assert optout.is_opted_out(DID)

    with session_scope() as session:
        actor = session.get(BridgedActor, DID)
        assert actor is not None
        assert actor.opted_out is True
        assert actor.opted_out_at is not None
        # every record now carries a Delete activity referencing a Tombstone
        rows = list(session.scalars(select(Record).where(Record.did == DID)))
        assert rows and all(r.op == "delete" and r.ap_activity_json for r in rows)


def test_opted_out_did_is_skipped_by_pipeline(settings, fixture_path):
    # Opt out BEFORE any data exists; pipeline must never bridge it.
    asyncio.run(optout.opt_out(DID))
    results = asyncio.run(replay_file(fixture_path, allow_network=False))
    assert all(r.activity["actor"].split("/users/")[-1] != DID for r in results)
    with session_scope() as session:
        assert (
            session.scalar(select(func.count()).select_from(Record).where(Record.did == DID)) == 0
        )
        # no actor was minted for the opted-out DID
        assert session.get(BridgedActor, DID) is None


def test_opt_in_clears_optout(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    asyncio.run(optout.opt_out(DID))
    assert optout.is_opted_out(DID)

    assert optout.opt_in(DID) is True
    assert not optout.is_opted_out(DID)
    with session_scope() as session:
        assert session.get(OptOut, DID) is None
        actor = session.get(BridgedActor, DID)
        assert actor is not None and actor.opted_out is False

    # Re-bridging works again after opting back in.
    results = asyncio.run(replay_file(fixture_path, allow_network=False))
    assert any(DID in r.at_uri for r in results)


@pytest.fixture
def client(settings, fixture_path, monkeypatch) -> TestClient:
    asyncio.run(replay_file(fixture_path, allow_network=False))
    # Stub credential verification so the test is fully offline.
    monkeypatch.setattr(
        auth, "verify_credentials", lambda ident, pw: auth.AuthResult(did=DID, handle="author.test")
    )
    # main.py imported `auth` as a module, so patching the attribute covers it.
    return TestClient(app)


def test_optout_endpoint_authenticates_and_purges(client):
    assert _active_records(DID) > 0
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "app_password": "good", "action": "opt-out"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] and body["opted_out"] and body["deleted"] > 0
    assert _active_records(DID) == 0
    assert optout.is_opted_out(DID)


def test_optout_endpoint_rejects_bad_credentials(settings, fixture_path, monkeypatch):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    monkeypatch.setattr(auth, "verify_credentials", lambda ident, pw: None)
    client = TestClient(app)
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "app_password": "wrong", "action": "opt-out"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 401
    assert _active_records(DID) > 0  # nothing purged on auth failure
    assert not optout.is_opted_out(DID)


def test_optout_form_renders(client):
    r = client.get("/optout")
    assert r.status_code == 200
    assert "app password" in r.text.lower()


def test_optout_endpoint_rejects_unknown_action(client):
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "app_password": "good", "action": "purge-everything"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 400
    assert _active_records(DID) > 0  # nothing deleted
    assert not optout.is_opted_out(DID)


def test_status_lookup_by_did(client):
    r = client.get("/optout", params={"q": DID})
    assert r.status_code == 200
    assert DID in r.text
    assert "active record" in r.text


def test_status_lookup_by_handle(client):
    with session_scope() as session:
        actor = session.get(BridgedActor, DID)
        assert actor is not None
        actor.handle = "author.test"
    r = client.get("/optout", params={"q": "@author.test"})
    assert r.status_code == 200
    assert "/users/author.test" in r.text
    assert "active record" in r.text


def test_status_lookup_shows_opted_out(client):
    asyncio.run(optout.opt_out(DID))
    r = client.get("/optout", params={"q": DID})
    assert r.status_code == 200
    assert "opted out" in r.text


def test_status_lookup_unresolvable_handle(client, monkeypatch):
    monkeypatch.setattr(auth, "resolve_did", lambda ident: None)
    r = client.get("/optout", params={"q": "nobody.test"})
    assert r.status_code == 200
    assert "No bridged records" in r.text
    assert "could not be resolved" in r.text


def test_status_lookup_preemptive_optout(client, monkeypatch):
    # Opted out by DID without ever being bridged: lookup by handle must still
    # report it, which requires the network handle->DID resolve (stubbed here).
    other = "did:plc:neverbridged0000000000000"
    asyncio.run(optout.opt_out(other))
    monkeypatch.setattr(auth, "resolve_did", lambda ident: other)
    r = client.get("/optout", params={"q": "somebody.test"})
    assert r.status_code == 200
    assert "opted out" in r.text


def test_optout_post_html_shows_status(client):
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "app_password": "good", "action": "opt-out"},
        headers={"accept": "text/html"},
    )
    assert r.status_code == 200
    assert "record(s) deleted" in r.text  # action message
    assert "opted out" in r.text  # status card reflects the new state


def test_opted_out_actor_is_gone(client):
    handle = "author.test"
    with session_scope() as session:
        # rename the fixture author's handle so we can address it predictably
        actor = session.get(BridgedActor, DID)
        assert actor is not None
        actor.handle = handle
    client.post(
        "/optout",
        data={"identifier": handle, "app_password": "good", "action": "opt-out"},
        headers={"accept": "application/json"},
    )
    r = client.get(f"/users/{handle}", headers={"accept": "application/activity+json"})
    assert r.status_code == 410
    wf = client.get("/.well-known/webfinger", params={"resource": f"acct:{handle}@bridge.test"})
    assert wf.status_code == 404
