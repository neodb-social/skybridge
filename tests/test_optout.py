"""Opt-out: authentication-gated self-service to leave the bridge."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from skybridge import optout
from skybridge.atproto import auth, oauth
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
        # every record is tombstoned; the ones that had been published to AP
        # carry a Delete activity referencing a Tombstone (archive-only rows
        # were never federated, so there is nothing to retract for them)
        rows = list(session.scalars(select(Record).where(Record.did == DID)))
        assert rows and all(r.op == "delete" for r in rows)
        published = [r for r in rows if r.ap_object_json]
        assert published and all(r.ap_activity_json for r in published)


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
    # Complete the OAuth flow offline: any callback verifies as the fixture
    # author, with the requested action smuggled through the state param.
    monkeypatch.setattr(
        oauth,
        "finish_flow",
        lambda state, code, iss: oauth.FlowResult(did=DID, handle="author.test", action=state),
    )
    return TestClient(app)


def test_optout_submit_starts_oauth_redirect(client, monkeypatch):
    monkeypatch.setattr(
        oauth,
        "start_flow",
        lambda identifier, action: oauth.FlowStart("https://as.example/authorize?req=1", "st1"),
    )
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "action": "opt-out"},
        headers={"accept": "text/html"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"] == "https://as.example/authorize?req=1"
    # JSON callers get the URL instead of a redirect
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "action": "opt-out"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 200
    assert r.json()["authorize_url"] == "https://as.example/authorize?req=1"


def test_optout_submit_rejects_unresolvable_account(client, monkeypatch):
    monkeypatch.setattr(oauth, "start_flow", lambda identifier, action: None)
    r = client.post(
        "/optout",
        data={"identifier": "nobody.test", "action": "opt-out"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "oauth_start_failed"


def test_oauth_callback_executes_optout(client):
    assert _active_records(DID) > 0
    r = client.get("/oauth/callback", params={"state": "opt-out", "code": "c", "iss": "x"})
    assert r.status_code == 200
    assert "record(s) deleted" in r.text  # action message
    assert "opted out" in r.text  # status card reflects the new state
    assert _active_records(DID) == 0
    assert optout.is_opted_out(DID)


def test_oauth_callback_with_unknown_flow_rejected(settings, fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    client = TestClient(app)  # no stub: the real finish_flow has no such state
    r = client.get("/oauth/callback", params={"state": "bogus", "code": "c", "iss": "x"})
    assert r.status_code == 400
    assert _active_records(DID) > 0  # nothing purged
    assert not optout.is_opted_out(DID)


def test_oauth_callback_denied_by_user(client):
    r = client.get("/oauth/callback", params={"state": "s", "error": "access_denied"})
    assert r.status_code == 400
    assert _active_records(DID) > 0


def test_optout_form_renders(client):
    r = client.get("/optout")
    assert r.status_code == 200
    assert "sign in" in r.text.lower()
    assert "app password" not in r.text.lower()


def test_client_metadata_endpoint(client, settings):
    r = client.get("/oauth/client-metadata.json")
    assert r.status_code == 200
    assert r.json()["client_id"] == settings.url("oauth/client-metadata.json")


def test_optout_endpoint_rejects_unknown_action(client):
    r = client.post(
        "/optout",
        data={"identifier": "author.test", "action": "purge-everything"},
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


def test_opted_out_actor_is_gone(client):
    handle = "author.test"
    with session_scope() as session:
        # rename the fixture author's handle so we can address it predictably
        actor = session.get(BridgedActor, DID)
        assert actor is not None
        actor.handle = handle
    client.get("/oauth/callback", params={"state": "opt-out", "code": "c", "iss": "x"})
    r = client.get(f"/users/{handle}", headers={"accept": "application/activity+json"})
    assert r.status_code == 410
    wf = client.get("/.well-known/webfinger", params={"resource": f"acct:{handle}@bridge.test"})
    assert wf.status_code == 404
