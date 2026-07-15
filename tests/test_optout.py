"""Opt-out: authentication-gated self-service to leave the bridge."""

from __future__ import annotations

import asyncio
import re
import time

import pytest
from fastapi.testclient import TestClient
from skybridge import optout, sessions
from skybridge.atproto import backfill, oauth
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
    # author. Actions run afterwards, inside the signed-in session.
    monkeypatch.setattr(
        oauth,
        "finish_flow",
        lambda state, code, iss: oauth.FlowResult(did=DID, handle="author.test"),
    )
    sessions._SESSIONS.clear()  # no leakage between tests
    # https base URL: the session cookie is Secure (settings.scheme is https)
    return TestClient(app, base_url="https://bridge.test")


def _csrf(html: str) -> str:
    m = re.search(r'name="csrf" value="([^"]+)"', html)
    assert m, "csrf token not found in page"
    return m.group(1)


def _sign_in(client: TestClient) -> str:
    """Complete the stubbed OAuth callback; returns the account view's CSRF."""
    r = client.get(
        "/oauth/callback",
        params={"state": "s", "code": "c", "iss": "x"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/optout"
    page = client.get("/optout")
    assert page.status_code == 200
    return _csrf(page.text)


def test_optout_submit_starts_oauth_redirect(client, monkeypatch):
    monkeypatch.setattr(
        oauth,
        "start_flow",
        lambda identifier: oauth.FlowStart("https://as.example/authorize?req=1", "st1"),
    )
    r = client.post(
        "/optout",
        data={"identifier": "author.test"},
        headers={"accept": "text/html"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert r.headers["location"] == "https://as.example/authorize?req=1"
    # JSON callers get the URL instead of a redirect
    r = client.post(
        "/optout",
        data={"identifier": "author.test"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 200
    assert r.json()["authorize_url"] == "https://as.example/authorize?req=1"


def test_optout_submit_rejects_unresolvable_account(client, monkeypatch):
    monkeypatch.setattr(oauth, "start_flow", lambda identifier: None)
    r = client.post(
        "/optout",
        data={"identifier": "nobody.test"},
        headers={"accept": "application/json"},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "oauth_start_failed"


def test_oauth_callback_opens_session_without_changes(client):
    before = _active_records(DID)
    assert before > 0
    _sign_in(client)
    page = client.get("/optout")
    assert "Signed in as" in page.text
    assert DID in page.text  # the signed-in account's status card
    assert "active record" in page.text
    # signing in is purely informational: nothing purged, no opt-out recorded
    assert _active_records(DID) == before
    assert not optout.is_opted_out(DID)


def test_opt_out_and_back_in_via_session(client):
    assert _active_records(DID) > 0
    csrf = _sign_in(client)

    r = client.post("/optout/opt-out", data={"csrf": csrf})
    assert r.status_code == 200
    assert "record(s) deleted" in r.text  # action message
    assert "opted out" in r.text  # status card reflects the new state
    assert _active_records(DID) == 0
    assert optout.is_opted_out(DID)

    r = client.post("/optout/opt-in", data={"csrf": csrf})
    assert r.status_code == 200
    assert "opted back in" in r.text
    assert not optout.is_opted_out(DID)


def test_actions_require_login(client):
    for path in ("/optout/opt-out", "/optout/opt-in"):
        r = client.post(path, data={"csrf": "whatever"})
        assert r.status_code == 401
    assert _active_records(DID) > 0  # nothing purged
    assert not optout.is_opted_out(DID)


def test_actions_require_csrf(client):
    _sign_in(client)
    r = client.post("/optout/opt-out", data={"csrf": "wrong"})
    assert r.status_code == 401
    r = client.post("/optout/opt-out")  # missing entirely
    assert r.status_code == 401
    assert _active_records(DID) > 0
    assert not optout.is_opted_out(DID)


def test_signout_ends_session(client):
    csrf = _sign_in(client)
    r = client.post("/optout/signout", follow_redirects=False)
    assert r.status_code == 303
    page = client.get("/optout")
    assert "Signed in as" not in page.text  # back to the sign-in form
    r = client.post("/optout/opt-out", data={"csrf": csrf})
    assert r.status_code == 401
    assert not optout.is_opted_out(DID)


def test_session_expires(client, monkeypatch):
    csrf = _sign_in(client)
    real_time = time.time
    monkeypatch.setattr(sessions.time, "time", lambda: real_time() + sessions.SESSION_TTL + 1)
    r = client.post("/optout/opt-out", data={"csrf": csrf})
    assert r.status_code == 401
    assert not optout.is_opted_out(DID)


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


def test_status_not_shown_without_login(client):
    # The old unauthenticated ?q= lookup is gone: no way to enumerate what we
    # hold about an account without signing in as it.
    r = client.get("/optout", params={"q": DID})
    assert r.status_code == 200
    assert DID not in r.text
    assert "active record" not in r.text


def test_import_action_starts_backfill(client, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        backfill, "start_import", lambda did, worker=None: calls.append(did) or True
    )
    csrf = _sign_in(client)
    r = client.post("/optout/import", data={"csrf": csrf})
    assert r.status_code == 200
    assert "Importing recent activity" in r.text
    assert calls == [DID]


def test_import_action_already_running(client, monkeypatch):
    monkeypatch.setattr(backfill, "start_import", lambda did, worker=None: False)
    csrf = _sign_in(client)
    r = client.post("/optout/import", data={"csrf": csrf})
    assert r.status_code == 200
    assert "already in progress" in r.text


def test_import_action_refused_when_opted_out(client, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        backfill, "start_import", lambda did, worker=None: calls.append(did) or True
    )
    csrf = _sign_in(client)
    asyncio.run(optout.opt_out(DID))
    r = client.post("/optout/import", data={"csrf": csrf})
    assert r.status_code == 200
    assert "import is disabled" in r.text
    assert calls == []  # server-side guard: never even attempted


def test_import_action_requires_session_and_csrf(client, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(
        backfill, "start_import", lambda did, worker=None: calls.append(did) or True
    )
    r = client.post("/optout/import", data={"csrf": "x"})  # never signed in
    assert r.status_code == 401
    _sign_in(client)
    r = client.post("/optout/import", data={"csrf": "wrong"})  # bad CSRF echo
    assert r.status_code == 401
    assert calls == []


def test_import_button_disabled_only_when_opted_out(client):
    # Signed-in account view for a bridged (not opted out) account: enabled.
    csrf = _sign_in(client)
    page = client.get("/optout")
    assert '<button type="submit">Import recent activity</button>' in page.text
    # After opting out the same view renders the button disabled.
    client.post("/optout/opt-out", data={"csrf": csrf})
    page = client.get("/optout")
    assert '<button type="submit" disabled>Import recent activity</button>' in page.text


def test_opted_out_actor_is_gone(client):
    handle = "author.test"
    with session_scope() as session:
        # rename the fixture author's handle so we can address it predictably
        actor = session.get(BridgedActor, DID)
        assert actor is not None
        actor.handle = handle
    csrf = _sign_in(client)
    client.post("/optout/opt-out", data={"csrf": csrf})
    r = client.get(f"/users/{handle}", headers={"accept": "application/activity+json"})
    assert r.status_code == 410
    wf = client.get("/.well-known/webfinger", params={"resource": f"acct:{handle}@bridge.test"})
    assert wf.status_code == 404
