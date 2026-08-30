"""The operator gate: who sees the admin view, and who may drive an import."""

from __future__ import annotations

import asyncio
import re
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient
from skybridge import admin, sessions
from skybridge.atproto import identity, oauth
from skybridge.atproto.replay import replay_file
from skybridge.config import Settings, set_settings
from skybridge.main import app

DID = "did:plc:i6k6scfcdaup4e2va33nkprb"  # the fixture author
OTHER_DID = "did:plc:someoneelse00000000000"

ADMIN_ROUTES = (
    "/manage/admin/import",
    "/manage/admin/import/dry-run",
    "/manage/admin/import/cancel",
)


@pytest.fixture
def client(settings: Settings, fixture_path, monkeypatch) -> TestClient:
    asyncio.run(replay_file(fixture_path, allow_network=False))
    monkeypatch.setattr(
        oauth,
        "finish_flow",
        lambda state, code, iss: oauth.FlowResult(did=DID, handle="author.test"),
    )
    # Signing in re-reads the account from the network (identity.resync_actor).
    # Keep the suite offline: an unreachable PLC/PDS is the resolver's normal
    # degraded path and leaves the actor untouched.
    monkeypatch.setattr(identity, "_http_json", lambda url, timeout=8.0: None)
    sessions._SESSIONS.clear()
    admin.reset_cache()
    return TestClient(app, base_url="https://bridge.test")


def _sign_in(client: TestClient) -> str:
    client.get("/oauth/callback", params={"state": "s", "code": "c"}, follow_redirects=False)
    page = client.get("/manage")
    match = re.search(r'name="csrf" value="([^"]+)"', page.text)
    assert match
    return match.group(1)


def _set_admins(settings: Settings, *entries: str, warm: bool = True) -> None:
    """Install the admin list, optionally warming the resolved-DID cache.

    `is_admin` never resolves inline — that would block the event loop inside
    a request — so handle entries only take effect once `refresh()` has run,
    which the app does from its lifespan. DID entries need no warming.
    """
    set_settings(replace(settings, admins=entries))
    admin.reset_cache()
    if warm:
        asyncio.run(admin.refresh())


def test_no_admins_configured_means_no_admin_view(client, settings):
    _sign_in(client)
    assert "Operator" not in client.get("/manage").text


def test_did_entry_grants_access(client, settings):
    _set_admins(settings, DID)
    _sign_in(client)
    page = client.get("/manage").text
    assert "Operator" in page
    assert "Start import" in page


def test_handle_entries_need_a_refresh_before_they_grant_access(client, settings, monkeypatch):
    """is_admin must not resolve inline, so an unwarmed handle grants nothing."""
    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", lambda ident: (DID, "https://pds.test"))
    _set_admins(settings, "operator.example.com", warm=False)
    _sign_in(client)
    assert "Operator" not in client.get("/manage").text
    asyncio.run(admin.refresh())
    assert "Operator" in client.get("/manage").text


def test_handle_entry_is_resolved_to_a_did(client, settings, monkeypatch):
    """Entries may be handles, but the comparison happens on the DID."""
    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", lambda ident: (DID, "https://pds.test"))
    _set_admins(settings, "operator.example.com")
    _sign_in(client)
    assert "Operator" in client.get("/manage").text


def test_a_handle_resolving_elsewhere_grants_nothing(client, settings, monkeypatch):
    """Holding the *name* is not enough; the signed-in DID must match."""
    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", lambda ident: (OTHER_DID, "https://x"))
    _set_admins(settings, "operator.example.com")
    _sign_in(client)
    assert "Operator" not in client.get("/manage").text


def test_typed_handle_cannot_stand_in_for_the_verified_did(client, settings, monkeypatch):
    """session.handle is user-typed input (oauth.start_flow), so a match on it
    must not grant access when the verified DID is somebody else's."""
    monkeypatch.setattr(
        oauth,
        "finish_flow",
        lambda state, code, iss: oauth.FlowResult(did=OTHER_DID, handle="admin.example.com"),
    )
    _set_admins(settings, "admin.example.com")
    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", lambda ident: (DID, "https://pds.test"))
    _sign_in(client)
    assert "Operator" not in client.get("/manage").text


def test_unresolvable_admin_entry_grants_nothing(client, settings, monkeypatch):
    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", lambda ident: (None, None))
    _set_admins(settings, "typo.example.com")
    _sign_in(client)
    assert "Operator" not in client.get("/manage").text


@pytest.mark.parametrize("path", ADMIN_ROUTES)
def test_admin_routes_reject_a_non_admin_session(client, settings, path):
    csrf = _sign_in(client)
    assert client.post(path, data={"csrf": csrf}).status_code == 403


@pytest.mark.parametrize("path", ADMIN_ROUTES)
def test_admin_routes_reject_a_bad_csrf_token(client, settings, path):
    _set_admins(settings, DID)
    _sign_in(client)
    assert client.post(path, data={"csrf": "wrong"}).status_code == 403


@pytest.mark.parametrize("path", ADMIN_ROUTES)
def test_admin_routes_reject_anonymous_callers(client, settings, path):
    _set_admins(settings, DID)
    assert client.post(path, data={"csrf": "whatever"}).status_code == 403


def test_import_refuses_without_an_api_key(client, settings):
    """The live tail needs no key, but the metered HTTP archive does."""
    _set_admins(settings, DID)
    csrf = _sign_in(client)
    page = client.post("/manage/admin/import", data={"csrf": csrf})
    assert page.status_code == 200
    assert "SKYBRIDGE_JETSTREAM_API_KEY" in page.text


def test_cancel_reports_when_nothing_is_running(client, settings):
    _set_admins(settings, DID)
    csrf = _sign_in(client)
    page = client.post("/manage/admin/import/cancel", data={"csrf": csrf})
    assert "No archive import is running" in page.text


def test_admin_entries_are_normalized(client, settings, monkeypatch):
    """A leading @ or odd casing must still match, as on the sign-in form."""
    seen: list[str] = []

    def resolve(ident: str):
        seen.append(ident)
        return DID, "https://pds.test"

    monkeypatch.setattr(admin.auth, "is_valid_identifier", lambda ident: True)
    monkeypatch.setattr(admin.auth, "_resolve_identity", resolve)
    _set_admins(settings, "@Operator.Example.COM")
    _sign_in(client)
    assert "Operator" in client.get("/manage").text
    assert seen == ["operator.example.com"]
