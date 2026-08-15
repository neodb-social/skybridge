"""DID resolution: handle/display-name/avatar extraction, offline-safe fallbacks."""

from __future__ import annotations

from collections.abc import Mapping

from skybridge.atproto import identity
from skybridge.models import BridgedActor

DID = "did:plc:test"
PDS = "https://pds.example"
HANDLE = "alice.test"

PLC_DOC = {
    "alsoKnownAs": [f"at://{HANDLE}"],
    "service": [{"id": "#atproto_pds", "serviceEndpoint": PDS}],
}

AVATAR_CID = "bafkreid2bchkp7nddjrm34vkw7nygts5szfdelcq4p4efpuy2dpypcmcmi"
AVATAR_URL = f"{PDS}/xrpc/com.atproto.sync.getBlob?did={DID}&cid={AVATAR_CID}"

BSKY_PROFILE_WITH_AVATAR = {
    "value": {
        "$type": "app.bsky.actor.profile",
        "avatar": {
            "ref": {"$link": AVATAR_CID},
            "size": 26276,
            "$type": "blob",
            "mimeType": "image/png",
        },
        "displayName": "Alice",
    }
}


def _fake_http_json(responses: Mapping[str, dict | None], calls: list[str] | None = None):
    """A stand-in for ``_http_json`` keyed by URL substring.

    Records every requested URL in ``calls`` (if provided) so tests can assert
    network behavior.
    """

    def fake(url: str, timeout: float = 8.0) -> dict | None:
        if calls is not None:
            calls.append(url)
        for substring, value in responses.items():
            if substring in url:
                return value
        raise AssertionError(f"unexpected URL requested in test: {url}")

    return fake


def _actor(ident: str) -> BridgedActor:
    """The bridged actor for a DID or handle; fails the test if there is none."""
    row = identity.actor_by_ident(ident)
    assert row is not None
    return row


def test_rename_actor_keeps_the_retired_handle_resolvable(settings):
    identity.ensure_actor(DID, allow_network=False)
    retired = _actor(DID).handle

    row = identity.rename_actor(DID, HANDLE)

    assert row is not None and row.handle == HANDLE
    assert _actor(HANDLE).did == DID
    # Already-federated actor and object ids carry the old handle forever.
    assert _actor(retired).did == DID


def test_rename_actor_never_mints_an_actor(settings):
    assert identity.rename_actor("did:plc:unknown", HANDLE) is None
    assert identity.actor_by_ident("did:plc:unknown") is None


def test_rename_actor_ignores_an_unchanged_handle(settings):
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)

    assert identity.rename_actor(DID, HANDLE) is None


def test_handle_taken_by_another_did_displaces_the_stale_actor(settings):
    """A handle points at one DID at a time, so the newcomer wins the name.

    Leaving both rows on it would let one account's URL, WebFinger record and
    signature key id resolve to the other's row.
    """
    other = "did:plc:other"
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)
    identity.ensure_actor(other, allow_network=False)

    identity.rename_actor(other, HANDLE)

    assert _actor(HANDLE).did == other
    assert _actor(DID).handle == "test.did"


def test_live_claim_outranks_a_retired_alias(settings):
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)  # "test.did" becomes an alias of DID
    retired = "test.did"

    # Someone else takes the retired name for real.
    newcomer = "did:plc:newcomer"
    identity.ensure_actor(newcomer, allow_network=False)
    identity.rename_actor(newcomer, retired)

    assert _actor(retired).did == newcomer
    # The first actor is still reachable, under the name it holds now.
    assert _actor(HANDLE).did == DID


def test_resolve_remote_falls_back_to_bsky_for_display_name_and_avatar(monkeypatch):
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {
            "value": {"$type": "social.popfeed.actor.profile", "displayName": "", "bannerUrl": ""}
        },
        "collection=app.bsky.actor.profile": BSKY_PROFILE_WITH_AVATAR,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.handle == HANDLE
    assert ident.display_name == "Alice"
    assert ident.avatar == AVATAR_URL


def test_resolve_remote_prefers_popfeed_display_name_over_bsky(monkeypatch):
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {
            "value": {"$type": "social.popfeed.actor.profile", "displayName": "Pop Alice"}
        },
        "collection=app.bsky.actor.profile": BSKY_PROFILE_WITH_AVATAR,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.display_name == "Pop Alice"
    assert ident.avatar == AVATAR_URL


def test_resolve_remote_avatar_none_when_bsky_fetch_fails(monkeypatch):
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {
            "value": {"$type": "social.popfeed.actor.profile", "displayName": ""}
        },
        # Simulates a network failure: _http_json swallows exceptions and
        # returns None, which must never propagate as an exception here.
        "collection=app.bsky.actor.profile": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.avatar is None
    assert ident.handle == HANDLE


def test_resolve_remote_skips_bsky_fetch_when_popfeed_has_everything(monkeypatch):
    calls: list[str] = []
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {
            "value": {
                "$type": "social.popfeed.actor.profile",
                "displayName": "Pop Alice",
                "avatar": {"ref": {"$link": AVATAR_CID}, "$type": "blob"},
            }
        },
        # No entry for app.bsky.actor.profile: the fake raises if it's hit.
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses, calls))

    ident = identity.resolve_remote(DID)

    assert ident.display_name == "Pop Alice"
    assert ident.avatar == AVATAR_URL

    # Verify exactly ONE URL contains "plc.directory" (no duplicate fetch).
    plc_calls = [url for url in calls if "plc.directory" in url]
    assert len(plc_calls) == 1
