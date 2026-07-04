"""DID resolution: handle/display-name/avatar extraction, offline-safe fallbacks."""

from __future__ import annotations

from collections.abc import Mapping

from skybridge.atproto import identity

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
