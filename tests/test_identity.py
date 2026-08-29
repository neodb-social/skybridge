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


def test_rename_actor_ignores_the_invalid_handle_placeholder(settings):
    """Every account whose handle stops resolving reports "handle.invalid";
    renaming to it would make them all fight over one actor URL."""
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)

    assert identity.rename_actor(DID, identity.INVALID_HANDLE) is None
    assert _actor(DID).handle == HANDLE


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
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.avatar is None
    assert ident.handle == HANDLE


def test_resolve_remote_still_reads_bsky_when_popfeed_has_everything(monkeypatch):
    """Popfeed values still win, but the bsky record is fetched regardless.

    It carries the ``!no-unauthenticated`` self-label, which has to be known
    even for an author whose popfeed profile supplied both name and avatar.
    """
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
        "collection=app.bsky.actor.profile": {"value": {"displayName": "Bsky Alice"}},
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses, calls))

    ident = identity.resolve_remote(DID)

    assert ident.display_name == "Pop Alice"
    assert ident.avatar == AVATAR_URL
    assert ident.no_unauthenticated is False

    # Verify exactly ONE URL contains "plc.directory" (no duplicate fetch).
    plc_calls = [url for url in calls if "plc.directory" in url]
    assert len(plc_calls) == 1
    assert sum("collection=app.bsky.actor.profile" in url for url in calls) == 1


# --------------------------------------------------------------------------- #
# Bluesky visibility preferences
# --------------------------------------------------------------------------- #
_NO_UNAUTH_PROFILE = {
    "value": {
        "$type": "app.bsky.actor.profile",
        "displayName": "Alice",
        "labels": {
            "$type": "com.atproto.label.defs#selfLabels",
            "values": [{"val": "!no-unauthenticated"}],
        },
    }
}


def test_resolve_remote_reads_both_visibility_preferences(monkeypatch):
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.profile": _NO_UNAUTH_PROFILE,
        "collection=app.bsky.actor.contentVisibilityDeclaration": {
            "value": {"hideFromAlgorithmicRecommendations": True}
        },
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.hide_from_recommendations is True
    assert ident.no_unauthenticated is True


def test_missing_declaration_record_means_false(monkeypatch):
    """The lexicon: "Consumers must treat a missing record as false.\""""
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    ident = identity.resolve_remote(DID)

    assert ident.hide_from_recommendations is False
    assert ident.no_unauthenticated is False


def test_ensure_actor_persists_visibility_preferences(settings, monkeypatch):
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.profile": _NO_UNAUTH_PROFILE,
        "collection=app.bsky.actor.contentVisibilityDeclaration": {
            "value": {"hideFromAlgorithmicRecommendations": True}
        },
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.ensure_actor(DID, allow_network=True)

    row = _actor(DID)
    assert row.hide_from_recommendations is True
    assert row.no_unauthenticated is True


def test_has_self_label_tolerates_malformed_labels():
    assert identity.has_self_label({}, "!no-unauthenticated") is False
    assert identity.has_self_label({"labels": []}, "!no-unauthenticated") is False
    assert identity.has_self_label({"labels": {"values": "x"}}, "!no-unauthenticated") is False
    assert identity.has_self_label({"labels": {"values": [None]}}, "!no-unauthenticated") is False


def test_set_hide_from_recommendations_never_mints_an_actor(settings):
    assert identity.set_hide_from_recommendations("did:plc:unknown", True) is None
    assert identity.actor_by_ident("did:plc:unknown") is None


def test_set_hide_from_recommendations_is_a_no_op_when_unchanged(settings):
    identity.ensure_actor(DID, allow_network=False)

    # Nothing to publish for a value that did not move.
    assert identity.set_hide_from_recommendations(DID, False) is None
    assert identity.set_hide_from_recommendations(DID, True) is not None
    assert identity.set_hide_from_recommendations(DID, True) is None
    assert _actor(DID).hide_from_recommendations is True


def test_refresh_never_clears_a_preference_on_a_failed_fetch(settings, monkeypatch):
    """A fetch that failed and a preference turned off look the same here.

    Both arrive as an empty record, so refresh only ever raises the flags.
    Turning them off has exact signals of its own: the declaration's own
    Jetstream commit, and a profile record that comes back without the label.
    """
    identity.ensure_actor(DID, allow_network=False)
    with identity.session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        row.hide_from_recommendations = True
        row.no_unauthenticated = True

    responses = {
        "plc.directory": PLC_DOC,
        # The PDS is answering for nothing right now.
        "collection=app.bsky.actor.profile": None,
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.refresh_actor(DID, {"displayName": "Alice"}, allow_network=True)

    row = _actor(DID)
    assert row.hide_from_recommendations is True
    assert row.no_unauthenticated is True


def test_refresh_clears_the_label_once_the_profile_comes_back_without_it(settings, monkeypatch):
    identity.ensure_actor(DID, allow_network=False)
    with identity.session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        row.no_unauthenticated = True

    responses = {
        "plc.directory": PLC_DOC,
        "collection=app.bsky.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.refresh_actor(DID, {"displayName": "Alice"}, allow_network=True)

    assert _actor(DID).no_unauthenticated is False
