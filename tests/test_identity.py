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

# How atproto reports a record that is not there: HTTP 400 with an error body.
# Distinct from a stub of ``None``, which stands for a request that never got
# an answer — the difference that decides whether a preference may be cleared.
_NOT_FOUND = {"error": "RecordNotFound", "message": "Could not locate record"}

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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
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
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.refresh_actor(DID, {"displayName": "Alice"}, allow_network=True)

    assert _actor(DID).no_unauthenticated is False


# --------------------------------------------------------------------------- #
# Sign-in resync
# --------------------------------------------------------------------------- #
def _network(monkeypatch, *, handle=HANDLE, display_name="Alice", labels=None, hide=False):
    profile: dict = {"$type": "app.bsky.actor.profile"}
    if display_name is not None:
        profile["displayName"] = display_name
    if labels is not None:
        profile["labels"] = {
            "$type": "com.atproto.label.defs#selfLabels",
            "values": [{"val": v} for v in labels],
        }
    responses = {
        "plc.directory": {
            "alsoKnownAs": [f"at://{handle}"],
            "service": [{"id": "#atproto_pds", "serviceEndpoint": PDS}],
        },
        "collection=social.popfeed.actor.profile": None,
        "collection=app.bsky.actor.profile": {"value": profile},
        "collection=app.bsky.actor.contentVisibilityDeclaration": (
            {"value": {"hideFromAlgorithmicRecommendations": True}} if hide else _NOT_FOUND
        ),
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))


def test_resync_refreshes_profile_and_preferences(settings, monkeypatch):
    identity.ensure_actor(DID, allow_network=False)
    _network(monkeypatch, display_name="New Name", labels=["!no-unauthenticated"], hide=True)

    row = identity.resync_actor(DID)

    assert row is not None  # something moved, so followers get an Update
    assert row.display_name == "New Name"
    assert row.hide_from_recommendations is True
    assert row.no_unauthenticated is True


def test_resync_applies_a_rename_and_keeps_the_old_name_resolvable(settings, monkeypatch):
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, "old.test")
    _network(monkeypatch, handle="new.test")

    row = identity.resync_actor(DID)

    assert row is not None and row.handle == "new.test"
    # Already-federated ids under the retired name keep dereferencing.
    assert identity.actor_by_ident("old.test") is not None


def test_resync_reports_no_change_when_nothing_moved(settings, monkeypatch):
    identity.ensure_actor(DID, allow_network=False)
    _network(monkeypatch, display_name="Alice")
    assert identity.resync_actor(DID) is not None  # first pass applies the name

    # Nothing to tell followers the second time.
    assert identity.resync_actor(DID) is None


def test_resync_may_turn_a_preference_off(settings, monkeypatch):
    """The point of doing this on a sign-in: the settings the user holds right
    now apply, in both directions."""
    identity.ensure_actor(DID, allow_network=False)
    with identity.session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        row.hide_from_recommendations = True
        row.no_unauthenticated = True

    # The repo is readable and neither preference is set in it any more.
    _network(monkeypatch, labels=None, hide=False)
    row = identity.resync_actor(DID)

    assert row is not None
    assert row.hide_from_recommendations is False
    assert row.no_unauthenticated is False


def _preferences_set(monkeypatch) -> None:
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)  # already on the name PLC reports
    with identity.session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        row.hide_from_recommendations = True
        row.no_unauthenticated = True


def test_resync_will_not_turn_a_preference_off_on_a_dead_pds(settings, monkeypatch):
    """Nothing answered, so nothing is believed to have been turned off."""
    _preferences_set(monkeypatch)
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": None,
        "collection=app.bsky.actor.profile": None,
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    assert identity.resync_actor(DID) is None
    row = _actor(DID)
    assert row.hide_from_recommendations is True
    assert row.no_unauthenticated is True


def test_a_failed_declaration_read_does_not_clear_it(settings, monkeypatch):
    """Each preference follows its OWN read.

    The three reads are three separate requests, so the bsky profile
    answering says nothing about a declaration fetch that timed out.
    """
    _preferences_set(monkeypatch)
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.contentVisibilityDeclaration": None,  # timed out
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.resync_actor(DID)

    row = _actor(DID)
    assert row.hide_from_recommendations is True  # untouched
    assert row.no_unauthenticated is False  # its own record answered


def test_a_failed_profile_read_does_not_clear_the_label(settings, monkeypatch):
    """The mirror case: the declaration answered, the label's record did not."""
    _preferences_set(monkeypatch)
    responses = {
        "plc.directory": PLC_DOC,
        "collection=social.popfeed.actor.profile": {"value": {"displayName": "Alice"}},
        "collection=app.bsky.actor.profile": None,  # timed out
        "collection=app.bsky.actor.contentVisibilityDeclaration": _NOT_FOUND,
    }
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    identity.resync_actor(DID)

    row = _actor(DID)
    assert row.no_unauthenticated is True  # untouched
    assert row.hide_from_recommendations is False  # its own record answered


def test_resync_does_not_rename_onto_the_placeholder_when_plc_is_down(settings, monkeypatch):
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)
    responses = {"plc.directory": None}
    monkeypatch.setattr(identity, "_http_json", _fake_http_json(responses))

    assert identity.resync_actor(DID) is None
    assert _actor(DID).handle == HANDLE


def test_resync_never_mints_an_actor(settings, monkeypatch):
    """Signing in is not activity: we bridge people for what they post."""
    _network(monkeypatch)

    assert identity.resync_actor("did:plc:stranger") is None
    assert identity.actor_by_ident("did:plc:stranger") is None


def test_resync_leaves_an_opted_out_account_alone(settings, monkeypatch):
    """Its records were retracted; nobody should hear about its actor again."""
    identity.ensure_actor(DID, allow_network=False)
    with identity.session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        row.opted_out = True
        row.display_name = "Old Name"
    _network(monkeypatch, display_name="New Name")

    assert identity.resync_actor(DID) is None
    assert _actor(DID).display_name == "Old Name"
