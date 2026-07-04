"""Auth hardening: identifier syntax checks and PDS endpoint safety (offline)."""

from __future__ import annotations

from skybridge.atproto import auth


def test_is_valid_identifier():
    assert auth.is_valid_identifier("alice.bsky.social")
    assert auth.is_valid_identifier("did:plc:i6k6scfcdaup4e2va33nkprb")
    assert auth.is_valid_identifier("did:web:example.com")
    assert not auth.is_valid_identifier("not a handle!")
    assert not auth.is_valid_identifier("nodots")
    assert not auth.is_valid_identifier("a" * 300 + ".com")
    assert not auth.is_valid_identifier("did:plc:" + "x" * 600)
    assert not auth.is_valid_identifier("")


def test_is_valid_identifier_rejects_special_use_hosts():
    # Handle resolution fetches https://<handle>/..., did:web fetches the
    # embedded host — neither may point at local/special-use names.
    assert not auth.is_valid_identifier("foo.localhost")
    assert not auth.is_valid_identifier("metadata.google.internal")
    assert not auth.is_valid_identifier("printer.local")
    assert not auth.is_valid_identifier("did:web:localhost")
    assert not auth.is_valid_identifier("did:web:127.0.0.1")
    assert not auth.is_valid_identifier("did:web:foo.internal")
    assert not auth.is_valid_identifier("did:web:localhost%3A8443")
    assert not auth.is_valid_identifier("did:web:single-label")
    assert auth.is_valid_identifier("did:web:pds.example.com%3A8443:alice")


def test_invalid_identifiers_rejected_before_any_network():
    # oauth.start_flow gates on is_valid_identifier before resolving
    assert not auth.is_valid_identifier("not a handle!")
    assert not auth.is_valid_identifier("' OR 1=1 --")


def test_is_public_https_rejects_unsafe_endpoints():
    assert not auth._is_public_https("http://pds.example.com")  # not https
    assert not auth._is_public_https("https://127.0.0.1:8443")  # loopback
    assert not auth._is_public_https("https://localhost")  # loopback
    assert not auth._is_public_https("https://10.0.0.1")  # private range
    assert not auth._is_public_https("https://169.254.169.254")  # link-local
    assert not auth._is_public_https("not a url")
