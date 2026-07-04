"""HTTP signature sign/verify round-trips."""

from __future__ import annotations

from skybridge.crypto import (
    digest_header,
    generate_keypair,
    http_date,
    sign_request,
    verify_request,
)


def test_post_sign_verify_roundtrip():
    private_pem, public_pem = generate_keypair()
    body = b'{"type":"Create"}'
    key_id = "https://bridge.test/users/alice#main-key"
    date = http_date()
    headers = sign_request(
        private_pem=private_pem,
        key_id=key_id,
        method="POST",
        url="https://remote.example/inbox",
        body=body,
        date=date,
    )
    assert headers["Digest"] == digest_header(body)
    assert 'keyId="' + key_id + '"' in headers["Signature"]

    ok = verify_request(
        public_pem=public_pem,
        method="POST",
        path="/inbox",
        headers=headers,
        body=body,
    )
    assert ok is True


def test_get_sign_verify_roundtrip():
    private_pem, public_pem = generate_keypair()
    headers = sign_request(
        private_pem=private_pem,
        key_id="https://bridge.test/actor#main-key",
        method="GET",
        url="https://remote.example/users/bob",
    )
    assert "Digest" not in headers  # GET does not cover a body
    ok = verify_request(
        public_pem=public_pem,
        method="GET",
        path="/users/bob",
        headers=headers,
    )
    assert ok is True


def test_tampered_body_fails_verification():
    private_pem, public_pem = generate_keypair()
    body = b'{"type":"Create"}'
    headers = sign_request(
        private_pem=private_pem,
        key_id="k#main-key",
        method="POST",
        url="https://remote.example/inbox",
        body=body,
    )
    assert not verify_request(
        public_pem=public_pem,
        method="POST",
        path="/inbox",
        headers=headers,
        body=b'{"type":"Delete"}',
    )


def test_wrong_key_fails_verification():
    private_pem, _ = generate_keypair()
    _, other_public = generate_keypair()
    headers = sign_request(
        private_pem=private_pem,
        key_id="k#main-key",
        method="GET",
        url="https://remote.example/x",
    )
    assert not verify_request(public_pem=other_public, method="GET", path="/x", headers=headers)


# --- relay key management (always operator-provided, never minted) ----------


def test_relay_key_env_pem_wins(settings):
    from skybridge.activitypub.actors import get_relay_keys

    priv, pub = get_relay_keys()
    # the conftest-provided key comes back with its derived public half
    assert priv == settings.relay_key_pem
    assert "BEGIN PUBLIC KEY" in pub
    # no key file gets written when the key comes from the environment
    import os

    assert not os.path.exists(settings.relay_key_file)


def test_relay_key_loaded_from_operator_file(settings):
    from dataclasses import replace

    from skybridge.activitypub.actors import get_relay_keys
    from skybridge.config import set_settings

    private_pem, public_pem = generate_keypair()
    with open(settings.relay_key_file, "w") as f:
        f.write(private_pem)
    set_settings(replace(settings, relay_key_pem=None))
    assert get_relay_keys() == (private_pem, public_pem)


def test_missing_relay_key_fails_loudly(settings):
    from dataclasses import replace

    import pytest
    from skybridge.activitypub.actors import get_relay_keys
    from skybridge.config import set_settings

    set_settings(replace(settings, relay_key_pem=None))
    with pytest.raises(RuntimeError, match="SKYBRIDGE_RELAY_KEY"):
        get_relay_keys()
