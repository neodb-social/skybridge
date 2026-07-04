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


# --- relay key management (env / file, outside the DB) ----------------------


def test_relay_key_minted_to_file_and_stable(settings):
    import os

    from skybridge.activitypub.actors import get_relay_keys

    priv1, pub1 = get_relay_keys()
    assert priv1.startswith("-----BEGIN PRIVATE KEY-----")
    assert os.path.exists(settings.relay_key_file)
    assert oct(os.stat(settings.relay_key_file).st_mode & 0o777) == "0o600"
    # stable across calls: the file is the source of truth
    assert get_relay_keys() == (priv1, pub1)
    with open(settings.relay_key_file) as f:
        assert f.read() == priv1


def test_relay_key_env_pem_wins(settings, tmp_path):
    from dataclasses import replace

    from skybridge.activitypub.actors import get_relay_keys
    from skybridge.config import set_settings

    private_pem, public_pem = generate_keypair()
    set_settings(replace(settings, relay_key_pem=private_pem))
    priv, pub = get_relay_keys()
    assert priv == private_pem
    assert pub == public_pem
    # no key file gets written when the key comes from the environment
    import os

    assert not os.path.exists(settings.relay_key_file)
