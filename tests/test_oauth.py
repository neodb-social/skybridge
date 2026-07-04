"""AT Protocol OAuth client: discovery, PAR, DPoP and code exchange (offline)."""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from skybridge.atproto import auth, oauth

ISSUER = "https://auth.example.com"
PDS = "https://pds.example.com"
DID = "did:plc:oauthtestuser0000000000"


def _b64d(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


@pytest.fixture
def oauth_env(settings, monkeypatch):
    """Offline OAuth world: stubbed identity resolution + a mock issuer."""
    monkeypatch.setattr(auth, "_resolve_identity", lambda i: (DID, PDS))
    monkeypatch.setattr(auth, "_is_public_https", lambda url: url.startswith("https://"))
    env: dict[str, Any] = {"par_calls": 0, "token_data": None, "sub": DID}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{PDS}/.well-known/oauth-protected-resource":
            return httpx.Response(200, json={"authorization_servers": [ISSUER]})
        if url == f"{ISSUER}/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": ISSUER,
                    "authorization_endpoint": f"{ISSUER}/authorize",
                    "token_endpoint": f"{ISSUER}/token",
                    "pushed_authorization_request_endpoint": f"{ISSUER}/par",
                },
            )
        if url == f"{ISSUER}/par":
            env["par_calls"] += 1
            assert "DPoP" in request.headers
            if env["par_calls"] == 1:
                # first attempt must be retried with the server nonce
                return httpx.Response(
                    400, json={"error": "use_dpop_nonce"}, headers={"DPoP-Nonce": "n0nce"}
                )
            payload = json.loads(_b64d(request.headers["DPoP"].split(".")[1]))
            assert payload["nonce"] == "n0nce"
            return httpx.Response(201, json={"request_uri": "urn:ietf:params:oauth:request_uri:x"})
        if url == f"{ISSUER}/token":
            assert "DPoP" in request.headers
            env["token_data"] = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
            return httpx.Response(
                200, json={"sub": env["sub"], "access_token": "at", "token_type": "DPoP"}
            )
        return httpx.Response(404)

    monkeypatch.setattr(oauth, "_transport", httpx.MockTransport(handler))
    return env


def test_full_flow_roundtrip(oauth_env, settings):
    flow = oauth.start_flow("alice.example.com", "opt-out")
    assert flow is not None
    assert flow.authorize_url.startswith(f"{ISSUER}/authorize?")
    query = {k: v[0] for k, v in parse_qs(urlsplit(flow.authorize_url).query).items()}
    assert query["client_id"] == settings.url("oauth/client-metadata.json")
    assert query["request_uri"] == "urn:ietf:params:oauth:request_uri:x"

    result = oauth.finish_flow(flow.state, "code123", ISSUER)
    assert result is not None
    assert (result.did, result.handle, result.action) == (DID, "alice.example.com", "opt-out")
    # the code exchange carried the PKCE verifier + our client_id
    assert oauth_env["token_data"]["code"] == "code123"
    assert oauth_env["token_data"]["client_id"] == query["client_id"]
    assert oauth_env["token_data"]["code_verifier"]


def test_flow_state_is_single_use(oauth_env):
    flow = oauth.start_flow("alice.example.com", "opt-out")
    assert flow is not None
    assert oauth.finish_flow(flow.state, "c", ISSUER) is not None
    assert oauth.finish_flow(flow.state, "c", ISSUER) is None  # replay rejected


def test_issuer_mismatch_rejected(oauth_env):
    flow = oauth.start_flow("alice.example.com", "opt-out")
    assert flow is not None
    assert oauth.finish_flow(flow.state, "c", "https://evil.example.com") is None
    # the flow is consumed even on failure
    assert oauth.finish_flow(flow.state, "c", ISSUER) is None


def test_token_sub_must_match_started_did(oauth_env):
    flow = oauth.start_flow("alice.example.com", "opt-out")
    assert flow is not None
    oauth_env["sub"] = "did:plc:someoneelse000000000000"
    assert oauth.finish_flow(flow.state, "c", ISSUER) is None


def test_unknown_state_rejected(oauth_env):
    assert oauth.finish_flow("no-such-state", "c", ISSUER) is None


def test_client_metadata_shape(settings):
    meta = oauth.client_metadata()
    assert meta["client_id"] == settings.url("oauth/client-metadata.json")
    assert meta["redirect_uris"] == [settings.url("oauth/callback")]
    assert meta["scope"] == "atproto"
    assert meta["token_endpoint_auth_method"] == "none"
    assert meta["dpop_bound_access_tokens"] is True
    assert meta["grant_types"] == ["authorization_code"]


def test_dpop_proof_is_valid_es256(settings):
    key_pem = oauth._generate_dpop_key_pem()
    proof = oauth.dpop_proof(key_pem, "post", "https://as.example/token?x=1#frag", nonce="abc")
    h64, p64, s64 = proof.split(".")
    header = json.loads(_b64d(h64))
    payload = json.loads(_b64d(p64))
    assert header["typ"] == "dpop+jwt" and header["alg"] == "ES256"
    assert header["jwk"]["kty"] == "EC" and header["jwk"]["crv"] == "P-256"
    assert payload["htm"] == "POST"
    assert payload["htu"] == "https://as.example/token"  # query+fragment stripped
    assert payload["nonce"] == "abc"
    # signature verifies against the embedded JWK
    x = int.from_bytes(_b64d(header["jwk"]["x"]), "big")
    y = int.from_bytes(_b64d(header["jwk"]["y"]), "big")
    pub = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1()).public_key()
    raw = _b64d(s64)
    der = encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big"))
    pub.verify(der, f"{h64}.{p64}".encode(), ec.ECDSA(hashes.SHA256()))


def test_metadata_issuer_mismatch_rejected(settings, monkeypatch):
    monkeypatch.setattr(auth, "_is_public_https", lambda url: url.startswith("https://"))

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/.well-known/oauth-protected-resource"):
            return httpx.Response(200, json={"authorization_servers": [ISSUER]})
        if url.endswith("/.well-known/oauth-authorization-server"):
            return httpx.Response(200, json={"issuer": "https://other.example.com"})
        return httpx.Response(404)

    monkeypatch.setattr(oauth, "_transport", httpx.MockTransport(handler))
    assert oauth.resolve_auth_server(PDS) is None


def test_loopback_client_for_local_dev(settings):
    from dataclasses import replace

    from skybridge.config import set_settings

    set_settings(replace(settings, domain="localhost:8000", scheme="http"))
    # RFC 8252: redirect must use a loopback IP, never the "localhost" name
    assert oauth.redirect_uri() == "http://127.0.0.1:8000/oauth/callback"
    cid = oauth.client_id()
    assert cid.startswith("http://localhost?")
    assert "redirect_uri=http%3A%2F%2F127.0.0.1%3A8000%2Foauth%2Fcallback" in cid
    assert "scope=atproto" in cid


def test_hosted_client_id_for_https(settings):
    assert oauth.client_id() == settings.url("oauth/client-metadata.json")
    assert oauth.redirect_uri() == settings.url("oauth/callback")
