"""Minimal AT Protocol OAuth client for the opt-out flow.

We only need to prove the caller controls a DID: run the atproto flavor of
the OAuth authorization-code flow (https://atproto.com/specs/oauth) and read
the authenticated DID (``sub``) off the token response — the tokens
themselves are discarded immediately.

Implemented pieces: authorization-server discovery from the user's PDS,
pushed authorization requests (PAR), PKCE (S256), and DPoP-bound requests
(ES256, with server-nonce retry) as a *public* client whose ``client_id`` is
the hosted client-metadata document.

Pending flows are held in memory (single-process server); they are
single-use and expire after ``_FLOW_TTL`` seconds.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from skybridge.atproto import auth
from skybridge.config import get_settings

log = logging.getLogger("skybridge.oauth")

_TIMEOUT = 10.0
_FLOW_TTL = 600.0  # seconds a pending authorization may take
_FLOWS_MAX = 1024

# Test hook: unit tests install an httpx.MockTransport here to run offline.
_transport: httpx.BaseTransport | None = None


def _client() -> httpx.Client:
    return httpx.Client(timeout=_TIMEOUT, follow_redirects=False, transport=_transport)


# --------------------------------------------------------------------------- #
# Client identity (a public client: client_id IS the metadata URL)
# --------------------------------------------------------------------------- #
def redirect_uri() -> str:
    uri = get_settings().url("oauth/callback")
    # RFC 8252: loopback redirects must use an IP literal, not "localhost" —
    # authorization servers reject the hostname form outright.
    return uri.replace("://localhost", "://127.0.0.1", 1)


def client_id() -> str:
    settings = get_settings()
    if settings.scheme == "https":
        return settings.url("oauth/client-metadata.json")
    # Local development: hosted client metadata requires https, so use the
    # atproto "loopback client" form — the authorization server synthesizes
    # the metadata from the query parameters.
    return "http://localhost?" + urlencode({"redirect_uri": redirect_uri(), "scope": "atproto"})


def client_metadata() -> dict:
    settings = get_settings()
    return {
        "client_id": client_id(),
        "client_name": settings.relay_name,
        "client_uri": settings.base_url,
        "application_type": "web",
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "redirect_uris": [redirect_uri()],
        "scope": "atproto",
        "token_endpoint_auth_method": "none",
        "dpop_bound_access_tokens": True,
    }


# --------------------------------------------------------------------------- #
# DPoP (ES256)
# --------------------------------------------------------------------------- #
def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _generate_dpop_key_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _load_dpop_key(pem: str) -> ec.EllipticCurvePrivateKey:
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    assert isinstance(key, ec.EllipticCurvePrivateKey)
    return key


def _jwk(key: ec.EllipticCurvePrivateKey) -> dict:
    numbers = key.public_key().public_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _b64url(numbers.x.to_bytes(32, "big")),
        "y": _b64url(numbers.y.to_bytes(32, "big")),
    }


def _htu(url: str) -> str:
    """DPoP ``htu``: the request URL without query or fragment."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def dpop_proof(key_pem: str, method: str, url: str, nonce: str | None = None) -> str:
    key = _load_dpop_key(key_pem)
    header = {"typ": "dpop+jwt", "alg": "ES256", "jwk": _jwk(key)}
    payload: dict = {
        "jti": secrets.token_urlsafe(16),
        "htm": method.upper(),
        "htu": _htu(url),
        "iat": int(time.time()),
    }
    if nonce:
        payload["nonce"] = nonce
    signing_input = (
        _b64url(json.dumps(header, separators=(",", ":")).encode())
        + "."
        + _b64url(json.dumps(payload, separators=(",", ":")).encode())
    )
    der = key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der)
    signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return signing_input + "." + _b64url(signature)


def _post_with_dpop(client: httpx.Client, url: str, data: dict, key_pem: str) -> httpx.Response:
    """POST with a DPoP proof, retrying once with the server-issued nonce."""
    resp = client.post(url, data=data, headers={"DPoP": dpop_proof(key_pem, "POST", url)})
    nonce = resp.headers.get("DPoP-Nonce")
    if resp.status_code in (400, 401) and nonce:
        resp = client.post(
            url, data=data, headers={"DPoP": dpop_proof(key_pem, "POST", url, nonce=nonce)}
        )
    return resp


# --------------------------------------------------------------------------- #
# Authorization-server discovery
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AuthServer:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    par_endpoint: str


def resolve_auth_server(pds: str) -> AuthServer | None:
    """PDS -> its authorization server's metadata, SSRF-guarded."""
    try:
        with _client() as client:
            resource = client.get(f"{pds}/.well-known/oauth-protected-resource")
            resource.raise_for_status()
            servers = resource.json().get("authorization_servers") or []
            issuer = servers[0] if servers else None
            if not issuer or not auth._is_public_https(issuer):
                log.warning("no public authorization server for pds %s", pds)
                return None
            issuer = issuer.rstrip("/")
            meta_resp = client.get(f"{issuer}/.well-known/oauth-authorization-server")
            meta_resp.raise_for_status()
            meta = meta_resp.json()
    except Exception as exc:
        log.info("auth server discovery failed for %s: %s", pds, type(exc).__name__)
        return None
    if meta.get("issuer", "").rstrip("/") != issuer:
        log.warning("issuer mismatch in metadata for %s", issuer)
        return None
    endpoints = (
        meta.get("authorization_endpoint"),
        meta.get("token_endpoint"),
        meta.get("pushed_authorization_request_endpoint"),
    )
    if not all(e and auth._is_public_https(e) for e in endpoints):
        log.warning("non-public oauth endpoints for %s", issuer)
        return None
    return AuthServer(issuer, *endpoints)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Pending flows (state -> everything needed to finish)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PendingFlow:
    did: str
    handle: str
    action: str
    issuer: str
    token_endpoint: str
    code_verifier: str
    dpop_key_pem: str
    expires_at: float


_FLOWS: dict[str, PendingFlow] = {}


def _put_flow(state: str, flow: PendingFlow) -> None:
    now = time.time()
    for key in [k for k, f in _FLOWS.items() if f.expires_at < now]:
        _FLOWS.pop(key, None)
    if len(_FLOWS) >= _FLOWS_MAX:
        _FLOWS.clear()  # under abuse, drop pending flows rather than memory
    _FLOWS[state] = flow


def _pop_flow(state: str) -> PendingFlow | None:
    flow = _FLOWS.pop(state, None)
    if flow is None or flow.expires_at < time.time():
        return None
    return flow


# --------------------------------------------------------------------------- #
# The flow
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FlowStart:
    authorize_url: str
    state: str


@dataclass(frozen=True)
class FlowResult:
    did: str
    handle: str
    action: str


def start_flow(identifier: str, action: str) -> FlowStart | None:
    """Resolve the identifier, push the authorization request, and return the
    URL to send the user to. ``None`` on any failure."""
    ident = identifier.strip().lstrip("@")
    if not auth.is_valid_identifier(ident):
        return None
    try:
        did, pds = auth._resolve_identity(ident)
    except Exception as exc:
        log.info("identity resolution failed for %s: %s", ident, type(exc).__name__)
        return None
    if not did or not pds or not auth._is_public_https(pds):
        return None
    server = resolve_auth_server(pds.rstrip("/"))
    if server is None:
        return None

    state = secrets.token_urlsafe(32)
    code_verifier = secrets.token_urlsafe(48)
    code_challenge = _b64url(hashlib.sha256(code_verifier.encode()).digest())
    key_pem = _generate_dpop_key_pem()
    try:
        with _client() as client:
            resp = _post_with_dpop(
                client,
                server.par_endpoint,
                {
                    "client_id": client_id(),
                    "response_type": "code",
                    "redirect_uri": redirect_uri(),
                    "scope": "atproto",
                    "state": state,
                    "login_hint": ident,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                },
                key_pem,
            )
            if resp.status_code not in (200, 201):
                log.warning("PAR failed for %s: %s %s", ident, resp.status_code, resp.text[:200])
                return None
            request_uri = resp.json().get("request_uri")
    except Exception as exc:
        log.info("PAR request failed for %s: %s", ident, type(exc).__name__)
        return None
    if not request_uri:
        return None

    _put_flow(
        state,
        PendingFlow(
            did=did,
            handle=ident,
            action=action,
            issuer=server.issuer,
            token_endpoint=server.token_endpoint,
            code_verifier=code_verifier,
            dpop_key_pem=key_pem,
            expires_at=time.time() + _FLOW_TTL,
        ),
    )
    query = urlencode({"client_id": client_id(), "request_uri": request_uri})
    return FlowStart(f"{server.authorization_endpoint}?{query}", state)


def finish_flow(state: str, code: str, iss: str | None) -> FlowResult | None:
    """Exchange the callback code; return the verified identity or ``None``.

    The flow is single-use; ``iss`` must match the issuer the flow started
    with, and the token's ``sub`` must be the DID the user named up front.
    """
    flow = _pop_flow(state)
    if flow is None or not code:
        return None
    if not iss or iss.rstrip("/") != flow.issuer:
        log.warning("issuer mismatch on callback: %r != %r", iss, flow.issuer)
        return None
    try:
        with _client() as client:
            resp = _post_with_dpop(
                client,
                flow.token_endpoint,
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri(),
                    "client_id": client_id(),
                    "code_verifier": flow.code_verifier,
                },
                flow.dpop_key_pem,
            )
            if resp.status_code != 200:
                log.warning("token exchange failed: %s %s", resp.status_code, resp.text[:200])
                return None
            sub = resp.json().get("sub")
    except Exception as exc:
        log.info("token exchange failed: %s", type(exc).__name__)
        return None
    if sub != flow.did:
        log.warning("token sub %r does not match started DID %r", sub, flow.did)
        return None
    return FlowResult(did=flow.did, handle=flow.handle, action=flow.action)
