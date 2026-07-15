"""RSA keypairs, HTTP Signatures (draft-cavage), and LD signatures for delivery.

Mastodon / Takahe (and therefore NeoDB) speak the "draft-cavage" HTTP
Signatures scheme with RSA-SHA256:

* POST signs ``(request-target) host date digest content-type``
* GET  signs ``(request-target) host date``
* ``keyId`` is ``<actor-id>#main-key`` and resolves to the actor's
  ``publicKey.publicKeyPem``.

Activities published through relays additionally need a Mastodon-style
``RsaSignature2017`` JSON-LD signature: the relay re-signs the HTTP delivery
with its *own* key, and NeoDB's inbox rejects any delivery whose HTTP signer
differs from ``activity.actor`` unless the document carries an LD signature
whose ``creator`` resolves to that actor (401 "Relay requires LD signature").
"""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime
from email.utils import format_datetime, parsedate_to_datetime
from functools import lru_cache
from typing import Any
from urllib.parse import urldefrag, urlparse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from pyld import jsonld

from skybridge.ld_contexts import SCHEMAS

LD_IDENTITY_CONTEXT = "https://w3id.org/identity/v1"


def generate_keypair() -> tuple[str, str]:
    """Return ``(private_pem, public_pem)`` for a fresh 2048-bit RSA key."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = (
        key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


def derive_public_pem(private_pem: str) -> str:
    """Public-key PEM derived from a private-key PEM."""
    return (
        load_private_key(private_pem)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )


# PEM parsing costs far more than a sign/verify, and delivery re-signs with
# the same author key once per target inbox — cache the parsed keys.
@lru_cache(maxsize=256)
def load_private_key(pem: str) -> RSAPrivateKey:
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    assert isinstance(key, RSAPrivateKey)
    return key


@lru_cache(maxsize=256)
def load_public_key(pem: str) -> RSAPublicKey:
    key = serialization.load_pem_public_key(pem.encode())
    assert isinstance(key, RSAPublicKey)
    return key


def digest_header(body: bytes) -> str:
    """RFC 3230 SHA-256 ``Digest`` header value for a request body."""
    sha = hashlib.sha256(body).digest()
    return "SHA-256=" + base64.b64encode(sha).decode()


def http_date(when: datetime | None = None) -> str:
    return format_datetime(when or datetime.now(UTC), usegmt=True)


def _build_signing_string(headers: list[tuple[str, str]]) -> str:
    return "\n".join(f"{name}: {value}" for name, value in headers)


def sign_request(
    *,
    private_pem: str,
    key_id: str,
    method: str,
    url: str,
    body: bytes | None = None,
    date: str | None = None,
) -> dict[str, str]:
    """Build the headers (incl. ``Signature``) for a signed AP request.

    For POSTs, pass ``body`` to include ``Digest`` + ``Content-Type`` in the
    covered set; for GETs leave it ``None``.
    """
    parsed = urlparse(url)
    host = parsed.netloc
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    date = date or http_date()
    method_l = method.lower()

    covered: list[tuple[str, str]] = [
        ("(request-target)", f"{method_l} {target}"),
        ("host", host),
        ("date", date),
    ]
    out_headers: dict[str, str] = {"Host": host, "Date": date}

    if body is not None:
        digest = digest_header(body)
        content_type = "application/activity+json"
        covered.append(("digest", digest))
        covered.append(("content-type", content_type))
        out_headers["Digest"] = digest
        out_headers["Content-Type"] = content_type

    signing_string = _build_signing_string(covered)
    key = load_private_key(private_pem)
    signature = key.sign(signing_string.encode(), padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.b64encode(signature).decode()
    headers_list = " ".join(name for name, _ in covered)
    out_headers["Signature"] = (
        f'keyId="{key_id}",algorithm="rsa-sha256",headers="{headers_list}",signature="{sig_b64}"'
    )
    return out_headers


def _ld_document_loader(url: str, options: dict | None = None) -> dict:
    """Resolve JSON-LD context URLs from the vendored cache, never the network.

    Mirrors takahe's ``builtin_document_loader`` (including the empty-context
    fallback for unknown URLs) so our URDNA2015 normalization is identical to
    the one receivers run when verifying.
    """
    pieces = urlparse(url)
    if pieces.hostname is None:
        return SCHEMAS["unknown"]
    key = pieces.hostname + pieces.path.rstrip("/")
    if key in SCHEMAS:
        return SCHEMAS[key]
    wildcard = "*" + pieces.path.rstrip("/")
    return SCHEMAS.get(wildcard, SCHEMAS["unknown"])


def _ld_normalized_hash(document: dict[str, Any]) -> bytes:
    """Hex SHA-256 (as ASCII bytes) of the URDNA2015 form, Mastodon-style.

    Reference: https://socialhub.activitypub.rocks/t/making-sense-of-rsasignature2017/347
    """
    norm = jsonld.normalize(
        document,
        {
            "algorithm": "URDNA2015",
            "format": "application/n-quads",
            "documentLoader": _ld_document_loader,
        },
    )
    return hashlib.sha256(norm.encode("utf8")).hexdigest().encode("ascii")


def _ld_signature_hash(document: dict[str, Any], *, creator: str, created: str) -> bytes:
    """The signed input: options-document hash + document hash, concatenated.

    Shared by signing and verification so the two can never drift; the
    ``document`` must not contain a ``signature`` member.
    """
    options = {"@context": LD_IDENTITY_CONTEXT, "creator": creator, "created": created}
    return _ld_normalized_hash(options) + _ld_normalized_hash(document)


def create_ld_signature(
    document: dict[str, Any], *, private_pem: str, key_id: str
) -> dict[str, str]:
    """Mastodon-compatible ``RsaSignature2017`` block for ``document``.

    ``key_id`` must belong to ``document["actor"]`` (receivers reject the
    signature when ``creator``'s defragged URL differs from the actor).
    Attach the result as ``document["signature"]``.
    """
    document = {k: v for k, v in document.items() if k != "signature"}
    creator = key_id
    created = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    final_hash = _ld_signature_hash(document, creator=creator, created=created)
    key = load_private_key(private_pem)
    signature = key.sign(final_hash, padding.PKCS1v15(), hashes.SHA256())
    return {
        "@context": LD_IDENTITY_CONTEXT,
        "creator": creator,
        "created": created,
        "signatureValue": base64.b64encode(signature).decode("ascii"),
        "type": "RsaSignature2017",
    }


def verify_ld_signature(document: dict[str, Any], *, public_pem: str) -> bool:
    """Verify a ``document`` carrying an ``RsaSignature2017`` ``signature``.

    Returns ``True`` only when the signature block is well-formed, its
    ``creator`` defrags to ``document["actor"]``, and the RSA signature
    matches — the same checks NeoDB's takahe inbox runs on relayed
    activities. Never raises: any malformed shape is just ``False``.
    """
    try:
        document = document.copy()
        signature = document.pop("signature")
        if signature["type"].lower() != "rsasignature2017":
            return False
        if urldefrag(signature["creator"]).url != document.get("actor"):
            return False
        final_hash = _ld_signature_hash(
            document, creator=signature["creator"], created=signature["created"]
        )
        load_public_key(public_pem).verify(
            base64.b64decode(signature["signatureValue"]),
            final_hash,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return True
    except Exception:
        return False


def parse_signature_header(value: str) -> dict[str, str]:
    """Parse a ``Signature:`` header into its key="value" components."""
    out: dict[str, str] = {}
    for part in value.split(","):
        if "=" not in part:
            continue
        k, _, v = part.strip().partition("=")
        out[k.strip()] = v.strip().strip('"')
    return out


def verify_request(
    *,
    public_pem: str,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes | None = None,
    max_skew_seconds: int = 3600,
) -> bool:
    """Verify an incoming signed request against ``public_pem``.

    ``headers`` keys are matched case-insensitively. Returns ``True`` only if
    the signature covers and matches the reconstructed signing string (and, when
    present, the ``Digest`` matches ``body`` and ``Date`` is within skew).
    """
    lower = {k.lower(): v for k, v in headers.items()}
    sig_raw = lower.get("signature")
    if not sig_raw:
        return False
    params = parse_signature_header(sig_raw)
    signed_headers = params.get("headers", "(request-target) host date").split()
    signature_b64 = params.get("signature", "")
    if not signature_b64:
        return False

    # Verify digest matches the body if it is part of the covered headers.
    if (
        "digest" in signed_headers
        and body is not None
        and lower.get("digest") != digest_header(body)
    ):
        return False

    # Reject stale dates to limit replay.
    if "date" in signed_headers and (date_val := lower.get("date")):
        try:
            sent = parsedate_to_datetime(date_val)
            if sent.tzinfo is None:
                sent = sent.replace(tzinfo=UTC)
            if abs((datetime.now(UTC) - sent).total_seconds()) > max_skew_seconds:
                return False
        except (TypeError, ValueError):
            return False

    covered: list[tuple[str, str]] = []
    for name in signed_headers:
        if name == "(request-target)":
            covered.append((name, f"{method.lower()} {path}"))
        else:
            covered.append((name, lower.get(name, "")))
    signing_string = _build_signing_string(covered)

    try:
        key = load_public_key(public_pem)
        key.verify(
            base64.b64decode(signature_b64),
            signing_string.encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return True
    except Exception:
        return False
