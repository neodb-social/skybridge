"""AT Protocol identity resolution with SSRF hardening.

Syntax-validates handles/DIDs, resolves them to ``(did, pds)`` and vets every
URL we would fetch as public https. Authentication happens via OAuth against
the user's own authorization server (see :mod:`skybridge.atproto.oauth`); no
credentials ever touch this relay.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from urllib.parse import unquote, urlparse

log = logging.getLogger("skybridge.auth")

# Official atproto handle syntax (domain-like, 253 chars max).
_HANDLE_RE = re.compile(
    r"^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
    r"[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$"
)
_DID_RE = re.compile(r"^did:[a-z]+:[A-Za-z0-9._:%-]{1,512}$")
_RESOLVE_TIMEOUT = 5.0

# Final DNS labels that denote local/special-use names; resolving an identity
# rooted at one of these would let callers point us at internal hosts.
_SPECIAL_SUFFIXES = frozenset(
    {"localhost", "local", "internal", "arpa", "test", "invalid", "onion", "home", "corp", "lan"}
)


def _is_public_name(host: str) -> bool:
    """Syntactic check that ``host`` is a public DNS name, not an IP literal
    or a special-use name like ``foo.localhost`` / ``metadata.google.internal``."""
    try:
        ipaddress.ip_address(host.strip("[]"))
        return False
    except ValueError:
        pass
    labels = host.lower().rstrip(".").split(".")
    return len(labels) >= 2 and labels[-1] not in _SPECIAL_SUFFIXES


def is_valid_identifier(identifier: str) -> bool:
    """Cheap syntax check so we never resolve attacker-shaped garbage."""
    if identifier.startswith("did:web:"):
        # did:web resolution fetches https://<host>/.well-known/did.json, so
        # the embedded host must itself look public.
        if _DID_RE.match(identifier) is None:
            return False
        host = unquote(identifier.removeprefix("did:web:").split(":", 1)[0]).split(":", 1)[0]
        return _is_public_name(host)
    if identifier.startswith("did:"):
        return _DID_RE.match(identifier) is not None
    return (
        len(identifier) <= 253
        and _HANDLE_RE.match(identifier) is not None
        # Handle resolution may fetch https://<handle>/.well-known/atproto-did.
        and _is_public_name(identifier)
    )


def _is_public_https(url: str) -> bool:
    """True if ``url`` is https and its host resolves only to public IPs.

    Guards against a crafted DID document pointing our PDS login at
    localhost/private/link-local endpoints (SSRF).
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname:
            return False
        infos = socket.getaddrinfo(parsed.hostname, parsed.port or 443)
        addrs = [info[4][0] for info in infos]
        return bool(addrs) and all(ipaddress.ip_address(a).is_global for a in addrs)
    except (OSError, ValueError):
        return False


def _resolve_identity(identifier: str) -> tuple[str | None, str | None]:
    """Resolve a handle-or-DID to ``(did, pds_endpoint)`` using atproto resolvers."""
    from atproto import IdResolver

    resolver = IdResolver(timeout=_RESOLVE_TIMEOUT)
    did = identifier if identifier.startswith("did:") else resolver.handle.resolve(identifier)
    if not did:
        return None, None
    doc = resolver.did.resolve(did)
    pds: str | None = None
    if doc is not None:
        for svc in getattr(doc, "service", None) or []:
            svc_id = getattr(svc, "id", None) or (svc.get("id") if isinstance(svc, dict) else None)
            if svc_id in ("#atproto_pds", "atproto_pds"):
                pds = getattr(svc, "service_endpoint", None) or (
                    svc.get("serviceEndpoint") if isinstance(svc, dict) else None
                )
                break
    return did, pds


def resolve_did(identifier: str) -> str | None:
    """Best-effort handle-or-DID → DID resolution. Returns ``None`` on failure.

    Syntactically invalid identifiers are rejected before any network I/O so
    unauthenticated callers cannot use us to probe arbitrary hosts.
    """
    ident = identifier.strip()
    if not is_valid_identifier(ident):
        return None
    try:
        did, _ = _resolve_identity(ident)
        return did
    except Exception as exc:
        log.info("identity resolution failed for %s: %s", identifier, type(exc).__name__)
        return None
