"""Resolve atproto DIDs to handles / display names, cached in the DB.

A bridged ``Person`` actor is created on first sight of a DID, with a freshly
minted RSA keypair. Resolution is best-effort and fully offline-safe: if the
network is unavailable we fall back to a synthetic handle derived from the DID
so ingestion never blocks.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass

from sqlalchemy import select

from skybridge.config import get_settings
from skybridge.crypto import generate_keypair
from skybridge.db import session_scope
from skybridge.models import BridgedActor, utcnow

PLC_DIRECTORY = "https://plc.directory"
_PROFILE_COLLECTION = "social.popfeed.actor.profile"


@dataclass
class Identity:
    did: str
    handle: str
    display_name: str | None = None
    avatar: str | None = None


def _http_json(url: str, timeout: float = 8.0) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": get_settings().user_agent})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except Exception:
        return None


def _fallback_handle(did: str) -> str:
    # did:plc:abc123 -> abc123.did (stable, dns-safe-ish, unambiguous)
    tail = did.rsplit(":", 1)[-1]
    return f"{tail}.did"


def resolve_remote(did: str) -> Identity:
    """Resolve a DID to a handle (+ optional display name) over the network."""
    doc = _http_json(f"{PLC_DIRECTORY}/{did}")
    handle: str | None = None
    pds: str | None = None
    if doc:
        for aka in doc.get("alsoKnownAs", []):
            if isinstance(aka, str) and aka.startswith("at://"):
                handle = aka[len("at://") :]
                break
        for svc in doc.get("service", []):
            if svc.get("id") == "#atproto_pds":
                pds = svc.get("serviceEndpoint")
    display_name: str | None = None
    avatar: str | None = None
    if pds:
        prof = _http_json(
            f"{pds}/xrpc/com.atproto.repo.getRecord"
            f"?repo={did}&collection={_PROFILE_COLLECTION}&rkey=self"
        )
        val = (prof or {}).get("value", {}) if isinstance(prof, dict) else {}
        display_name = val.get("displayName") or val.get("name")
    return Identity(
        did=did,
        handle=handle or _fallback_handle(did),
        display_name=display_name,
        avatar=avatar,
    )


def ensure_actor(did: str, *, allow_network: bool = True) -> Identity:
    """Return the bridged actor for ``did``, creating + persisting if needed.

    Mints an RSA keypair on first sight. Network resolution is attempted only
    for new actors (and only when ``allow_network``); existing rows are reused.
    """
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        if row is not None:
            row.last_seen = utcnow()
            return Identity(row.did, row.handle, row.display_name, row.avatar)

        ident = resolve_remote(did) if allow_network else Identity(did, _fallback_handle(did))
        private_pem, public_pem = generate_keypair()
        session.add(
            BridgedActor(
                did=did,
                handle=ident.handle,
                display_name=ident.display_name,
                avatar=ident.avatar,
                private_key_pem=private_pem,
                public_key_pem=public_pem,
            )
        )
        return ident


def actor_by_handle(handle: str) -> BridgedActor | None:
    with session_scope() as session:
        return session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))


def actor_by_ident(ident: str) -> BridgedActor | None:
    """Look up a bridged actor by either its DID or its handle."""
    with session_scope() as session:
        if ident.startswith("did:"):
            return session.get(BridgedActor, ident)
        return session.scalar(select(BridgedActor).where(BridgedActor.handle == ident))
