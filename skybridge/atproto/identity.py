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
_BSKY_PROFILE_COLLECTION = "app.bsky.actor.profile"


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


def _profile_record(pds: str, did: str, collection: str) -> dict:
    prof = _http_json(
        f"{pds}/xrpc/com.atproto.repo.getRecord?repo={did}&collection={collection}&rkey=self"
    )
    return (prof or {}).get("value", {}) if isinstance(prof, dict) else {}


def _avatar_url(value: dict, *, did: str, pds: str) -> str | None:
    """Extract an avatar blob URL from a profile record's ``avatar`` field.

    Handles both the modern blob shape (``{"ref": {"$link": cid}, ...}``) and
    the legacy shape (a plain ``"cid"`` key). Defensive by design: any
    missing/unexpected shape yields ``None`` rather than raising, since this
    only ever runs opportunistically during best-effort identity resolution.
    """
    try:
        blob = value.get("avatar")
        if not isinstance(blob, dict):
            return None
        cid = blob.get("cid") or blob.get("ref", {}).get("$link")
        if not cid:
            return None
        return f"{pds}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}"
    except Exception:
        return None


def _pds_from_doc(doc: dict) -> str | None:
    """Extract the atproto PDS endpoint from a PLC document."""
    for svc in doc.get("service", []):
        if svc.get("id") == "#atproto_pds":
            return svc.get("serviceEndpoint")
    return None


def resolve_pds(did: str) -> str | None:
    """Fetch the PLC doc for ``did`` and return its atproto PDS endpoint.

    Offline-safe: yields ``None`` on any failure (unresolvable DID, network
    error, malformed doc, no ``#atproto_pds`` service) rather than raising.
    """
    doc = _http_json(f"{PLC_DIRECTORY}/{did}")
    return _pds_from_doc(doc) if doc else None


def resolve_remote(did: str) -> Identity:
    """Resolve a DID to a handle (+ optional display name/avatar) over the network.

    Profile info comes from the ``social.popfeed.actor.profile`` record first.
    Observed popfeed profiles never carry an ``avatar`` blob (and sometimes
    have an empty ``displayName``), so whenever either field is still missing
    we fall back to the ``app.bsky.actor.profile`` record on the same PDS —
    in practice the only real source of an avatar. Popfeed values win over the
    bsky fallback whenever present. Avatars are blobs served off the PDS via
    the ``com.atproto.sync.getBlob`` endpoint, not plain URLs.
    """
    doc = _http_json(f"{PLC_DIRECTORY}/{did}")
    handle: str | None = None
    if doc:
        for aka in doc.get("alsoKnownAs", []):
            if isinstance(aka, str) and aka.startswith("at://"):
                handle = aka[len("at://") :]
                break
    pds = _pds_from_doc(doc) if doc else None
    display_name: str | None = None
    avatar: str | None = None
    if pds:
        val = _profile_record(pds, did, _PROFILE_COLLECTION)
        display_name = val.get("displayName") or val.get("name") or None
        avatar = _avatar_url(val, did=did, pds=pds)
        if not display_name or not avatar:
            bsky_val = _profile_record(pds, did, _BSKY_PROFILE_COLLECTION)
            if not display_name:
                display_name = bsky_val.get("displayName") or bsky_val.get("name") or None
            if not avatar:
                avatar = _avatar_url(bsky_val, did=did, pds=pds)
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


def refresh_actor(
    did: str, popfeed_value: dict, *, allow_network: bool = True
) -> BridgedActor | None:
    """Refresh an existing bridged actor's display name/avatar from a fresh
    ``social.popfeed.actor.profile`` record.

    A profile edit alone must never mint an actor: returns ``None`` if none
    exists yet for ``did``. Mirrors ``resolve_remote``'s source precedence —
    the popfeed value (already in hand from the firehose event) wins, falling
    back to the ``app.bsky.actor.profile`` record on the same PDS when either
    field is still missing, network permitting.
    """
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        if row is None:
            return None

        display_name = popfeed_value.get("displayName") or popfeed_value.get("name") or None
        avatar: str | None = None
        pds = resolve_pds(did) if allow_network else None
        if pds:
            avatar = _avatar_url(popfeed_value, did=did, pds=pds)

        if pds and (not display_name or not avatar):
            bsky_val = _profile_record(pds, did, _BSKY_PROFILE_COLLECTION)
            if not display_name:
                display_name = bsky_val.get("displayName") or bsky_val.get("name") or None
            if not avatar:
                avatar = _avatar_url(bsky_val, did=did, pds=pds)

        if pds:
            # Both popfeed and the bsky fallback were consulted and neither
            # had a name: the user genuinely cleared it, so clear ours too.
            row.display_name = display_name
        elif display_name is not None:
            # No PDS consulted (network disallowed, or PLC unreachable): only
            # overwrite with a real value — never clear on partial information.
            row.display_name = display_name

        # Avatar is NEVER cleared here: a missing candidate could mean the
        # user removed their avatar, or simply that the fetch failed / the
        # network was disallowed — we can't tell those apart, so only
        # overwrite when a fresh value was actually found.
        if avatar is not None:
            row.avatar = avatar

        row.last_seen = utcnow()
        return row


def actor_by_handle(handle: str) -> BridgedActor | None:
    with session_scope() as session:
        return session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))


def actor_by_ident(ident: str) -> BridgedActor | None:
    """Look up a bridged actor by either its DID or its handle."""
    with session_scope() as session:
        if ident.startswith("did:"):
            return session.get(BridgedActor, ident)
        return session.scalar(select(BridgedActor).where(BridgedActor.handle == ident))
