"""Resolve atproto DIDs to handles / display names, cached in the DB.

A bridged ``Person`` actor is created on first sight of a DID, with a freshly
minted RSA keypair. Resolution is best-effort and fully offline-safe: if the
network is unavailable we fall back to a synthetic handle derived from the DID
so ingestion never blocks.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.orm import Session

from skybridge.config import get_settings
from skybridge.crypto import generate_keypair
from skybridge.db import session_scope
from skybridge.models import BridgedActor, HandleAlias, utcnow

log = logging.getLogger("skybridge.identity")

PLC_DIRECTORY = "https://plc.directory"

# Reported in place of a handle when it no longer resolves back to its DID
# (https://atproto.com/specs/handle). It is a placeholder, not a name: every
# account in that state reports the same one, so treating it as a rename would
# make them all fight over a single /users/handle.invalid actor.
INVALID_HANDLE = "handle.invalid"
_PROFILE_COLLECTION = "social.popfeed.actor.profile"
_BSKY_PROFILE_COLLECTION = "app.bsky.actor.profile"

# Bluesky's "Ask apps to hide my posts from algorithmic recommendations"
# toggle. A public record, rkey `self`, whose only field is a boolean; the
# lexicon requires consumers to read a MISSING record as false.
VISIBILITY_COLLECTION = "app.bsky.actor.contentVisibilityDeclaration"
HIDE_FIELD = "hideFromAlgorithmicRecommendations"

# Bluesky's "hide my posts from logged-out users" toggle, which rides as a
# self-label on the bsky profile record rather than as a record of its own.
NO_UNAUTHENTICATED = "!no-unauthenticated"


@dataclass
class Identity:
    did: str
    handle: str
    display_name: str | None = None
    avatar: str | None = None
    # Visibility preferences read off the atproto account; see BridgedActor.
    hide_from_recommendations: bool = False
    no_unauthenticated: bool = False


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


def has_self_label(bsky_value: dict, label: str) -> bool:
    """Is ``label`` among the self-labels on an ``app.bsky.actor.profile`` record?

    Shape is ``labels: {$type: com.atproto.label.defs#selfLabels, values:
    [{val: ...}]}``. Defensive throughout: an account with no labels at all is
    the common case, and a malformed one must read as "not labelled" rather
    than raise in the middle of identity resolution.
    """
    labels = bsky_value.get("labels")
    values = labels.get("values") if isinstance(labels, dict) else None
    if not isinstance(values, list):
        return False
    return any(isinstance(v, dict) and v.get("val") == label for v in values)


def _hide_from_recommendations(pds: str, did: str) -> bool:
    """The contentVisibilityDeclaration flag for ``did``.

    An absent record reads as ``False``, which the lexicon requires:
    "Consumers must treat a missing record as false." A fetch that failed is
    indistinguishable here and reads as ``False`` too, which is why the
    callers only ever *raise* the stored flag on this value — see
    ``refresh_actor``.
    """
    return bool(_profile_record(pds, did, VISIBILITY_COLLECTION).get(HIDE_FIELD))


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

    The bsky profile record is now read unconditionally, because it also
    carries the ``!no-unauthenticated`` self-label, which has to be known
    whether or not popfeed supplied a name and avatar. In practice this costs
    nothing: popfeed profiles carry no avatar, so the fallback already fired
    on almost every account.
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
    hide_from_recommendations = False
    no_unauthenticated = False
    if pds:
        val = _profile_record(pds, did, _PROFILE_COLLECTION)
        display_name = val.get("displayName") or val.get("name") or None
        avatar = _avatar_url(val, did=did, pds=pds)
        bsky_val = _profile_record(pds, did, _BSKY_PROFILE_COLLECTION)
        if not display_name:
            display_name = bsky_val.get("displayName") or bsky_val.get("name") or None
        if not avatar:
            avatar = _avatar_url(bsky_val, did=did, pds=pds)
        no_unauthenticated = has_self_label(bsky_val, NO_UNAUTHENTICATED)
        hide_from_recommendations = _hide_from_recommendations(pds, did)
    return Identity(
        did=did,
        handle=handle or _fallback_handle(did),
        display_name=display_name,
        avatar=avatar,
        hide_from_recommendations=hide_from_recommendations,
        no_unauthenticated=no_unauthenticated,
    )


def _claim_handle(session: Session, handle: str, *, did: str) -> None:
    """Give ``handle`` exclusively to ``did`` inside this session.

    A handle points at exactly one DID at a time on atproto, so anyone else
    still holding it here is stale: we saw them under that name before it
    moved. They are pushed onto their synthetic DID handle rather than left to
    collide, because every actor lookup is ``where(handle == ...)`` — a
    duplicate would let one account's URL, WebFinger record and HTTP signature
    key id resolve to the other account's row.
    """
    stale = session.scalars(
        select(BridgedActor).where(BridgedActor.handle == handle, BridgedActor.did != did)
    ).all()
    for row in stale:
        row.handle = _fallback_handle(row.did)
        log.warning(
            "handle %s moved to %s; displaced actor %s to %s", handle, did, row.did, row.handle
        )
    # A live claim outranks any retired alias on the same name.
    session.execute(sa_delete(HandleAlias).where(HandleAlias.handle == handle))


def ensure_actor(did: str, *, allow_network: bool = True) -> Identity:
    """Return the bridged actor for ``did``, creating + persisting if needed.

    Mints an RSA keypair on first sight. Network resolution is attempted only
    for new actors (and only when ``allow_network``); existing rows are reused.
    """
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        if row is not None:
            row.last_seen = utcnow()
            return Identity(
                row.did,
                row.handle,
                row.display_name,
                row.avatar,
                hide_from_recommendations=bool(row.hide_from_recommendations),
                no_unauthenticated=bool(row.no_unauthenticated),
            )

        ident = resolve_remote(did) if allow_network else Identity(did, _fallback_handle(did))
        private_pem, public_pem = generate_keypair()
        _claim_handle(session, ident.handle, did=did)
        session.add(
            BridgedActor(
                did=did,
                handle=ident.handle,
                display_name=ident.display_name,
                avatar=ident.avatar,
                hide_from_recommendations=ident.hide_from_recommendations,
                no_unauthenticated=ident.no_unauthenticated,
                private_key_pem=private_pem,
                public_key_pem=public_pem,
            )
        )
        return ident


def rename_actor(did: str, handle: str) -> BridgedActor | None:
    """Apply a handle change from a Jetstream ``identity`` event.

    Returns the updated row, or ``None`` when there is nothing to do: an
    identity event must never mint an actor (we bridge on content, not on
    existence), and an unchanged handle is not worth an ``Update(Person)``.
    The retired handle is kept as a :class:`HandleAlias` so already-federated
    actor and object ids keep resolving. A handle that stopped resolving
    (``INVALID_HANDLE``) is not a rename: the actor keeps the last name we
    know it by until a real one arrives.
    """
    if not handle or handle == INVALID_HANDLE:
        return None
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        if row is None or row.handle == handle:
            return None

        retired = row.handle
        _claim_handle(session, handle, did=did)
        row.handle = handle
        row.last_seen = utcnow()
        session.merge(HandleAlias(handle=retired, did=did))
        log.info("actor %s renamed %s -> %s", did, retired, handle)
        return row


def set_hide_from_recommendations(did: str, hide: bool) -> BridgedActor | None:
    """Apply a contentVisibilityDeclaration commit to an actor we already bridge.

    Returns the updated row, or ``None`` when there is nothing to do. Like a
    handle change, a declaration must never mint an actor — we bridge people
    because of what they post, not because they set a preference — and a value
    that did not move is not worth an ``Update(Person)`` to every follower.
    """
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        if row is None or bool(row.hide_from_recommendations) == hide:
            return None
        row.hide_from_recommendations = hide
        row.last_seen = utcnow()
        log.info("actor %s hide_from_recommendations -> %s", did, hide)
        return row


def did_for_ident(session: Session, ident: str) -> str | None:
    """Resolve a route identifier to a DID: DID, live handle, then retired handle.

    Every handle-keyed lookup goes through this so a rename does not 404 the
    URLs peers already hold.
    """
    if not ident:
        return None
    if ident.startswith("did:"):
        return ident
    did = session.scalar(select(BridgedActor.did).where(BridgedActor.handle == ident))
    if did is not None:
        return did
    return session.scalar(select(HandleAlias.did).where(HandleAlias.handle == ident))


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

    Also the point at which ``!no-unauthenticated`` is re-read: that label
    lives on the bsky profile record, and we deliberately do not tail
    ``app.bsky.actor.profile`` on Jetstream (see config.WANTED_COLLECTIONS),
    so a toggle lands here rather than the moment it is made.
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
            # Unconditional now, unlike the name/avatar fallback it also
            # serves: the self-label has to be read even when popfeed already
            # supplied both fields.
            bsky_val = _profile_record(pds, did, _BSKY_PROFILE_COLLECTION)
            if not display_name:
                display_name = bsky_val.get("displayName") or bsky_val.get("name") or None
            if not avatar:
                avatar = _avatar_url(bsky_val, did=did, pds=pds)
            # Visibility preferences are only ever RAISED here, never cleared,
            # for the same reason the avatar is never cleared below: a failed
            # fetch and a preference the user turned off both arrive as an
            # empty record, and the two must not be confused when one of them
            # means "keep publishing this person more widely again".
            #
            # Clearing has an exact signal of its own and does not need this
            # path: hide_from_recommendations is cleared by the declaration's
            # own Jetstream commit (pipeline._process_visibility, where a
            # delete and a false both mean false), and no_unauthenticated by a
            # bsky profile record that came back and no longer carries the
            # label.
            if bsky_val:
                row.no_unauthenticated = has_self_label(bsky_val, NO_UNAUTHENTICATED)
            if _hide_from_recommendations(pds, did):
                row.hide_from_recommendations = True

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
    """Look up a bridged actor by its DID, its handle, or a retired handle."""
    with session_scope() as session:
        did = did_for_ident(session, ident)
        return session.get(BridgedActor, did) if did else None
