"""Who counts as an operator, and what the admin view shows.

Admins are named in ``SKYBRIDGE_ADMINS`` as atproto handles and/or DIDs. They
sign in through the same OAuth flow as any other account on the self-service
page and get an extra panel; there is no separate password or admin token.

**Authorisation is on the DID, never the handle.** The session's ``handle`` is
the string the user typed into the sign-in form (see
``skybridge.atproto.oauth.start_flow``), not a canonically resolved handle —
only the DID is proven by the OAuth exchange. Handle entries are therefore
resolved to a DID before comparison, through the same resolver the sign-in
flow uses.

Note that an atproto handle is transferable: naming an admin by handle means
whoever currently holds that handle is an admin. Resolutions are cached only
briefly so a transfer takes effect rather than being trusted forever, but a
DID entry is the durable choice and is what ``.env.example`` recommends.
"""

from __future__ import annotations

import asyncio
import logging
import time

from skybridge.atproto import auth
from skybridge.config import get_settings

log = logging.getLogger("skybridge.admin")

# Handle -> DID resolutions are cached briefly: long enough that signing in
# doesn't re-resolve on every request, short enough that moving a handle to a
# different account takes effect within the hour.
_TTL = 900.0
_cache: dict[str, tuple[str | None, float]] = {}


def _normalize(entry: str) -> str:
    """Match the sign-in flow's normalisation so entries compare like-for-like."""
    return entry.strip().lstrip("@").lower()


def _resolve(entry: str) -> str | None:
    """The DID an admin-list entry refers to, or ``None`` if unresolvable."""
    if entry.startswith("did:"):
        return entry

    now = time.time()
    cached = _cache.get(entry)
    if cached is not None and cached[1] > now:
        return cached[0]

    did: str | None = None
    if auth.is_valid_identifier(entry):
        try:
            did, _pds = auth._resolve_identity(entry)
        except Exception as exc:
            log.info("could not resolve admin handle %s: %s", entry, type(exc).__name__)
    if did is None:
        log.warning("SKYBRIDGE_ADMINS entry %r does not resolve to a DID", entry)
    _cache[entry] = (did, now + _TTL)
    return did


def admin_dids() -> set[str]:
    """Every DID currently granted admin access, resolving handles as needed.

    Blocking: it performs DNS/HTTPS identity resolution for handle entries.
    Call it from :func:`refresh` (which runs it off-thread), never from a
    request handler — see :func:`is_admin`.
    """
    return {did for entry in get_settings().admins if (did := _resolve(_normalize(entry)))}


def _did_entries() -> set[str]:
    """Admin entries that are already DIDs, so need no network at all."""
    return {e for entry in get_settings().admins if (e := _normalize(entry)).startswith("did:")}


def is_admin(did: str | None) -> bool:
    """Is this OAuth-verified DID an operator?

    Deliberately non-blocking: it reads whatever :func:`refresh` has cached
    and never resolves inline. Resolution is a synchronous DNS + HTTPS round
    trip, and this is reached from async request handlers — doing it here
    would stall the event loop, live ingest included, every time the cache
    expired. DID entries need no resolution and work even before the first
    refresh completes.
    """
    if not did:
        return False
    if did in _did_entries():
        return True
    now = time.time()
    return any(cached_did == did for cached_did, expires in _cache.values() if expires > now)


async def refresh() -> set[str]:
    """Resolve the admin list off-thread; started and scheduled by the app."""
    return await asyncio.to_thread(admin_dids)


async def refresh_loop(interval: float = _TTL) -> None:
    """Keep the resolved admin set warm so `is_admin` never has to block."""
    while True:
        try:
            await refresh()
        except Exception:
            log.exception("admin refresh failed")
        await asyncio.sleep(interval)


def reset_cache() -> None:
    """Drop cached handle resolutions (tests, and after a settings change)."""
    _cache.clear()
