"""ActivityPub actor documents: the relay ``Application`` + per-user ``Person``.

The relay actor's private key is ALWAYS operator-provided (never minted):
either inline via ``SKYBRIDGE_RELAY_KEY`` (PEM) or as a PEM file the operator
placed at ``$SKYBRIDGE_DATA/relay_key.pem``. Startup fails loudly when
neither is present.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from skybridge.config import get_settings
from skybridge.crypto import derive_public_pem
from skybridge.models import BridgedActor

RELAY_DID = "did:skybridge:relay"
SECURITY_CONTEXT = "https://w3id.org/security/v1"
AS_CONTEXT = "https://www.w3.org/ns/activitystreams"

# Mastodon's namespace, where `discoverable` and `indexable` live. Only the
# PREFIX is declared here, never the `discoverable`/`indexable` aliases
# Mastodon's own context defines, because the two consumers we care about read
# the flags differently and the prefix-only context is what lets us satisfy
# both at once (see _visibility_flags).
TOOT_TERMS = {"toot": "http://joinmastodon.org/ns#"}


def _public_key_block(actor_id: str, public_pem: str) -> dict:
    return {
        "id": f"{actor_id}#main-key",
        "owner": actor_id,
        "publicKeyPem": public_pem,
    }


def get_relay_keys() -> tuple[str, str]:
    """Return ``(private_pem, public_pem)`` for the relay actor."""
    settings = get_settings()
    return _relay_keys(settings.relay_key_pem, settings.relay_key_file)


@lru_cache(maxsize=4)
def _relay_keys(inline_pem: str | None, key_file: str) -> tuple[str, str]:
    if inline_pem:
        return inline_pem, derive_public_pem(inline_pem)
    path = Path(key_file)
    if not path.exists():
        raise RuntimeError(
            f"relay signing key not found: set SKYBRIDGE_RELAY_KEY or place a "
            f"PEM at {path} — e.g.\n"
            f"  printf 'SKYBRIDGE_RELAY_KEY=\"%s\"\\n' "
            f'"$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048)" >> .env'
        )
    private_pem = path.read_text()
    return private_pem, derive_public_pem(private_pem)


def relay_actor() -> dict[str, Any]:
    """The relay's ``Application`` actor document."""
    settings = get_settings()
    _, public_pem = get_relay_keys()
    actor_id = settings.relay_actor_id
    return {
        "@context": [AS_CONTEXT, SECURITY_CONTEXT],
        "id": actor_id,
        "type": "Application",
        "preferredUsername": settings.relay_username,
        "name": settings.relay_name,
        "summary": settings.relay_summary,
        "inbox": f"{actor_id}/inbox",
        "outbox": f"{actor_id}/outbox",
        "followers": f"{actor_id}/followers",
        "following": f"{actor_id}/following",
        "endpoints": {"sharedInbox": settings.url("inbox")},
        # Not a bridged identity itself (it's the relay/Application), so no
        # alsoKnownAs / rel="me" account-linking is needed here.
        "url": settings.base_url,
        "publicKey": _public_key_block(actor_id, public_pem),
    }


def person_actor(actor: BridgedActor) -> dict[str, Any]:
    """A bridged author's ``Person`` actor document."""
    settings = get_settings()
    actor_id = settings.actor_id(actor.handle)
    doc: dict[str, Any] = {
        "@context": [
            AS_CONTEXT,
            SECURITY_CONTEXT,
            {"alsoKnownAs": {"@id": "as:alsoKnownAs", "@type": "@id"}, **TOOT_TERMS},
        ],
        "id": actor_id,
        "type": "Person",
        "preferredUsername": actor.handle,
        "name": actor.display_name or actor.handle,
        "summary": (
            f"Bridged from Atmosphere. Original account: "
            f'<a href="https://bsky.app/profile/{actor.did}" rel="me">{actor.handle}</a>'
        ),
        "alsoKnownAs": [
            f"at://{actor.did}",
            f"https://bsky.app/profile/{actor.did}",
        ],
        "inbox": f"{actor_id}/inbox",
        "outbox": f"{actor_id}/outbox",
        "followers": f"{actor_id}/followers",
        "following": f"{actor_id}/following",
        "endpoints": {"sharedInbox": settings.url("inbox")},
        "url": actor_id,
        "attachment": [
            {
                "type": "PropertyValue",
                "name": "AT Protocol",
                "value": (
                    f'<a href="https://bsky.app/profile/{actor.did}" rel="me">{actor.did}</a>'
                ),
            }
        ],
        "publicKey": _public_key_block(actor_id, actor.public_key_pem),
    }
    if actor.avatar:
        doc["icon"] = {"type": "Image", "url": actor.avatar}
    doc.update(_visibility_flags(actor))
    return doc


def _visibility_flags(actor: BridgedActor) -> dict[str, Any]:
    """Mastodon's `discoverable`/`indexable`, carrying the atproto preferences.

    Written under BOTH the bare and the ``toot:``-prefixed key, because the two
    receivers read them in incompatible ways and each ignores the other's form:

    - Mastodon never compacts a fetched actor; it reads the raw JSON key
      (``@json['discoverable']`` in ``process_account_service.rb``), so it
      needs the bare name.
    - NeoDB/takahe compacts the actor against the document's OWN context and
      then reads ``toot:discoverable`` (``users/models/identity.py``), so it
      only sees the value when a ``toot:``-prefixed key survives compaction.

    Declaring the `discoverable` alias in our context would satisfy neither:
    compaction would then fold both keys into one array. With the prefix alone
    (see TOOT_TERMS) the bare key stays an undefined term, so it passes through
    takahe's compaction untouched and is dropped entirely by a receiver whose
    context defines the alias — leaving exactly one value on every path.

    Emitted only when a flag is set. Both fields default to permissive on the
    receiving side, so silence is already the right answer for everyone else,
    and volunteering `discoverable: true` would opt every bridged author into
    directories and follow recommendations that nobody asked for.
    """
    flags: dict[str, Any] = {}
    if actor.hide_from_recommendations or actor.no_unauthenticated:
        # "Ask apps to hide my posts from algorithmic recommendations." On
        # Mastodon this drops the account from the profile directory, from
        # follow recommendations and from trends; on NeoDB it also withholds
        # consent to be featured in someone's collection (FEP-7aa9).
        flags["discoverable"] = False
        flags["toot:discoverable"] = False
    if actor.no_unauthenticated:
        # An account that hides from logged-out readers plainly does not want
        # its posts in a full-text search index either.
        flags["indexable"] = False
        flags["toot:indexable"] = False
    return flags
