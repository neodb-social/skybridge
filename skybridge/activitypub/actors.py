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
        "url": settings.base_url,
        "publicKey": _public_key_block(actor_id, public_pem),
    }


def person_actor(actor: BridgedActor) -> dict[str, Any]:
    """A bridged author's ``Person`` actor document."""
    settings = get_settings()
    actor_id = settings.actor_id(actor.handle)
    doc: dict[str, Any] = {
        "@context": [AS_CONTEXT, SECURITY_CONTEXT],
        "id": actor_id,
        "type": "Person",
        "preferredUsername": actor.handle,
        "name": actor.display_name or actor.handle,
        "summary": (
            f"Bridged from popfeed on the AT Protocol. Original account: "
            f'<a href="https://bsky.app/profile/{actor.did}">{actor.handle}</a>'
        ),
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
    return doc
