"""ActivityPub actor documents: the relay ``Application`` + per-user ``Person``.

The relay actor's keypair lives in the DB under a reserved pseudo-DID so it is
managed exactly like a bridged actor (mint-on-first-use, stable thereafter).
"""

from __future__ import annotations

from typing import Any

from skybridge.config import get_settings
from skybridge.crypto import generate_keypair
from skybridge.db import session_scope
from skybridge.models import BridgedActor, utcnow

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
    """Return ``(private_pem, public_pem)`` for the relay actor, minting once."""
    with session_scope() as session:
        row = session.get(BridgedActor, RELAY_DID)
        if row is None:
            settings = get_settings()
            private_pem, public_pem = generate_keypair()
            row = BridgedActor(
                did=RELAY_DID,
                handle=settings.relay_username,
                display_name=settings.relay_name,
                private_key_pem=private_pem,
                public_key_pem=public_pem,
            )
            session.add(row)
        row.last_seen = utcnow()
        return row.private_key_pem, row.public_key_pem


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
