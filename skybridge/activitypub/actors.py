"""ActivityPub actor documents: the relay ``Application`` + per-user ``Person``.

The relay actor's private key lives OUTSIDE the database: either supplied
explicitly via ``SKYBRIDGE_RELAY_KEY`` (PEM), or in the PEM file at
``SKYBRIDGE_RELAY_KEY_FILE``, minted there on first use. Pre-existing
deployments that minted the key into the DB (reserved pseudo-DID row) are
migrated to the file automatically so the relay identity survives upgrades.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from skybridge.config import get_settings
from skybridge.crypto import derive_public_pem, generate_keypair
from skybridge.db import session_scope
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
        private_pem = _legacy_db_relay_key() or generate_keypair()[0]
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(private_pem)
    private_pem = path.read_text()
    return private_pem, derive_public_pem(private_pem)


def _legacy_db_relay_key() -> str | None:
    """Key from the pre-file-storage DB row, so upgrades keep the identity."""
    with session_scope() as session:
        row = session.get(BridgedActor, RELAY_DID)
        return row.private_key_pem if row is not None else None


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
