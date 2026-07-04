"""WebFinger discovery for the relay actor and bridged ``Person`` actors."""

from __future__ import annotations

from sqlalchemy import select

from skybridge.activitypub.actors import RELAY_DID
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor


def _jrd(subject: str, actor_id: str, extra_aliases: list[str] | None = None) -> dict:
    return {
        "subject": subject,
        "aliases": [actor_id, *(extra_aliases or [])],
        "links": [
            {
                "rel": "self",
                "type": "application/activity+json",
                "href": actor_id,
            },
            {
                "rel": "http://webfinger.net/rel/profile-page",
                "type": "text/html",
                "href": actor_id,
            },
        ],
    }


def resolve(resource: str) -> dict | None:
    """Resolve an ``acct:user@domain`` (or actor URL) resource to a JRD."""
    settings = get_settings()
    resource = resource.strip()
    username: str | None = None

    if resource.startswith("acct:"):
        acct = resource[len("acct:") :]
        if "@" in acct:
            username, _, host = acct.partition("@")
            if host and host != settings.domain:
                return None
        else:
            username = acct
    elif resource.startswith(("http://", "https://")):
        username = resource.rstrip("/").rsplit("/", 1)[-1]

    if not username:
        return None

    if username == settings.relay_username:
        return _jrd(settings.acct(settings.relay_username), settings.relay_actor_id)

    with session_scope() as session:
        actor = session.scalar(
            select(BridgedActor).where(
                BridgedActor.handle == username, BridgedActor.did != RELAY_DID
            )
        )
        if actor is None or actor.opted_out:
            return None
        return _jrd(
            settings.acct(actor.handle),
            settings.actor_id(actor.handle),
            extra_aliases=[f"https://bsky.app/profile/{actor.did}"],
        )
