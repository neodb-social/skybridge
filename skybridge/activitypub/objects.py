"""Serve dereferenceable AP objects: Notes, Tombstones and catalog works."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select

from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, Work
from skybridge.translate.works import category_for


def _record_for(ident: str, rkey: str) -> Record | None:
    """Find the archived record for a bridged author's post by handle/did + rkey."""
    with session_scope() as session:
        if ident.startswith("did:"):
            did = ident
        else:
            author = session.scalar(select(BridgedActor).where(BridgedActor.handle == ident))
            if author is None:
                return None
            did = author.did
        return session.scalar(select(Record).where(Record.did == did, Record.rkey == rkey))


def get_post_object(ident: str, rkey: str) -> dict[str, Any] | None:
    """Return the stored ``Note`` (or a ``Tombstone`` if deleted)."""
    record = _record_for(ident, rkey)
    if record is None:
        return None
    if record.ap_object_json is None and record.ap_activity_json is None:
        # Archived without ever being published to AP (lists, collection
        # membership, merged-away pair records) — deleted or not, there is
        # nothing to dereference and nothing to tombstone.
        return None
    settings = get_settings()
    object_id = settings.post_id(ident, rkey)
    if record.deleted_at is not None:
        return {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": object_id,
            "type": "Tombstone",
            "formerType": "Note",
            "deleted": record.deleted_at.isoformat() if record.deleted_at else None,
        }
    if record.ap_object_json is None:
        return None
    obj = json.loads(record.ap_object_json)
    obj.setdefault("@context", "https://www.w3.org/ns/activitystreams")
    return obj


def get_work_object(work_type: str, work_id: str) -> dict[str, Any] | None:
    work_key = f"{work_type}:{work_id}"
    with session_scope() as session:
        row = session.get(Work, work_key)
        if row is None:
            return None
        identifiers = json.loads(row.identifiers_json or "{}")
        title = row.title
        poster = row.poster_url
    settings = get_settings()
    doc: dict[str, Any] = {
        "@context": [
            "https://www.w3.org/ns/activitystreams",
            {"category": "https://joinmastodon.org/ns#category"},
        ],
        "id": settings.catalog_id(work_type, work_id),
        "type": "Document",
        "name": title or work_id,
        "category": category_for(work_type),
        "attachment": [
            {"type": "PropertyValue", "name": k, "value": str(v)} for k, v in identifiers.items()
        ],
    }
    if poster:
        doc["image"] = {"type": "Image", "url": poster}
    return doc
