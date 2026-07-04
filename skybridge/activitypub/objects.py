"""Serve dereferenceable AP objects: Notes, Tombstones and catalog works."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select

from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Record, Work
from skybridge.translate import works
from skybridge.translate.neodb import AP_CONTEXT
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
    # Serve the full extension context so JSON-LD-strict consumers keep the
    # neodb relatedWith terms when they re-fetch the Note.
    obj.setdefault("@context", AP_CONTEXT)
    return obj


def get_work_object(work_type: str, work_id: str) -> dict[str, Any] | None:
    """AP catalog object, shaped like NeoDB's own item JSON (ItemSchema).

    NeoDB peers resolve ``withRegardTo``/work-tag URLs through
    catalog/sites/fedi.py, which requires ``type`` to be a NeoDB catalog type
    and ``id`` to equal the fetched URL exactly; ``external_resources`` /
    ``imdb`` / ``isbn`` let the peer merge our work with an item it already
    knows, and ``cover_image_url`` / ``localized_title`` feed its metadata.
    """
    work_key = f"{work_type}:{work_id}"
    with session_scope() as session:
        row = session.get(Work, work_key)
        if row is None:
            return None
        identifiers = json.loads(row.identifiers_json or "{}")
        title = row.title
        poster = row.poster_url
    settings = get_settings()
    url = settings.catalog_id(work_type, work_id)
    display_title = title or work_id
    doc: dict[str, Any] = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": url,
        "type": works.ap_type_for(work_type) or "Document",
        "name": display_title,
        "title": display_title,
        "display_title": display_title,
        "localized_title": [{"lang": "en", "text": display_title}],
        "category": category_for(work_type),
        "external_resources": [
            {"url": u} for u in works.external_resource_urls(work_type, identifiers)
        ],
    }
    if poster:
        doc["cover_image_url"] = poster
    if identifiers.get("imdbId"):
        doc["imdb"] = str(identifiers["imdbId"])
    isbn = identifiers.get("isbn13") or identifiers.get("isbn") or identifiers.get("isbn10")
    if isbn:
        doc["isbn"] = str(isbn)
    return doc
