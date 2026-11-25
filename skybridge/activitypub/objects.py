"""Serve dereferenceable AP objects: Notes, Tombstones and catalog works."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, text

from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Record, Work
from skybridge.translate import works
from skybridge.translate.neodb import AP_CONTEXT
from skybridge.translate.works import category_for

# Every distinct collection in the archive, one index seek per collection
# rather than a pass over the index: each step asks ix_record_collection for
# the next value after the last one found.
_COLLECTIONS = text(
    "WITH RECURSIVE c(name) AS ("
    " SELECT min(collection) FROM record"
    " UNION ALL"
    " SELECT (SELECT min(collection) FROM record WHERE collection > name)"
    " FROM c WHERE name IS NOT NULL"
    ") SELECT name FROM c WHERE name IS NOT NULL"
)


def _record_for(ident: str, rkey: str) -> Record | None:
    """Find the archived record for a bridged author's post by handle/did + rkey.

    ``ident`` may be a handle the author has since renamed away from: object
    ids are minted from the handle of the day and stay federated forever.

    The post URL carries no collection, and no index covers ``(did, rkey)``,
    so filtering on those two columns reads every row the author has: for an
    author with hundreds of thousands of teal.fm plays, that is seconds per
    request. A record's key is ``at://<did>/<collection>/<rkey>``, though, and
    the archive holds only a handful of collections, so this tries each one
    by primary key instead.
    """
    with session_scope() as session:
        did = identity.did_for_ident(session, ident)
        if did is None:
            return None
        uris = [f"at://{did}/{name}/{rkey}" for name in session.scalars(_COLLECTIONS)]
        if not uris:
            return None
        return session.scalar(select(Record).where(Record.at_uri.in_(uris)).limit(1))


@dataclass(frozen=True)
class PostView:
    """A dereferenced post: the AP document plus the atproto URI behind it.

    ``at_uri`` is not part of the ``Note`` — the translator never puts it there
    — but the human-readable page names the source record, so it is carried
    alongside rather than re-queried.
    """

    document: dict[str, Any]
    at_uri: str


def get_post_view(ident: str, rkey: str) -> PostView | None:
    """Return the stored ``Note`` (or a ``Tombstone`` if deleted), plus its source URI."""
    record = _record_for(ident, rkey)
    if record is None:
        return None
    if record.ap_object_json is None and record.ap_activity_json is None:
        # Archived without ever being published to AP (lists, collection
        # membership, merged-away pair records) — deleted or not, there is
        # nothing to dereference and nothing to tombstone.
        return None
    settings = get_settings()
    stored = json.loads(record.ap_object_json) if record.ap_object_json else {}
    # Serve the id peers hold, not one rebuilt from whatever handle the caller
    # happened to use: post ids outlive the handle they were minted from.
    object_id = stored.get("id") or settings.post_id(ident, rkey)
    if record.deleted_at is not None:
        return PostView(
            {
                "@context": "https://www.w3.org/ns/activitystreams",
                "id": object_id,
                "type": "Tombstone",
                "formerType": "Note",
                "deleted": record.deleted_at.isoformat() if record.deleted_at else None,
            },
            record.at_uri,
        )
    if record.ap_object_json is None:
        return None
    # Serve the full extension context so JSON-LD-strict consumers keep the
    # neodb relatedWith terms when they re-fetch the Note.
    stored.setdefault("@context", AP_CONTEXT)
    return PostView(stored, record.at_uri)


def get_post_object(ident: str, rkey: str) -> dict[str, Any] | None:
    """Return just the stored ``Note`` / ``Tombstone`` (see :func:`get_post_view`)."""
    view = get_post_view(ident, rkey)
    return view.document if view is not None else None


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
