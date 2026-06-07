"""Mint / look up catalog "work" objects for NeoDB ``withRegardTo`` links.

A work is identified by an external id from the popfeed record
(``imdbId`` / ``tmdbId`` / ``igdbId``) plus its ``creativeWorkType``. We mint a
stable, dereferenceable URL on our own domain so NeoDB instances can resolve
the catalog item a mark/review refers to.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Work

# popfeed creativeWorkType -> NeoDB catalog category.
WORK_TYPE_TO_CATEGORY: dict[str, str] = {
    "movie": "movie",
    "tv_show": "tv",
    "video_game": "game",
    "book": "book",
    "music": "music",
}

# Preferred identifier per work type (first match wins), then any remaining.
_ID_PRIORITY = ("imdbId", "tmdbId", "igdbId", "isbn", "musicbrainzId")


@dataclass
class WorkRef:
    work_key: str
    work_type: str  # popfeed creativeWorkType
    work_id: str
    url: str
    title: str | None = None
    poster_url: str | None = None


def _pick_identifier(identifiers: dict) -> tuple[str, str] | None:
    for key in _ID_PRIORITY:
        if identifiers.get(key):
            return key, str(identifiers[key])
    for key, val in identifiers.items():
        if val:
            return str(key), str(val)
    return None


def work_ref(record: dict) -> WorkRef | None:
    """Derive a :class:`WorkRef` from a popfeed record, or ``None`` if it has
    no resolvable creative-work identifier."""
    identifiers = record.get("identifiers") or {}
    work_type = record.get("creativeWorkType") or "unknown"
    picked = _pick_identifier(identifiers)
    if picked is None:
        return None
    id_key, id_val = picked
    # Namespacing the id by its source keeps keys unambiguous across providers.
    work_id = f"{id_key}-{id_val}"
    work_key = f"{work_type}:{work_id}"
    settings = get_settings()
    return WorkRef(
        work_key=work_key,
        work_type=work_type,
        work_id=work_id,
        url=settings.catalog_id(work_type, work_id),
        title=record.get("title"),
        poster_url=record.get("posterUrl"),
    )


def mint(record: dict) -> WorkRef | None:
    """Resolve a work ref and upsert its catalog row, returning the ref."""
    ref = work_ref(record)
    if ref is None:
        return None
    with session_scope() as session:
        row = session.get(Work, ref.work_key)
        if row is None:
            session.add(
                Work(
                    work_key=ref.work_key,
                    creative_work_type=ref.work_type,
                    title=ref.title,
                    poster_url=ref.poster_url,
                    identifiers_json=json.dumps(record.get("identifiers") or {}),
                )
            )
        else:
            # Backfill metadata we may not have had at first sight.
            if ref.title and not row.title:
                row.title = ref.title
            if ref.poster_url and not row.poster_url:
                row.poster_url = ref.poster_url
    return ref


def category_for(work_type: str) -> str:
    return WORK_TYPE_TO_CATEGORY.get(work_type, "item")
