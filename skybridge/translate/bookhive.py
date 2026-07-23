"""Adapter for BookHive (``buzz.bookhive.*``) records.

BookHive (https://github.com/nperez0111/bookhive) is a separate AT Protocol
app — a decentralized Goodreads — with its own lexicons. Unlike popfeed, which
splits a user action across ``feed.review`` + ``feed.listItem`` records, a
BookHive book lives in a *single* ``buzz.bookhive.book`` record that already
carries the shelf status, star rating and review text together. It therefore
bridges to ONE AP ``Note`` (Status + Rating + Comment) through the pipeline's
simple, non-paired translate path — none of the popfeed pair-merging applies.

This module isolates the BookHive specifics: the collection name, its shelf
status vocabulary, and normalizing a book record into the generic work shape
:func:`skybridge.translate.works.mint` consumes. The work type is ``book``
(same as a popfeed book), so a BookHive book and a popfeed book that share an
ISBN merge into one catalog entry automatically.
"""

from __future__ import annotations

from typing import Any

# The only BookHive collection we bridge. Others are deliberately skipped:
#   buzz.bookhive.buzz — a comment/reply on a book (strongRef to book+parent);
#     like popfeed's feed.reaction, no per-work mark of its own, not bridged.
#   buzz.bookhive.hiveBook / buzz.bookhive.catalogBook — the app's own catalog
#     entries (book metadata), not user activity; nothing to mark.
BOOK_COLLECTION = "buzz.bookhive.book"
COLLECTIONS: frozenset[str] = frozenset({BOOK_COLLECTION})

WORK_TYPE = "book"

# BookHive status token (the part after ``buzz.bookhive.defs#``) -> NeoDB shelf
# status. Mirrors the reading verbs NeoDB uses for books (do / doing / done /
# dropped == want / reading / finished / abandoned).
_STATUS_MAP = {
    "wanttoread": "wishlist",
    "reading": "progress",
    "finished": "complete",
    "abandoned": "dropped",
}


def is_book(record: dict[str, Any]) -> bool:
    """Whether *record* is a ``buzz.bookhive.book`` record.

    Keys on ``$type`` (present on both firehose commits and listRecords
    values); ``hiveId`` is a secondary tell for records that omit ``$type``.
    """
    if record.get("$type") == BOOK_COLLECTION:
        return True
    return "$type" not in record and "hiveId" in record


def shelf_status(record: dict[str, Any]) -> str | None:
    """Map a book record's ``status`` to a NeoDB shelf status, or ``None``.

    Accepts both the fully-qualified token (``buzz.bookhive.defs#finished``)
    and a bare one (``finished``), case-insensitively.
    """
    status = record.get("status")
    if not isinstance(status, str) or not status:
        return None
    token = status.rpartition("#")[2] or status
    return _STATUS_MAP.get(token.lower())


def _identifiers(record: dict[str, Any]) -> dict[str, str]:
    """Flatten a book record's external identifiers into a single dict.

    Merges the nested ``identifiers`` ref (isbn10/isbn13/goodreadsId/hiveId)
    with the top-level ``hiveId``, keeping only non-empty string/number values.
    """
    ids: dict[str, str] = {}
    nested = record.get("identifiers")
    if isinstance(nested, dict):
        for key, val in nested.items():
            if val not in (None, ""):
                ids[str(key)] = str(val)
    hive_id = record.get("hiveId")
    if hive_id not in (None, "") and "hiveId" not in ids:
        ids["hiveId"] = str(hive_id)
    return ids


def as_work_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a book record into the generic shape ``works.mint`` expects.

    Every BookHive book is a ``book`` work; its identity comes from the merged
    identifiers (``hiveId`` is always present, so a work always mints). The
    cover is a PDS blob rather than a URL, so no poster is derived here.
    """
    return {
        "$type": BOOK_COLLECTION,
        "creativeWorkType": WORK_TYPE,
        "title": record.get("title"),
        "identifiers": _identifiers(record),
    }
