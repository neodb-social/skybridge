"""Translate popfeed atproto records into NeoDB-compatible ActivityPub.

Output contract (so a real NeoDB/Takahe instance ingests our messages as
marks while generic Mastodon servers still render the base ``Note``):

* The activity object is always a Mastodon-compatible ``Note`` — never an
  ``Article`` or titled ``Review``.
* NeoDB catalog semantics ride in a ``relatedWith`` array of typed objects
  (``Status`` / ``Rating`` / ``Comment``), each carrying a ``withRegardTo``
  pointing at a dereferenceable catalog item. (``Review`` / ``Shelf`` remain
  declared in the JSON-LD context for compatibility but are not emitted.)
* The ``Note`` is wrapped in ``Create`` / ``Update`` / ``Delete`` (the latter
  referencing a ``Tombstone``).
"""

from __future__ import annotations

import html
import re
from datetime import UTC, datetime
from typing import Any

from skybridge.config import get_settings
from skybridge.translate import works

PUBLIC = "https://www.w3.org/ns/activitystreams#Public"

# JSON-LD context. The trailing object maps the NeoDB extension terms onto a
# namespace so consumers that don't understand them ignore them gracefully.
AP_CONTEXT: list[Any] = [
    "https://www.w3.org/ns/activitystreams",
    "https://w3id.org/security/v1",
    {
        "neodb": "https://joinmastodon.org/ns#",
        "withRegardTo": {"@id": "neodb:withRegardTo", "@type": "@id"},
        "relatedWith": "neodb:relatedWith",
        "Status": "neodb:Status",
        "Rating": "neodb:Rating",
        "Review": "neodb:Review",
        "Comment": "neodb:Comment",
        "Shelf": "neodb:Shelf",
        "status": "neodb:status",
        "sensitive": "as:sensitive",
    },
]

# popfeed listType -> NeoDB shelf status (None => no shelf mark, just
# membership). Keys match either the whole listType or a single token of a
# compound one ("watched_movies", "books_to_read"); see _shelf_status. Covers
# the do / doing / done / dropped verb per media type (watch, play, read,
# listen), since popfeed names its system lists "<verb>_<media-plural>".
_LIST_STATUS = {
    # do (wishlist)
    "wishlist": "wishlist",
    "watchlist": "wishlist",
    "want": "wishlist",
    "backlog": "wishlist",
    "queued": "wishlist",
    "planned": "wishlist",
    # doing (progress)
    "progress": "progress",
    "current": "progress",
    "watching": "progress",
    "playing": "progress",
    "reading": "progress",
    "listening": "progress",
    # done (complete)
    "complete": "complete",
    "completed": "complete",
    "finished": "complete",
    "done": "complete",
    "watched": "complete",
    "played": "complete",
    "read": "complete",
    "listened": "complete",
    # dropped
    "dropped": "dropped",
    "abandoned": "dropped",
    "shelved": "dropped",
    "dnf": "dropped",
}

# popfeed ratings are on a 0-10 scale (half-star increments doubled).
_RATING_BEST = 10
_RATING_WORST = 1


def shelf_status(list_type: str) -> str | None:
    """Map a popfeed ``listType`` to a NeoDB shelf status.

    listTypes can be compound (``watched_movies``, ``books_to_read``), so after
    an exact lookup fall back to matching individual tokens.
    """
    if not list_type:
        return None
    status = _LIST_STATUS.get(list_type)
    if status:
        return status
    tokens = re.split(r"[_\-\s]+", list_type)
    for i, token in enumerate(tokens):
        status = _LIST_STATUS.get(token)
        if status is None:
            continue
        # "to_<verb>" ("to_read", "books_to_read") is a want-list even though
        # the bare past-tense verb ("read") means completed.
        if status == "complete" and i > 0 and tokens[i - 1] == "to":
            return "wishlist"
        return status
    return None


def _published(record: dict, time_us: int | None) -> str:
    """Best-effort ISO-8601 published timestamp.

    popfeed records sometimes carry ``createdAt`` as an empty object; fall back
    to the firehose ``time_us`` and finally to *now*.
    """
    created = record.get("createdAt") or record.get("addedAt")
    if isinstance(created, str) and created:
        return created
    if time_us:
        return datetime.fromtimestamp(time_us / 1_000_000, tz=UTC).isoformat()
    return datetime.now(UTC).isoformat()


def render_facets(text: str, facets: list[dict] | None) -> str:
    """Render atproto richtext ``facets`` (byte-indexed links) into HTML.

    Mirrors app.bsky richtext: indices are byte offsets into the UTF-8 text.
    """
    raw = text.encode("utf-8")
    if not facets:
        return f"<p>{html.escape(text)}</p>"
    spans = sorted(facets, key=lambda f: f.get("index", {}).get("byteStart", 0))
    out: list[str] = []
    cursor = 0
    for facet in spans:
        idx = facet.get("index", {})
        start, end = idx.get("byteStart"), idx.get("byteEnd")
        if start is None or end is None or start < cursor:
            continue
        out.append(html.escape(raw[cursor:start].decode("utf-8", "ignore")))
        slice_text = raw[start:end].decode("utf-8", "ignore")
        link = next(
            (
                f["uri"]
                for f in facet.get("features", [])
                if f.get("$type", "").endswith("#link") and f.get("uri")
            ),
            None,
        )
        if link:
            out.append(
                f'<a href="{html.escape(link)}" rel="nofollow noopener">'
                f"{html.escape(slice_text)}</a>"
            )
        else:
            out.append(html.escape(slice_text))
        cursor = end
    out.append(html.escape(raw[cursor:].decode("utf-8", "ignore")))
    return "<p>" + "".join(out) + "</p>"


def _work_tag(ref: works.WorkRef) -> dict:
    return {
        "type": "Link",
        "href": ref.url,
        "name": ref.title or ref.work_id,
        "mediaType": "application/activity+json",
    }


def _related(kind: str, work_url: str, extra: dict | None = None) -> dict:
    obj = {"type": kind, "withRegardTo": work_url}
    if extra:
        obj.update(extra)
    return obj


def build_note(
    *,
    did: str,
    handle: str,
    collection: str,
    rkey: str,
    record: dict,
    time_us: int | None,
    ref: works.WorkRef | None,
    shelf_status: str | None = None,
) -> dict:
    """Build the AP ``Note`` for a popfeed record, including ``relatedWith``.

    ``shelf_status`` folds a companion listItem's shelf mark into a review's
    Note so one user action ("watched + rated") stays one AP post (see
    pipeline merge handling).
    """
    settings = get_settings()
    actor = settings.actor_id(handle)
    object_id = settings.post_id(handle, rkey)
    published = _published(record, time_us)

    note: dict[str, Any] = {
        "id": object_id,
        "type": "Note",
        "attributedTo": actor,
        "published": published,
        "to": [PUBLIC],
        "cc": [f"{actor}/followers"],
        "url": object_id,
        "tag": [],
        "relatedWith": [],
    }

    # (Legacy social.popfeed.feed.post — free text about a work, superseded by
    # feed.review in 2025 — is no longer bridged; see config.WANTED_COLLECTIONS.)
    if collection.endswith("feed.list"):
        _populate_list(note, record, handle, rkey, ref)
    elif collection.endswith("feed.listItem"):
        _populate_list_item(note, record, ref)
    elif collection.endswith("feed.review"):
        _populate_review(note, record, ref, shelf_status)
    else:
        raise ValueError(f"no AP mapping for collection {collection!r}")

    # Drop empty optional arrays to keep payloads tidy.
    if not note["tag"]:
        del note["tag"]
    if not note["relatedWith"]:
        del note["relatedWith"]
    return note


def _populate_review(
    note: dict, record: dict, ref: works.WorkRef | None, shelf_status: str | None = None
) -> None:
    title = record.get("title") or "a work"
    text = record.get("text") or ""
    rating = record.get("rating")
    if not isinstance(rating, int | float) or isinstance(rating, bool):
        rating = None

    if text:
        note["content"] = render_facets(text, record.get("facets"))
    elif rating is not None:
        note["content"] = (
            f"<p>Rated <strong>{html.escape(title)}</strong> {rating:g}/{_RATING_BEST}</p>"
        )
    else:
        note["content"] = f"<p>Reviewed <strong>{html.escape(title)}</strong></p>"
    if record.get("title"):
        note["name"] = record["title"]
    if record.get("containsSpoilers"):
        # Mastodon renders ``summary`` as the content warning text.
        note["sensitive"] = True
        note["summary"] = f"Spoilers: {title}"
    for tag in record.get("tags") or []:
        note["tag"].append({"type": "Hashtag", "name": f"#{tag}"})
    if record.get("posterUrl"):
        note["attachment"] = [
            {
                "type": "Document",
                "mediaType": "image/jpeg",
                "url": record["posterUrl"],
                "name": title,
            }
        ]
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        if rating is not None:
            note["relatedWith"].append(
                _related(
                    "Rating",
                    ref.url,
                    {"value": rating, "best": _RATING_BEST, "worst": _RATING_WORST},
                )
            )
        if text:
            # popfeed review text is untitled (Letterboxd-style), so it maps
            # to a NeoDB Comment on the mark — never a titled Review (which
            # NeoDB renders as an Article-like page). We emit Notes only.
            note["relatedWith"].append(_related("Comment", ref.url, {"content": note["content"]}))
        if shelf_status:
            note["relatedWith"].append(_related("Status", ref.url, {"status": shelf_status}))


def _populate_list(
    note: dict, record: dict, handle: str, rkey: str, ref: works.WorkRef | None
) -> None:
    # Currently unreachable from the pipeline: feed.list is archive-only (see
    # pipeline.ARCHIVE_ONLY_COLLECTIONS) because we don't emit AP posts for
    # lists/collections yet. Kept (and unit-tested) as the intended mapping
    # for when custom lists are bridged as NeoDB Collections.
    settings = get_settings()
    name = record.get("name") or "Untitled list"
    desc = record.get("description") or ""
    note["name"] = name
    note["content"] = f"<p>Created list <strong>{html.escape(name)}</strong></p>" + (
        f"<p>{html.escape(desc)}</p>" if desc else ""
    )
    shelf = {
        "type": "Shelf",
        "id": settings.url(f"users/{handle}/lists/{rkey}"),
        "name": name,
        "summary": desc,
        "totalItems": 0,
    }
    for tag in record.get("tags") or []:
        note["tag"].append({"type": "Hashtag", "name": f"#{tag}"})
    note["relatedWith"].append(shelf)


def _populate_list_item(note: dict, record: dict, ref: works.WorkRef | None) -> None:
    # Deliberately ignored for now: tv listItems may carry a
    # ``watchedEpisodes`` array ({tmdbId, seasonNumber, episodeNumber} per
    # episode). NeoDB supports episode-level marks; we only emit the
    # whole-work Status until that mapping is designed.
    title = record.get("title") or "a work"
    list_type = (record.get("listType") or "").lower()
    note["content"] = f"<p>Added <strong>{html.escape(title)}</strong> to a list</p>"
    note["name"] = title
    if record.get("posterUrl"):
        note["attachment"] = [
            {
                "type": "Document",
                "mediaType": "image/jpeg",
                "url": record["posterUrl"],
                "name": title,
            }
        ]
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        status = shelf_status(list_type)
        if status:
            note["relatedWith"].append(_related("Status", ref.url, {"status": status}))
        # No shelf status => collection membership, which the pipeline archives
        # without AP emission (Collections aren't bridged yet), so no facet.


def wrap_activity(note: dict, *, handle: str, op: str, prior_object_id: str | None = None) -> dict:
    """Wrap a ``Note`` (or a tombstone, for deletes) in a C/U/D activity."""
    settings = get_settings()
    actor = settings.actor_id(handle)
    object_id = note.get("id") if isinstance(note, dict) else prior_object_id

    if op == "delete":
        target = prior_object_id or object_id
        activity_type = "Delete"
        obj: Any = {"id": target, "type": "Tombstone", "formerType": "Note"}
    else:
        activity_type = "Update" if op == "update" else "Create"
        obj = note

    return {
        "@context": AP_CONTEXT,
        "id": f"{object_id}#{op}",
        "type": activity_type,
        "actor": actor,
        "published": (note or {}).get("published") or datetime.now(UTC).isoformat(),
        "to": [PUBLIC],
        "cc": [f"{actor}/followers"],
        "object": obj,
    }


def translate(
    *,
    did: str,
    handle: str,
    collection: str,
    rkey: str,
    record: dict | None,
    operation: str,
    time_us: int | None,
    ref: works.WorkRef | None = None,
    prior_object_id: str | None = None,
    shelf_status: str | None = None,
) -> tuple[dict | None, dict]:
    """Translate one record op into ``(note, activity)``.

    For deletes ``record`` is ``None`` and ``note`` is ``None``; the activity is
    a ``Delete`` referencing the prior object's id (a ``Tombstone``).
    """
    if operation == "delete" or record is None:
        activity = wrap_activity({}, handle=handle, op="delete", prior_object_id=prior_object_id)
        return None, activity
    note = build_note(
        did=did,
        handle=handle,
        collection=collection,
        rkey=rkey,
        record=record,
        time_us=time_us,
        ref=ref,
        shelf_status=shelf_status,
    )
    activity = wrap_activity(note, handle=handle, op=operation)
    return note, activity
