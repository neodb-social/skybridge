"""Translate popfeed atproto records into NeoDB-compatible ActivityPub.

Output contract (so a real NeoDB/Takahe instance ingests our messages as
marks/reviews while generic Mastodon servers still render the base ``Note``):

* The activity object is a Mastodon-compatible ``Note``.
* NeoDB catalog semantics ride in a ``relatedWith`` array of typed objects
  (``Status`` / ``Review`` / ``Rating`` / ``Comment`` / ``Shelf``), each
  carrying a ``withRegardTo`` pointing at a dereferenceable catalog item.
* The ``Note`` is wrapped in ``Create`` / ``Update`` / ``Delete`` (the latter
  referencing a ``Tombstone``).
"""

from __future__ import annotations

import html
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

# popfeed listType -> NeoDB shelf status (None => no shelf mark, just membership)
_LIST_STATUS = {
    "wishlist": "wishlist",
    "watchlist": "wishlist",
    "want": "wishlist",
    "progress": "progress",
    "watching": "progress",
    "playing": "progress",
    "reading": "progress",
    "complete": "complete",
    "watched": "complete",
    "played": "complete",
    "completed": "complete",
    "finished": "complete",
}


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
) -> dict:
    """Build the AP ``Note`` for a popfeed record, including ``relatedWith``."""
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

    if collection.endswith("feed.list"):
        _populate_list(note, record, handle, rkey, ref)
    elif collection.endswith("feed.listItem"):
        _populate_list_item(note, record, ref)
    else:  # feed.post
        _populate_post(note, record, ref)

    # Drop empty optional arrays to keep payloads tidy.
    if not note["tag"]:
        del note["tag"]
    if not note["relatedWith"]:
        del note["relatedWith"]
    return note


def _populate_post(note: dict, record: dict, ref: works.WorkRef | None) -> None:
    text = record.get("text") or ""
    note["content"] = render_facets(text, record.get("facets"))
    if record.get("title"):
        note["name"] = record["title"]
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        # A generic catalog comment ties the note to the work for NeoDB.
        note["relatedWith"].append(_related("Comment", ref.url, {"content": note["content"]}))


def _populate_list(
    note: dict, record: dict, handle: str, rkey: str, ref: works.WorkRef | None
) -> None:
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
        status = _LIST_STATUS.get(list_type)
        if status:
            note["relatedWith"].append(_related("Status", ref.url, {"status": status}))
        else:
            note["relatedWith"].append(_related("Comment", ref.url, {"content": note["content"]}))


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
    )
    activity = wrap_activity(note, handle=handle, op=operation)
    return note, activity
