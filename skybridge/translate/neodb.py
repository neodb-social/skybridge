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
import json
import re
from datetime import UTC, datetime
from typing import Any

from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Record
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
    """Catalog-item tag in NeoDB's ``ap_object_ref`` shape.

    The ``type`` must be a NeoDB catalog type (Movie/Edition/Game/...) —
    NeoDB's inbound handler only accepts posts whose ``tag`` contains exactly
    one such entry and resolves the item from its ``href``.
    """
    tag = {
        "type": works.ap_type_for(ref.work_type) or "Link",
        "href": ref.url,
        "name": ref.title or ref.work_id,
    }
    if ref.poster_url:
        tag["image"] = ref.poster_url
    return tag


def _related(note: dict, kind: str, work_url: str, extra: dict | None = None) -> dict:
    """A ``relatedWith`` entry in NeoDB's wire shape.

    ``id`` and ``published`` are hard requirements — NeoDB's journal
    ``update_by_ap_object`` reads ``obj["id"]`` / ``obj["published"]`` on
    ingest. ``updated`` advances on Note updates so edits pass the peer's
    staleness check (``edited_time >= updated`` skips the write).
    """
    facet_id = f"{note['id']}#{kind.lower()}"
    obj: dict[str, Any] = {
        "id": facet_id,
        "type": kind,
        "withRegardTo": work_url,
        "attributedTo": note["attributedTo"],
        "href": facet_id,
        "published": note["published"],
        "updated": note.get("updated") or note["published"],
    }
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
    operation: str = "create",
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
    if operation != "create":
        # A fresh `updated` also stamps the relatedWith facets so peers
        # accept the new state (see _related).
        note["updated"] = datetime.now(UTC).isoformat()

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


def _title_html(title: str, ref: works.WorkRef | None) -> str:
    """The work's title for Note content: linked to our catalog page when a
    work was minted, plain emphasis otherwise."""
    if ref is not None:
        return f'<a href="{html.escape(ref.url)}">{html.escape(title)}</a>'
    return f"<strong>{html.escape(title)}</strong>"


def _populate_review(
    note: dict, record: dict, ref: works.WorkRef | None, shelf_status: str | None = None
) -> None:
    title = record.get("title") or "a work"
    text = record.get("text") or ""
    rating = record.get("rating")
    if not isinstance(rating, int | float) or isinstance(rating, bool):
        rating = None

    # A lead-in line keeps the work visible (and linked) on plain Mastodon
    # renderers even when the review text never names it; no ``name`` field —
    # a titled Note renders as an Article-like page (see module docstring).
    if rating is not None:
        lead = f"<p>Rated {_title_html(title, ref)} {rating:g}/{_RATING_BEST}</p>"
    else:
        lead = f"<p>Reviewed {_title_html(title, ref)}</p>"
    text_html = render_facets(text, record.get("facets")) if text else ""
    note["content"] = lead + text_html
    if record.get("containsSpoilers"):
        # Mastodon renders ``summary`` as the content warning text.
        note["sensitive"] = True
        note["summary"] = f"Spoilers: {title}"
    for tag in record.get("tags") or []:
        note["tag"].append({"type": "Hashtag", "name": f"#{tag}"})
    # The poster is deliberately NOT attached as media: peers should show it
    # from the catalog-item tag (_work_tag) instead of a bare image post.
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        if rating is not None:
            note["relatedWith"].append(
                _related(
                    note,
                    "Rating",
                    ref.url,
                    {"value": rating, "best": _RATING_BEST, "worst": _RATING_WORST},
                )
            )
        if text:
            # popfeed review text is untitled (Letterboxd-style), so it maps
            # to a NeoDB Comment on the mark — never a titled Review (which
            # NeoDB renders as an Article-like page). We emit Notes only.
            # The Comment carries just the review text, not the lead-in line.
            note["relatedWith"].append(_related(note, "Comment", ref.url, {"content": text_html}))
        if shelf_status:
            note["relatedWith"].append(_related(note, "Status", ref.url, {"status": shelf_status}))


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
        "attributedTo": note["attributedTo"],
        "published": note["published"],
        "updated": note.get("updated") or note["published"],
    }
    for tag in record.get("tags") or []:
        note["tag"].append({"type": "Hashtag", "name": f"#{tag}"})
    note["relatedWith"].append(shelf)


_LIST_COLLECTION = "social.popfeed.feed.list"

# Dead/unreachable list URIs we've already tried to fetch this process, so a
# list that will never resolve (deleted, wrong PDS, network down) is only
# attempted once — otherwise every listItem pointing at it would separately
# pay a network timeout.
_LIST_FETCH_FAILED: set[str] = set()


def _list_value_label(value: dict) -> str | None:
    """Shared name-or-description extraction for a ``feed.list`` record value."""
    label = value.get("name") or value.get("description") or None
    return label.strip() or None if isinstance(label, str) else None


def _fetch_and_archive_list(list_uri: str) -> dict | None:
    """Fetch an unarchived listItem's parent ``feed.list`` from its author's PDS.

    Lists created before ingestion began are never archived by the pipeline
    (see pipeline.ARCHIVE_ONLY_COLLECTIONS), so ``_list_label`` would
    otherwise fall back to generic wording for them forever. This is a
    best-effort, purely cosmetic fetch: worst case the note just says "a
    list". A successfully fetched list is archived here (via ``Record``) so
    every later listItem referencing it hits the DB instead of paying a
    network round-trip; a failed fetch is remembered in
    ``_LIST_FETCH_FAILED`` so it too is only ever attempted once per process.
    """
    if not list_uri.startswith("at://"):
        return None
    parts = list_uri[len("at://") :].split("/")
    if len(parts) != 3 or parts[1] != _LIST_COLLECTION:
        return None
    if list_uri in _LIST_FETCH_FAILED:
        return None
    did, collection, rkey = parts
    pds = identity.resolve_pds(did)
    resp = (
        identity._http_json(
            f"{pds}/xrpc/com.atproto.repo.getRecord?repo={did}&collection={collection}&rkey={rkey}"
        )
        if pds
        else None
    )
    value = resp.get("value") if isinstance(resp, dict) else None
    if not isinstance(resp, dict) or not isinstance(value, dict):
        _LIST_FETCH_FAILED.add(list_uri)
        return None
    with session_scope() as session:
        session.merge(
            Record(
                at_uri=list_uri,
                did=did,
                collection=collection,
                rkey=rkey,
                cid=resp.get("cid"),
                source_json=json.dumps(value),
                op="create",
            )
        )
    return value


def _list_label(list_uri: Any) -> str | None:
    """Best-effort display label for the parent ``feed.list`` of a listItem.

    Looks up the archived ``feed.list`` record (every processed one is kept
    in the ``Record`` table, see pipeline.ARCHIVE_ONLY_COLLECTIONS) and
    prefers its ``name`` over its ``description``. On a miss, falls back to
    fetching + archiving the list once (see ``_fetch_and_archive_list``)
    rather than giving up outright, since plenty of lists predate ingestion.
    Defensive throughout: any lookup/parse/fetch failure (unknown uri,
    malformed JSON, missing fields, unreachable PDS) just yields ``None`` so
    the caller can fall back to generic wording.
    """
    if not list_uri or not isinstance(list_uri, str):
        return None
    with session_scope() as session:
        row = session.get(Record, list_uri)
        source_json = row.source_json if row is not None else None
    if source_json is not None:
        try:
            value = json.loads(source_json or "{}")
        except Exception:
            return None
    else:
        value = _fetch_and_archive_list(list_uri)
    if not isinstance(value, dict):
        return None
    return _list_value_label(value)


def _populate_list_item(note: dict, record: dict, ref: works.WorkRef | None) -> None:
    # Deliberately ignored for now: tv listItems may carry a
    # ``watchedEpisodes`` array ({tmdbId, seasonNumber, episodeNumber} per
    # episode). NeoDB supports episode-level marks; we only emit the
    # whole-work Status until that mapping is designed.
    title = record.get("title") or "a work"
    list_type = (record.get("listType") or "").lower()
    label = _list_label(record.get("listUri"))
    if label:
        note["content"] = (
            f"<p>Added {_title_html(title, ref)} to <strong>{html.escape(label)}</strong></p>"
        )
    else:
        note["content"] = f"<p>Added {_title_html(title, ref)} to a list</p>"
    # As with reviews, the poster rides on the catalog-item tag, not as a
    # direct media attachment.
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        status = shelf_status(list_type)
        if status:
            note["relatedWith"].append(_related(note, "Status", ref.url, {"status": status}))
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

    if op == "update":
        # Every Update needs its own id: peers dedup activities by id (takahe
        # keys PostInteraction on the activity id), so a reused id makes them
        # drop the edit — or crash on concurrent duplicates. Mastodon-style
        # #updates/{µs}, stamped from the Note's own `updated` time.
        updated = (note or {}).get("updated")
        stamp = datetime.fromisoformat(updated) if updated else datetime.now(UTC)
        if stamp.tzinfo is None:
            # Never local time: the id must not depend on the server's tz.
            stamp = stamp.replace(tzinfo=UTC)
        activity_id = f"{object_id}#updates/{int(stamp.timestamp() * 1_000_000)}"
    else:
        activity_id = f"{object_id}#{op}"

    return {
        "@context": AP_CONTEXT,
        "id": activity_id,
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
        operation=operation,
    )
    activity = wrap_activity(note, handle=handle, op=operation)
    return note, activity
