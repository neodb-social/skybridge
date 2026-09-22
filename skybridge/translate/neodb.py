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
* The work link in Note ``content`` carries NeoDB's ``/~neodb~/`` URL marker
  so peer instances localize it for their readers; ``tag`` hrefs stay
  unmarked.
* Every user string we lay out ourselves (titles, list names) is escaped, but
  a *review body* is the writing app's own HTML in practice, so it goes
  through :mod:`skybridge.translate.richtext`'s allowlist sanitizer instead
  (see :func:`_review_body`).
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from skybridge.atproto import identity
from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Record
from skybridge.translate import bookhive, richtext, teal, works

log = logging.getLogger("skybridge.translate")

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


def list_item_status(record: dict) -> str | None:
    """Shelf status a listItem record marks on its work.

    An episode list-add is bridged as activity on the parent season (see
    works.season_view), where it always means "watching": one episode never
    completes (or wishlists) a season, whatever the list's own verb says.
    """
    list_type = record.get("listType")
    status = shelf_status(list_type.lower() if isinstance(list_type, str) else "")
    if status and record.get("creativeWorkType") == works.EPISODE_TYPE:
        return "progress"
    return status


def _published(record: dict, event_time: str | None) -> str:
    """Best-effort ISO-8601 published timestamp.

    popfeed records sometimes carry ``createdAt`` as an empty object; fall back
    to the firehose event time and finally to *now*. A teal.fm play has no
    ``createdAt`` at all: its ``playedTime`` is when playback began, which is
    the moment the Note is about.

    The firehose value arrives as ISO-8601 already (Jetstream v2 sends it that
    way, and the v1/archive adapters render their epoch microseconds before
    handing it over), so it is only re-parsed to normalise the format — no
    integer/float round trip, which at ~1.8e15 microseconds sat close enough
    to float64's exact-integer limit to risk a microsecond of drift.
    """
    created = record.get("createdAt") or record.get("addedAt") or record.get("playedTime")
    if isinstance(created, str) and created:
        return created
    if event_time:
        try:
            return datetime.fromisoformat(event_time).astimezone(UTC).isoformat()
        except ValueError:
            log.debug("unparseable event time: %r", event_time)
    return datetime.now(UTC).isoformat()


def _byte_index(facet: dict) -> dict:
    idx = facet.get("index")
    return idx if isinstance(idx, dict) else {}


def _text(value: Any) -> str:
    """A record field as text: the string itself, or empty for anything else."""
    return value if isinstance(value, str) else ""


def render_facets(text: str, facets: list[dict] | None) -> str:
    """Render atproto richtext ``facets`` (byte-indexed links) into HTML.

    Mirrors app.bsky richtext: indices are byte offsets into the UTF-8 text.
    """
    raw = text.encode("utf-8")
    # Records are not validated against their lexicon before they reach us,
    # so a facet list that is not a list, or a facet that is not an object,
    # is treated as "no facets" rather than allowed to raise mid-ingest.
    facets = [f for f in facets if isinstance(f, dict)] if isinstance(facets, list) else []
    if not facets:
        return f"<p>{html.escape(text)}</p>"
    spans = sorted(facets, key=lambda f: _byte_index(f).get("byteStart", 0))
    out: list[str] = []
    cursor = 0
    for facet in spans:
        idx = _byte_index(facet)
        start, end = idx.get("byteStart"), idx.get("byteEnd")
        if not isinstance(start, int) or not isinstance(end, int) or start < cursor:
            continue
        out.append(html.escape(raw[cursor:start].decode("utf-8", "ignore")))
        slice_text = raw[start:end].decode("utf-8", "ignore")
        features = facet.get("features")
        link = next(
            (
                f["uri"]
                for f in (features if isinstance(features, list) else [])
                if isinstance(f, dict)
                and isinstance(f.get("$type"), str)
                and f["$type"].endswith("#link")
                and isinstance(f.get("uri"), str)
            ),
            None,
        )
        # Facet URIs are author-controlled: only http(s) may become a live
        # link, or a javascript: URI would execute wherever the content HTML
        # is embedded (our post page, AP peers).
        if link and link.startswith(("http://", "https://")):
            out.append(
                f'<a href="{html.escape(link)}" rel="nofollow noopener">'
                f"{html.escape(slice_text)}</a>"
            )
        else:
            out.append(html.escape(slice_text))
        cursor = end
    out.append(html.escape(raw[cursor:].decode("utf-8", "ignore")))
    return "<p>" + "".join(out) + "</p>"


def _hashtags(record: dict) -> list[str]:
    tags = record.get("tags")
    if not isinstance(tags, list):
        return []
    return [tag for tag in tags if isinstance(tag, str) and tag]


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


def _addressing(actor: str, *, unlisted: bool) -> tuple[list[str], list[str]]:
    """``(to, cc)`` for a bridged post. Public unless ``unlisted``.

    Unlisted is the AP spelling of Mastodon's "unlisted" visibility: the
    author's followers are the primary audience and ``as:Public`` appears only
    in ``cc``. Receiving servers still accept and file the post — takahe reads
    ``cc``-public as unlisted, and neodb-relay redistributes on either field —
    but Mastodon-family peers keep it out of the public, local, federated and
    hashtag timelines, and out of trends.

    Used for an author carrying Bluesky's ``!no-unauthenticated`` label. It
    reduces reach; it is not a privacy boundary, because ActivityPub has no
    way to say "signed-in readers only" while staying publicly federated.
    """
    followers = f"{actor}/followers"
    return ([followers], [PUBLIC]) if unlisted else ([PUBLIC], [followers])


def build_note(
    *,
    did: str,
    handle: str,
    collection: str,
    rkey: str,
    record: dict,
    event_time: str | None,
    ref: works.WorkRef | None,
    shelf_status: str | None = None,
    operation: str = "create",
    object_id: str | None = None,
    unlisted: bool = False,
) -> dict:
    """Build the AP ``Note`` for a popfeed record, including ``relatedWith``.

    ``shelf_status`` folds a companion listItem's shelf mark into a review's
    Note so one user action ("watched + rated") stays one AP post (see
    pipeline merge handling).

    ``object_id`` pins the Note to the id peers already hold. Without it an
    update re-mints the id from the *current* handle, so a rename would
    orphan the published Note and update an id nobody ever received.

    ``unlisted`` drops the post out of the public timelines; see _addressing.
    """
    settings = get_settings()
    actor = settings.actor_id(handle)
    object_id = object_id or settings.post_id(handle, rkey)
    published = _published(record, event_time)
    to, cc = _addressing(actor, unlisted=unlisted)

    note: dict[str, Any] = {
        "id": object_id,
        "type": "Note",
        "attributedTo": actor,
        "published": published,
        "to": to,
        "cc": cc,
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
    if collection == bookhive.BOOK_COLLECTION:
        _populate_book(note, record, ref)
    elif collection in teal.PLAY_COLLECTIONS:
        _populate_play(note, record, ref)
    elif collection.endswith("feed.list"):
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


def _marker_url(url: str) -> str:
    """Insert NeoDB's ``~neodb~`` marker after the origin of *url*.

    NeoDB peers rewrite ``href="https://domain/~neodb~/path"`` in incoming
    post content to their local search resolver, so their readers land on
    their own instance's copy of the work. Everyone else resolves the link
    via our ``/~neodb~/{path}`` redirect route (see main.py).
    """
    origin, sep, path = url.partition("://")[2].partition("/")
    scheme = url.split("://", 1)[0]
    return f"{scheme}://{origin}/~neodb~{sep}{path}"


def _title_html(title: str, ref: works.WorkRef | None) -> str:
    """The work's title for Note content: linked to our catalog page (with
    NeoDB's ``~neodb~`` marker so peers localize the link) when a work was
    minted, plain emphasis otherwise."""
    if ref is not None:
        return f'<a href="{html.escape(_marker_url(ref.url))}">{html.escape(title)}</a>'
    return f"<strong>{html.escape(title)}</strong>"


def _review_body(text: str, facets: list[dict] | None = None) -> str:
    """Render a review body into HTML.

    Two mutually exclusive shapes reach us. A record with ``facets`` declares
    its text to be atproto richtext, so it renders through
    :func:`render_facets` and every character is escaped — facet indices are
    byte offsets into the raw text, so markup inside it would shift them and
    the link spans would land on the wrong words. A record without facets may
    carry the writing app's editor HTML (see :mod:`skybridge.translate.richtext`),
    so it goes through the sanitizer instead.
    """
    if facets:
        return render_facets(text, facets)
    return richtext.review_html(text)


def _populate_review(
    note: dict, record: dict, ref: works.WorkRef | None, shelf_status: str | None = None
) -> None:
    # Prefer the minted work's (normalized) title: popfeed sometimes labels a
    # show-typed record with the watched episode's title (see
    # works.normalize_title); the Note should name the work being marked.
    title = (ref.title if ref is not None else None) or _text(record.get("title")) or "a work"
    text = _text(record.get("text"))
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
    text_html = _review_body(text, record.get("facets")) if text else ""
    note["content"] = lead + text_html
    if record.get("containsSpoilers"):
        # Mastodon renders ``summary`` as the content warning text.
        note["sensitive"] = True
        note["summary"] = f"Spoilers: {title}"
    for tag in _hashtags(record):
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


# NeoDB shelf status -> the reading verb that leads a status-only book Note
# (one with no rating and no review text), so an unrated shelf-add still reads
# naturally on a plain Mastodon renderer.
_BOOK_STATUS_LEAD = {
    "wishlist": "Wants to read",
    "progress": "Reading",
    "complete": "Finished reading",
    "dropped": "Abandoned",
}


def _populate_book(note: dict, record: dict, ref: works.WorkRef | None) -> None:
    """Populate the Note for a ``buzz.bookhive.book`` record.

    A single BookHive book carries the shelf status, star rating (1-10) and
    review text together, so its one Note may carry ``Status`` + ``Rating`` +
    ``Comment`` at once — the same facets popfeed reassembles from a
    review/listItem pair (see :func:`_populate_review`).
    """
    title = (ref.title if ref is not None else None) or _text(record.get("title")) or "a book"
    review = record.get("review") if isinstance(record.get("review"), str) else ""
    stars = record.get("stars")
    if not isinstance(stars, int) or isinstance(stars, bool):
        stars = None
    status = bookhive.shelf_status(record)

    if stars is not None:
        lead = f"<p>Rated {_title_html(title, ref)} {stars:g}/{_RATING_BEST}</p>"
    elif review:
        lead = f"<p>Reviewed {_title_html(title, ref)}</p>"
    elif status:
        lead = f"<p>{_BOOK_STATUS_LEAD[status]} {_title_html(title, ref)}</p>"
    else:
        lead = f"<p>Added {_title_html(title, ref)}</p>"
    # BookHive carries no facets: the review is editor HTML (see _review_body).
    review_html = _review_body(review) if review else ""
    note["content"] = lead + review_html

    # As with reviews, the cover rides on the catalog-item tag, not as media.
    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        if stars is not None:
            note["relatedWith"].append(
                _related(
                    note,
                    "Rating",
                    ref.url,
                    {"value": stars, "best": _RATING_BEST, "worst": _RATING_WORST},
                )
            )
        if review:
            # An untitled Comment on the mark (never a titled Review, which
            # NeoDB renders Article-like); carries only the review text.
            note["relatedWith"].append(_related(note, "Comment", ref.url, {"content": review_html}))
        if status:
            note["relatedWith"].append(_related(note, "Status", ref.url, {"status": status}))


def _populate_play(note: dict, record: dict, ref: works.WorkRef | None) -> None:
    """Populate the Note for a teal.fm play, as the mark on its *release*.

    One Note stands for every play of one listening session — see
    ``pipeline._process_play`` — so the content must not depend on which
    track or how many tracks were played: it names the album and the artists
    of the anchoring play, nothing per-track, and carries a ``Status`` of
    ``progress`` (NeoDB's "listening"). A scrobbler reports that the author is
    playing the album, never that they reached its end, so the mark says they
    are listening and no later activity completes it. No Rating and no
    Comment: a scrobble has neither.
    """
    title = (ref.title if ref is not None else None) or teal.release_title(record) or "an album"
    artists = teal.artist_names(record)
    lead = f"<p>Listening to {_title_html(title, ref)}"
    if artists:
        lead += f" by {html.escape(', '.join(artists))}"
    note["content"] = lead + "</p>"

    if ref is not None:
        note["tag"].append(_work_tag(ref))
        note["tag"].append({"type": "Hashtag", "name": f"#{works.category_for(ref.work_type)}"})
        note["relatedWith"].append(_related(note, "Status", ref.url, {"status": "progress"}))


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
    for tag in _hashtags(record):
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


def ensure_list_archived(list_uri: Any) -> None:
    """Archive a listItem's parent list now if it is not archived yet.

    The pipeline calls this off the event loop before translating, so that
    :func:`_list_label` — which runs inside the synchronous translation and
    would otherwise fetch inline — finds the row (or the negative-cache entry)
    and never touches the network on the loop. Blocking; call via
    ``asyncio.to_thread``.
    """
    if not isinstance(list_uri, str) or not list_uri:
        return
    with session_scope() as session:
        if session.get(Record, list_uri) is not None:
            return
    _fetch_and_archive_list(list_uri)


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
    # The linked/displayed title is the minted work's, so an episode add
    # bridged as season activity names the season, not the episode.
    title = (ref.title if ref is not None else None) or _text(record.get("title")) or "a work"
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
        status = list_item_status(record)
        if status:
            note["relatedWith"].append(_related(note, "Status", ref.url, {"status": status}))
        # No shelf status => collection membership, which the pipeline archives
        # without AP emission (Collections aren't bridged yet), so no facet.


def wrap_activity(
    note: dict,
    *,
    handle: str,
    op: str,
    prior_object_id: str | None = None,
    unlisted: bool = False,
) -> dict:
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

    to, cc = _addressing(actor, unlisted=unlisted)
    return {
        "@context": AP_CONTEXT,
        "id": activity_id,
        "type": activity_type,
        "actor": actor,
        "published": (note or {}).get("published") or datetime.now(UTC).isoformat(),
        "to": to,
        "cc": cc,
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
    event_time: str | None,
    ref: works.WorkRef | None = None,
    prior_object_id: str | None = None,
    shelf_status: str | None = None,
    unlisted: bool = False,
) -> tuple[dict | None, dict]:
    """Translate one record op into ``(note, activity)``.

    For deletes ``record`` is ``None`` and ``note`` is ``None``; the activity is
    a ``Delete`` referencing the prior object's id (a ``Tombstone``).

    ``unlisted`` reflects the author's atproto visibility preference; see
    _addressing. A ``Delete`` carries it too, so a retraction is addressed the
    same way as the post it retracts.
    """
    if operation == "delete" or record is None:
        activity = wrap_activity(
            {},
            handle=handle,
            op="delete",
            prior_object_id=prior_object_id,
            unlisted=unlisted,
        )
        return None, activity
    note = build_note(
        did=did,
        handle=handle,
        collection=collection,
        rkey=rkey,
        record=record,
        event_time=event_time,
        ref=ref,
        shelf_status=shelf_status,
        operation=operation,
        object_id=prior_object_id,
        unlisted=unlisted,
    )
    activity = wrap_activity(note, handle=handle, op=operation, unlisted=unlisted)
    return note, activity
