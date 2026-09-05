"""FastAPI application: ActivityPub endpoints + stats/archive UI.

The lifespan starts the in-process delivery worker (and, when
``SKYBRIDGE_INGEST=1``, the live Jetstream consumer). All hostnames come from
:mod:`skybridge.config`, so the same app serves any configured ``SKYBRIDGE_DOMAIN``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from skybridge import admin, neodb_servers, optout, pipeline, sessions, telemetry
from skybridge.activitypub import nodeinfo, objects, webfinger
from skybridge.activitypub.actors import RELAY_DID, get_relay_keys, person_actor, relay_actor
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.activitypub.inbox import handle_inbox
from skybridge.activitypub.relays import reconcile_relays
from skybridge.atproto import archive as archive_replay
from skybridge.atproto import backfill, discover, identity, oauth
from skybridge.config import get_settings
from skybridge.db import init_db, session_scope
from skybridge.models import BridgedActor, Cursor, Record, Work
from skybridge.stats import collect_stats, usage_refresh_loop
from skybridge.translate import works

log = logging.getLogger("skybridge")

AP_CONTENT_TYPE = "application/activity+json"
_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "web"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.environ.get("SKYBRIDGE_LOG", "INFO"))
    telemetry.init_sentry()  # first, so failures below are captured when enabled
    init_db()
    get_relay_keys()  # operator-provided; fail fast if missing
    worker = DeliveryWorker()
    worker.start()
    app.state.worker = worker

    relay_task = asyncio.create_task(reconcile_relays(worker), name="relay-reconcile")
    app.state.relay_task = relay_task

    servers_task = asyncio.create_task(neodb_servers.refresh_loop(), name="neodb-servers")
    app.state.servers_task = servers_task

    ingest_task: asyncio.Task | None = None
    if os.environ.get("SKYBRIDGE_INGEST") == "1":
        from skybridge.atproto.jetstream import run as jetstream_run

        ingest_task = asyncio.create_task(jetstream_run(worker), name="ingest")
    app.state.ingest_task = ingest_task

    # Runs archive imports requested from the admin view or the CLI, and
    # resumes one left unfinished by a restart.
    archive_task = asyncio.create_task(
        archive_replay.watch_jobs(worker=worker), name="archive-jobs"
    )
    app.state.archive_task = archive_task

    # Keeps the resolved admin set warm off-thread, so is_admin() never has to
    # do a blocking identity resolution inside a request.
    admin_task = asyncio.create_task(admin.refresh_loop(), name="admin-refresh")
    app.state.admin_task = admin_task

    # Counts the NodeInfo document serves. Off in its own slow loop because
    # they are whole-table aggregates and nothing waits on them being current.
    usage_task = asyncio.create_task(usage_refresh_loop(), name="usage-refresh")
    app.state.usage_task = usage_task
    try:
        yield
    finally:
        if ingest_task is not None:
            ingest_task.cancel()
        archive_task.cancel()
        with suppress(asyncio.CancelledError):
            await archive_task
        admin_task.cancel()
        with suppress(asyncio.CancelledError):
            await admin_task
        usage_task.cancel()
        with suppress(asyncio.CancelledError):
            await usage_task
        if not relay_task.done():
            relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await relay_task
        servers_task.cancel()
        with suppress(asyncio.CancelledError):
            await servers_task
        # Imports must stop enqueueing before worker.stop() awaits the queue
        # drain, or a long replay stalls shutdown / feeds a dead queue.
        await backfill.cancel_all_imports()
        await archive_replay.cancel()
        await worker.stop()


app = FastAPI(title="Skybridge", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _wants_ap(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "activity+json" in accept or "ld+json" in accept or "application/json" in accept


def ap_response(doc: dict[str, Any], status: int = 200) -> Response:
    return JSONResponse(doc, status_code=status, media_type=AP_CONTENT_TYPE)


def _handle_of(did: str) -> str:
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        return row.handle if row else did


def _actor_for_ident(session: Session, ident: str) -> BridgedActor | None:
    """Resolve a ``/users/<ident>`` path segment to a bridged (non-relay) actor.

    Accepts the live handle, the DID, or a handle the author has since renamed
    away from — every URL we ever published stays dereferenceable.
    """
    did = identity.did_for_ident(session, ident)
    if did is None or did == RELAY_DID:
        return None
    return session.get(BridgedActor, did)


def _stored_object_id(ap_object_json: str | None) -> str | None:
    """The id of a stored ``Note``, or ``None`` when it can't be read."""
    try:
        obj = json.loads(ap_object_json or "")
    except ValueError:
        return None
    return obj.get("id") if isinstance(obj, dict) else None


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #
@app.get("/.well-known/webfinger")
async def well_known_webfinger(resource: str = "") -> Response:
    jrd = webfinger.resolve(resource)
    if jrd is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse(jrd, media_type="application/jrd+json")


@app.get("/.well-known/nodeinfo")
async def well_known_nodeinfo() -> Response:
    return JSONResponse(nodeinfo.discovery())


@app.get("/nodeinfo/2.1")
async def nodeinfo_document() -> Response:
    return JSONResponse(nodeinfo.document())


@app.get("/robots.txt")
async def robots_txt() -> Response:
    return PlainTextResponse("User-agent: *\nAllow: /\n")


# --------------------------------------------------------------------------- #
# Actors + inboxes
# --------------------------------------------------------------------------- #
@app.get("/actor")
async def get_relay_actor() -> Response:
    return ap_response(relay_actor())


@app.post("/actor/inbox")
@app.post("/inbox")
async def relay_inbox(request: Request) -> Response:
    activity = await request.json()
    status = await handle_inbox(
        activity,
        target_actor_id=get_settings().relay_actor_id,
        worker=getattr(app.state, "worker", None),
    )
    return Response(status_code=status)


@app.get("/users/{ident}")
async def get_user(ident: str, request: Request) -> Response:
    with session_scope() as session:
        actor = _actor_for_ident(session, ident)
        # An opted-out actor is Gone: don't serve its profile.
        if actor is not None and actor.opted_out:
            return JSONResponse({"error": "gone"}, status_code=410)
        # Reached under a retired handle: send callers to the canonical URL
        # rather than serving one actor under two ids.
        if actor is not None and ident != actor.handle:
            return RedirectResponse(get_settings().actor_id(actor.handle), status_code=301)
        doc = person_actor(actor) if actor else None
        profile: dict[str, Any] | None = None
        if actor is not None:
            post_count = session.scalar(
                select(func.count())
                .select_from(Record)
                .where(
                    Record.did == actor.did,
                    Record.deleted_at.is_(None),
                    Record.ap_object_json.isnot(None),
                )
            )
            profile = {
                "name": actor.display_name or actor.handle,
                "handle": actor.handle,
                "did": actor.did,
                "avatar": actor.avatar,
                "post_count": post_count or 0,
                "no_unauthenticated": bool(actor.no_unauthenticated),
            }
    if doc is None or profile is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if _wants_ap(request):
        return ap_response(doc)
    # An author hiding from signed-out readers gets the identity-only page:
    # don't list their posts on it (see _hides_from_anonymous).
    posts = (
        []
        if profile["no_unauthenticated"]
        else _record_rows(_recent_posts(Record.did == profile["did"]))
    )
    return _TEMPLATES.TemplateResponse(
        request,
        "profile.html",
        {**profile, "posts": posts, "actor_id": doc["id"], "settings": get_settings()},
    )


@app.post("/users/{ident}/inbox")
async def user_inbox(ident: str, request: Request) -> Response:
    activity = await request.json()
    # A POST can't be redirected safely, so a delivery addressed to a retired
    # handle is accepted here and attributed to the canonical actor.
    with session_scope() as session:
        actor = _actor_for_ident(session, ident)
        target = actor.handle if actor is not None else ident
    status = await handle_inbox(
        activity,
        target_actor_id=get_settings().actor_id(target),
        worker=getattr(app.state, "worker", None),
    )
    return Response(status_code=status)


@app.get("/users/{ident}/followers")
async def user_followers(ident: str) -> Response:
    settings = get_settings()
    with session_scope() as session:
        actor = _actor_for_ident(session, ident)
        actor_id = settings.actor_id(actor.handle if actor is not None else ident)
    return ap_response(
        {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": f"{actor_id}/followers",
            "type": "OrderedCollection",
            "totalItems": 0,
            "orderedItems": [],
        }
    )


@app.get("/users/{ident}/outbox")
async def user_outbox(ident: str) -> Response:
    settings = get_settings()
    with session_scope() as session:
        actor = _actor_for_ident(session, ident)
        actor_id = settings.actor_id(actor.handle if actor is not None else ident)
        items: list[str] = []
        if actor is not None:
            rows = session.execute(
                select(Record.rkey, Record.ap_object_json)
                .where(
                    Record.did == actor.did,
                    Record.deleted_at.is_(None),
                    # Exclude archive-only records that were never published.
                    Record.ap_object_json.isnot(None),
                )
                .order_by(Record.created_at.desc())
            ).all()
            # Prefer each Note's own id: posts minted before a rename keep the
            # handle they were published under.
            items = [
                _stored_object_id(ap_object_json) or settings.post_id(actor.handle, rkey)
                for rkey, ap_object_json in rows
            ]
    return ap_response(
        {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": f"{actor_id}/outbox",
            "type": "OrderedCollection",
            "totalItems": len(items),
            "orderedItems": items,
        }
    )


# Records stored before render_facets validated link schemes may carry unsafe
# hrefs (e.g. javascript:). The translator double-quotes attributes and
# html.escape()s all user text, so attribute matching by regex is reliable on
# this generated HTML.
_UNSAFE_HREF = re.compile(r'\bhref="(?!https?://)[^"]*"')


def _plain_text(fragment: str) -> str:
    """Collapse a generated HTML fragment to a single line of plain text."""
    return " ".join(re.sub(r"<[^>]+>", " ", fragment).split())


def _display_time(iso: str) -> str:
    """An ISO-8601 timestamp as ``YYYY-MM-DD HH:MM UTC``.

    Falls back to the raw leading date: ``published`` comes from the author's
    own record, so it is not guaranteed to parse.
    """
    try:
        stamp = datetime.fromisoformat(iso)
    except ValueError:
        return iso[:10]
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _facets(obj: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The Note's NeoDB ``relatedWith`` facets, keyed by type (first wins)."""
    out: dict[str, dict[str, Any]] = {}
    for facet in obj.get("relatedWith") or []:
        if isinstance(facet, dict) and isinstance(facet.get("type"), str):
            out.setdefault(facet["type"], facet)
    return out


def _rating_value(facet: dict[str, Any] | None) -> float | None:
    """The numeric score of a ``Rating`` facet, if it holds one.

    Stored Notes are re-read years after they were written, so the shape is
    checked rather than trusted (a bool is an int in Python, and is not a score).
    """
    value = facet.get("value") if facet else None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return value
    return None


def _review_doc(note: dict[str, Any], handle: str) -> dict[str, Any] | None:
    """A schema.org ``Review`` for a bridged Note, or ``None`` when the Note
    states neither a rating nor review text.

    The same facts the ``Note`` carries in NeoDB's vocabulary, restated in the
    one search engines and unfurlers read. ``itemReviewed`` is left to the
    caller: a post page names the work, an item page nests the review inside it.
    """
    facets = _facets(note)
    rating, comment = facets.get("Rating"), facets.get("Comment")
    if rating is None and comment is None:
        return None
    published = note.get("published") or ""
    doc: dict[str, Any] = {
        "@type": "Review",
        "url": note.get("id"),
        "datePublished": published,
        "dateModified": note.get("updated") or published,
        "author": {"@type": "Person", "name": handle, "url": get_settings().actor_id(handle)},
    }
    value = _rating_value(rating)
    if rating is not None and value is not None:
        doc["reviewRating"] = {
            "@type": "Rating",
            "ratingValue": value,
            # Every facet the translator writes carries both bounds; the
            # defaults only cover a Note stored malformed.
            "bestRating": rating.get("best", 10),
            "worstRating": rating.get("worst", 1),
        }
    # Spoiler-marked text stays behind the page's disclosure control, so it is
    # not restated in the machine-readable copy.
    if comment is not None and not note.get("sensitive"):
        body = _plain_text(str(comment.get("content") or ""))
        if body:
            doc["reviewBody"] = body
    return doc


def _review_schema(obj: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any] | None:
    """The post page's standalone ``Review``: :func:`_review_doc`, plus the work
    it is about and its own JSON-LD context."""
    doc = _review_doc(obj, ctx["handle"])
    if doc is None:
        return None
    work = ctx["work"]
    if work is not None and work.get("name"):
        item: dict[str, Any] = {
            "@type": works.schema_type_for_ap_type(work.get("type")),
            "name": work["name"],
        }
        if work.get("href"):
            item["url"] = work["href"]
        if work.get("image"):
            item["image"] = work["image"]
        doc["itemReviewed"] = item
    return {"@context": "https://schema.org", **doc}


def _aggregate_rating(work_key: str) -> dict[str, Any] | None:
    """The mean of every rating bridged for a work, or ``None`` if it has none.

    Deliberately unfiltered and uncapped, unlike the listing beside it: an
    average is a statistic over all the ratings, and it attributes no score to
    anyone, so an author hiding their posts from signed-out readers is still
    counted here. Only a retracted record drops out — an opt-out tombstones the
    data rather than hiding it.
    """
    with session_scope() as session:
        notes = session.scalars(
            select(Record.ap_object_json).where(
                Record.work_key == work_key,
                Record.deleted_at.is_(None),
                Record.ap_object_json.isnot(None),
            )
        )
        facets = [_facets(json.loads(note)).get("Rating") for note in notes if note]
    scored = [(facet, _rating_value(facet)) for facet in facets if facet is not None]
    scores = [value for _facet, value in scored if value is not None]
    if not scores:
        return None
    mean = round(sum(scores) / len(scores), 1)
    first = scored[0][0]
    return {
        "@type": "AggregateRating",
        # A whole number stays whole: "8" reads better than "8.0", and nothing
        # downstream distinguishes them.
        "ratingValue": int(mean) if mean == int(mean) else mean,
        "ratingCount": len(scores),
        "bestRating": first.get("best", 10),
        "worstRating": first.get("worst", 1),
    }


def _work_schema(
    doc: dict[str, Any],
    records: list[Record],
    handles: dict[str, str],
    aggregate: dict[str, Any] | None,
) -> dict[str, Any]:
    """schema.org description of a catalog item and the marks bridged for it.

    The AP catalog object at the same URL says this in NeoDB's vocabulary, which
    only NeoDB peers read. The nested reviews are the posts the page itself
    lists; ``aggregate`` counts every rating (see :func:`_aggregate_rating`) and
    is shown on the page in its own right.
    """
    item: dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": works.schema_type_for_ap_type(doc.get("type")),
        "@id": doc["id"],
        "url": doc["id"],
        "name": doc["name"],
    }
    if doc.get("cover_image_url"):
        item["image"] = doc["cover_image_url"]
    # The identifier URLs that let a NeoDB peer merge this work also tell a
    # search engine which known thing it is.
    same_as = [e["url"] for e in doc.get("external_resources") or [] if e.get("url")]
    if same_as:
        item["sameAs"] = same_as
    if doc.get("isbn"):
        item["isbn"] = doc["isbn"]
    if aggregate is not None:
        item["aggregateRating"] = aggregate

    reviews = []
    for record in records:
        note = json.loads(record.ap_object_json or "{}")
        review = _review_doc(note, handles.get(record.did, record.did))
        if review is not None:
            reviews.append(review)
    if reviews:
        item["review"] = reviews
    return item


def _post_page_ctx(obj: dict[str, Any], ident: str, at_uri: str) -> dict[str, Any]:
    """Template context for the human-readable view of a Note."""
    handle = _handle_of(ident) if ident.startswith("did:") else ident
    work: dict[str, Any] | None = None
    hashtags: list[str] = []
    for tag in obj.get("tag") or []:
        if tag.get("type") == "Hashtag":
            hashtags.append(tag.get("name", ""))
        elif tag.get("href") and work is None:
            work = {
                "name": tag.get("name"),
                "href": tag["href"],
                "image": tag.get("image"),
                "type": tag.get("type"),
            }
    content = _UNSAFE_HREF.sub("", obj.get("content", ""))
    if obj.get("sensitive"):
        description = obj.get("summary") or "Sensitive content"
    else:
        description = _plain_text(content)[:200]
    published = obj.get("published") or ""
    updated = obj.get("updated")
    ctx = {
        "og_title": obj.get("name") or f"Post by @{handle}",
        "title": obj.get("name"),
        "handle": handle,
        "author_url": get_settings().actor_id(handle),
        "content": content,
        "sensitive": bool(obj.get("sensitive")),
        "summary": obj.get("summary"),
        "published": published,
        "updated": updated,
        "published_display": _display_time(published) if published else "",
        "hashtags": hashtags,
        "work": work,
        "url": obj["id"],
        "at_uri": at_uri,
        "description": description,
        "settings": get_settings(),
    }
    ctx["schema"] = _review_schema(obj, ctx)
    return ctx


def _hides_from_anonymous(ident: str) -> bool:
    """Does this author carry Bluesky's ``!no-unauthenticated`` label?

    These pages have no sign-in, so "hidden from logged-out readers" can only
    mean hidden from everyone here. It gates the HTML views alone — the AP
    representation is unchanged, because a peer that already follows the
    author is exactly the audience the label still allows.
    """
    actor = identity.actor_by_ident(ident)
    return actor is not None and bool(actor.no_unauthenticated)


@app.get("/users/{ident}/posts/{rkey}")
async def get_post(ident: str, rkey: str, request: Request) -> Response:
    view = objects.get_post_view(ident, rkey)
    wants_ap = _wants_ap(request)
    if view is None:
        if wants_ap:
            return JSONResponse({"error": "not found"}, status_code=404)
        return HTMLResponse("<h1>404</h1><p>No such post.</p>", status_code=404)
    obj = view.document
    if wants_ap:
        status = 410 if obj.get("type") == "Tombstone" else 200
        return ap_response(obj, status=status)
    if obj.get("type") == "Tombstone":
        return HTMLResponse("<h1>410</h1><p>This post was deleted.</p>", status_code=410)
    ctx = _post_page_ctx(obj, ident, view.at_uri)
    ctx["no_unauthenticated"] = _hides_from_anonymous(ident)
    return _TEMPLATES.TemplateResponse(request, "post.html", ctx)


@app.get("/objects/{ident}/{rkey}")
async def get_object(ident: str, rkey: str, request: Request) -> Response:
    return await get_post(ident, rkey, request)


@app.get("/catalog/{work_type}/{work_id}")
async def get_catalog(work_type: str, work_id: str, request: Request) -> Response:
    doc = objects.get_work_object(work_type, work_id)
    if doc is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if _wants_ap(request):
        return ap_response(doc)
    identifiers = [(k, doc[key]) for k, key in (("IMDb", "imdb"), ("ISBN", "isbn")) if doc.get(key)]
    work_key = f"{work_type}:{work_id}"
    # This page is public, so authors hiding from signed-out readers stay out
    # of the listing. The rating average counts them all the same: see
    # _aggregate_rating.
    records = _recent_posts(
        Record.work_key == work_key,
        Record.did.not_in(_anonymous_hidden_dids()),
    )
    aggregate = _aggregate_rating(work_key)
    return _TEMPLATES.TemplateResponse(
        request,
        "work.html",
        {
            "title": doc["name"],
            "category": doc["category"],
            "poster": doc.get("cover_image_url"),
            "url": doc["id"],
            "links": [e["url"] for e in doc.get("external_resources", [])],
            "identifiers": identifiers,
            "peers": neodb_servers.peer_links(doc["id"]),
            "posts": _record_rows(records),
            # The template reads the JSON-LD object itself, so what the page
            # shows and what it marks up cannot drift apart.
            "rating": aggregate,
            "schema": _work_schema(doc, records, _handles_for(records), aggregate),
            "settings": get_settings(),
        },
    )


@app.get("/~neodb~/{path:path}")
async def neodb_marker_redirect(path: str, request: Request) -> Response:
    # Outgoing Note content links carry the /~neodb~/ marker so NeoDB peers
    # rewrite them to their own instance (see translate/neodb.py); every
    # other reader lands here and gets the plain local page. lstrip guards
    # against a protocol-relative ("//host/...") open redirect; backslashes
    # count too, since browsers normalize "\" to "/" when resolving Location.
    target = "/" + path.lstrip("/\\")
    if request.url.query:
        target += "?" + request.url.query
    return RedirectResponse(target, status_code=302)


# --------------------------------------------------------------------------- #
# Opt-out (AT Protocol user self-service)
# --------------------------------------------------------------------------- #
def _status_ctx(did: str, fallback_handle: str | None = None) -> dict[str, Any]:
    """Template context describing a DID's bridging status."""
    st = optout.lookup_status(did)
    return {
        "did": st.did,
        "handle": st.handle or fallback_handle or st.did,
        "bridged": st.bridged,
        "opted_out": st.opted_out,
        "record_count": st.record_count,
        "recent": _record_rows(st.recent_rows),
        "hide_from_recommendations": st.hide_from_recommendations,
        "no_unauthenticated": st.no_unauthenticated,
    }


def _admin_ctx(estimate: archive_replay.PlanEstimate | None = None) -> dict[str, Any]:
    """Operational state for the admin panel (DB-only; no network calls)."""
    settings = get_settings()
    with session_scope() as db:
        cursor = db.get(Cursor, 1)
        cursor_seq = cursor.seq if cursor is not None else None
    return {
        "cursor_seq": cursor_seq,
        "is_v2": settings.jetstream_is_v2,
        "has_api_key": bool(settings.jetstream_api_key),
        "can_import": settings.jetstream_is_v2 and bool(settings.jetstream_api_key),
        "running": archive_replay.is_running(),
        "job": archive_replay.current_job(),
        "collections": discover.report(),
        "wanted": set(settings.wanted_collections),
        "estimate": estimate,
    }


def _optout_page(
    request: Request,
    *,
    session: sessions.Session | None = None,
    message: str | None = None,
    q: str = "",
    status_code: int = 200,
    estimate: archive_replay.PlanEstimate | None = None,
) -> Response:
    """Render the self-service page: sign-in form, or the account view."""
    is_admin = session is not None and admin.is_admin(session.did)
    return _TEMPLATES.TemplateResponse(
        request,
        "manage.html",
        {
            "message": message,
            "q": q,
            "signed_in": session is not None,
            "status": _status_ctx(session.did, session.handle) if session else None,
            "csrf": session.csrf if session else None,
            "settings": get_settings(),
            "is_admin": is_admin,
            "admin": _admin_ctx(estimate) if is_admin else None,
        },
        status_code=status_code,
    )


def _current_session(request: Request) -> sessions.Session | None:
    return sessions.get(request.cookies.get(sessions.COOKIE_NAME))


@app.get("/manage", response_class=HTMLResponse)
async def optout_form(request: Request) -> Response:
    # Status is only shown to the signed-in account holder: an open lookup
    # would let anyone enumerate what we hold about a user.
    return _optout_page(request, session=_current_session(request))


def _optout_error(
    request: Request, wants_html: bool, msg: str, code: str, status: int, q: str = ""
) -> Response:
    if wants_html:
        return _optout_page(request, message=msg, q=q, status_code=status)
    return JSONResponse({"ok": False, "error": code}, status_code=status)


@app.post("/manage")
async def optout_submit(request: Request, identifier: str = Form(...)) -> Response:
    """Start the atproto OAuth sign-in that gates the self-service actions.

    The user proves control of the account by logging in on their OWN
    authorization server (no passwords ever touch this relay); the callback
    opens a short-lived session and the account view offers the actions.
    """
    wants_html = "text/html" in request.headers.get("accept", "")
    flow = await asyncio.to_thread(oauth.start_flow, identifier)
    if flow is None:
        return _optout_error(
            request,
            wants_html,
            "Could not start sign-in for that account — check the handle or DID.",
            "oauth_start_failed",
            400,
            q=identifier,
        )
    if wants_html:
        return RedirectResponse(flow.authorize_url, status_code=303)
    return JSONResponse({"ok": True, "authorize_url": flow.authorize_url, "state": flow.state})


def _action_session(request: Request, csrf: str) -> sessions.Session | None:
    """The caller's session, iff the form echoed its CSRF token."""
    session = _current_session(request)
    if session is None or not csrf or not secrets.compare_digest(csrf, session.csrf):
        return None
    return session


_SESSION_EXPIRED = "Your sign-in has expired — please sign in again."


@app.post("/manage/opt-out", response_class=HTMLResponse)
async def optout_action_opt_out(request: Request, csrf: str = Form("")) -> Response:
    session = _action_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _SESSION_EXPIRED, "session_expired", 401)
    purged = await optout.opt_out(session.did, worker=getattr(app.state, "worker", None))
    msg = f"{session.handle} opted out; {purged} bridged record(s) deleted."
    return _optout_page(request, session=session, message=msg)


@app.post("/manage/opt-in", response_class=HTMLResponse)
async def optout_action_opt_in(request: Request, csrf: str = Form("")) -> Response:
    session = _action_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _SESSION_EXPIRED, "session_expired", 401)
    was_out = optout.opt_in(session.did)
    msg = (
        f"{session.handle} is opted back in; future activity will bridge again."
        if was_out
        else f"{session.handle} was not opted out."
    )
    return _optout_page(request, session=session, message=msg)


@app.post("/manage/import", response_class=HTMLResponse)
async def optout_action_import(request: Request, csrf: str = Form("")) -> Response:
    """Kick off a background import of the account's recent activity."""
    session = _action_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _SESSION_EXPIRED, "session_expired", 401)
    if optout.is_opted_out(session.did):
        # Server-side guard behind the disabled button: never re-publish an
        # opted-out account's records.
        msg = f"{session.handle} is opted out; import is disabled."
    elif backfill.start_import(session.did, worker=getattr(app.state, "worker", None)):
        msg = f"Importing recent activity for {session.handle} in the background."
    else:
        msg = f"An import for {session.handle} is already in progress."
    return _optout_page(request, session=session, message=msg)


def _admin_session(request: Request, csrf: str) -> sessions.Session | None:
    """The caller's session, iff it passed CSRF *and* is an operator.

    Authorisation is on the OAuth-verified DID; see :mod:`skybridge.admin` for
    why the session's handle must not be used for this.
    """
    session = _action_session(request, csrf)
    if session is None or not admin.is_admin(session.did):
        return None
    return session


_NOT_ADMIN = "That account is not an operator of this relay."


@app.post("/manage/admin/import/dry-run", response_class=HTMLResponse)
async def admin_import_dry_run(
    request: Request, csrf: str = Form(""), after_seq: int = Form(0)
) -> Response:
    """Plan the import without downloading, so its byte cost is known first."""
    session = _admin_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _NOT_ADMIN, "forbidden", 403)
    try:
        estimate = await archive_replay.estimate(after_seq=after_seq)
    except archive_replay.ArchiveError as exc:
        return _optout_page(request, session=session, message=str(exc))
    msg = (
        f"Planned {estimate.segments} segment(s) covering seq "
        f"{estimate.after_seq} to {estimate.before_seq}."
    )
    return _optout_page(request, session=session, message=msg, estimate=estimate)


@app.post("/manage/admin/import", response_class=HTMLResponse)
async def admin_import_start(
    request: Request, csrf: str = Form(""), after_seq: int = Form(0)
) -> Response:
    """Start the historical import in the background, on this process."""
    session = _admin_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _NOT_ADMIN, "forbidden", 403)
    settings = get_settings()
    if not settings.jetstream_is_v2 or not settings.jetstream_api_key:
        msg = "Archive import needs a Jetstream v2 endpoint and SKYBRIDGE_JETSTREAM_API_KEY."
    elif archive_replay.is_running():
        msg = "An archive import is already running."
    else:
        job_id = archive_replay.create_job(after_seq=after_seq)
        archive_replay.start(job_id, worker=getattr(app.state, "worker", None))
        msg = f"Archive import #{job_id} started in the background."
    return _optout_page(request, session=session, message=msg)


@app.post("/manage/admin/import/cancel", response_class=HTMLResponse)
async def admin_import_cancel(request: Request, csrf: str = Form("")) -> Response:
    session = _admin_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _NOT_ADMIN, "forbidden", 403)
    cancelled = await archive_replay.cancel()
    msg = "Archive import cancelled." if cancelled else "No archive import is running."
    return _optout_page(request, session=session, message=msg)


def _session_cookie_attrs() -> dict[str, Any]:
    # delete_cookie must repeat the attributes set_cookie used, or browsers
    # may treat it as a different cookie and keep the original.
    return {
        "path": "/",
        "httponly": True,
        "samesite": "lax",
        "secure": get_settings().scheme == "https",
    }


@app.post("/manage/signout")
async def optout_signout(request: Request) -> Response:
    sessions.drop(request.cookies.get(sessions.COOKIE_NAME))
    resp = RedirectResponse("/manage", status_code=303)
    resp.delete_cookie(sessions.COOKIE_NAME, **_session_cookie_attrs())
    return resp


@app.get("/oauth/client-metadata.json")
async def oauth_client_metadata() -> Response:
    return JSONResponse(oauth.client_metadata())


@app.get("/oauth/callback", response_class=HTMLResponse)
async def oauth_callback(
    request: Request,
    state: str = "",
    code: str = "",
    iss: str = "",
    error: str = "",
    error_description: str = "",
) -> Response:
    """Finish the OAuth sign-in and open the self-service session."""
    if error:
        msg = error_description or f"Sign-in was not completed ({error})."
        return _optout_error(request, True, msg, error, 400)
    result = await asyncio.to_thread(oauth.finish_flow, state, code, iss or None)
    if result is None:
        return _optout_error(
            request,
            True,
            "Sign-in could not be verified — please try again.",
            "oauth_failed",
            400,
        )
    token = sessions.create(result.did, result.handle)
    # The account holder is here and has just proved control of the DID, so
    # re-read their handle, profile and visibility preferences before showing
    # the status page — otherwise it reports whatever the firehose last
    # happened to tell us. A no-op for a DID we do not bridge or that opted
    # out; peers hear about it only when something actually moved.
    refreshed = await asyncio.to_thread(identity.resync_actor, result.did)
    if refreshed is not None:
        await pipeline.deliver_person_update(
            refreshed, seq=None, worker=getattr(app.state, "worker", None)
        )
    resp = RedirectResponse("/manage", status_code=303)
    resp.set_cookie(
        sessions.COOKIE_NAME,
        token,
        max_age=int(sessions.SESSION_TTL),
        **_session_cookie_attrs(),
    )
    return resp


# --------------------------------------------------------------------------- #
# Stats + archive UI
# --------------------------------------------------------------------------- #
@app.get("/stats")
async def stats_json() -> Response:
    return JSONResponse(collect_stats())


def _work_name(obj: dict[str, Any]) -> str | None:
    """The catalog item a Note marks, from its work tag.

    Marks and reviews are untitled Notes (see translate.neodb), so the work is
    the only human-readable name a listing can show for them.
    """
    for tag in obj.get("tag") or []:
        if isinstance(tag, dict) and tag.get("href") and tag.get("name"):
            return str(tag["name"])
    return None


def _handles_for(rows: list[Record]) -> dict[str, str]:
    """did -> live handle, for the authors of ``rows``."""
    handles: dict[str, str] = {}
    with session_scope() as session:
        for did in {r.did for r in rows}:
            actor = session.get(BridgedActor, did)
            handles[did] = actor.handle if actor else did
    return handles


def _record_rows(rows: list[Record]) -> list[dict[str, Any]]:
    handles = _handles_for(rows)
    out = []
    for r in rows:
        title = None
        post_url = None
        rating = None
        if r.ap_object_json:
            obj = json.loads(r.ap_object_json)
            title = obj.get("name") or _work_name(obj)
            facet = _facets(obj).get("Rating")
            score = _rating_value(facet)
            if facet is not None and score is not None:
                rating = f"{score:g}/{facet.get('best', 10):g}"
            # Link to the id peers hold rather than one rebuilt from the
            # author's current handle: post ids outlive a rename. Archive-only
            # rows have no post page and a deleted one only tombstones, so
            # both keep the archived-record link instead.
            if r.deleted_at is None:
                post_url = obj.get("id")
        out.append(
            {
                "at_uri": r.at_uri,
                "did": r.did,
                "rkey": r.rkey,
                "collection": r.collection,
                "op": r.op,
                "updated_at": r.updated_at,
                "handle": handles.get(r.did, r.did),
                "title": title,
                "post_url": post_url,
                "rating": rating,
            }
        )
    return out


# How many posts the profile and catalog-item pages list.
RECENT_POSTS = 100


def _recent_posts(*where: Any) -> list[Record]:
    """The most recently bridged live posts matching ``where``, newest first.

    Restricted to records that were actually published to AP and are not
    tombstoned — the ones with a post page to link to. Ordered like
    :func:`user_outbox`, so a profile listing agrees with the AP collection of
    the same posts.
    """
    with session_scope() as session:
        return list(
            session.scalars(
                select(Record)
                .where(
                    Record.deleted_at.is_(None),
                    Record.ap_object_json.isnot(None),
                    *where,
                )
                .order_by(Record.created_at.desc())
                .limit(RECENT_POSTS)
            )
        )


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> Response:
    stats = collect_stats()
    ingest_task = getattr(app.state, "ingest_task", None)
    ingesting = ingest_task is not None and not ingest_task.done()
    return _TEMPLATES.TemplateResponse(
        request,
        "dashboard.html",
        {
            "stats": stats,
            "jetstream_url": get_settings().jetstream_url if ingesting else None,
            "settings": get_settings(),
        },
    )


def _anonymous_hidden_dids() -> Any:
    """Subquery of the DIDs whose records these pages must not show.

    The archive views render the raw source record, so an author carrying
    ``!no-unauthenticated`` has to be filtered out of them as well as out of
    the profile and post pages. The author's own view of their archive is on
    the opt-out page, which is behind an atproto sign-in.
    """
    return select(BridgedActor.did).where(BridgedActor.no_unauthenticated.is_(True))


@app.get("/archive", response_class=HTMLResponse)
async def archive(request: Request, q: str = "") -> Response:
    with session_scope() as session:
        stmt = (
            select(Record)
            .where(Record.did.not_in(_anonymous_hidden_dids()))
            .order_by(Record.updated_at.desc())
        )
        if q:
            like = f"%{q}%"
            stmt = stmt.where(
                or_(
                    Record.collection.like(like),
                    Record.rkey.like(like),
                    Record.source_json.like(like),
                )
            )
        rows = list(session.scalars(stmt.limit(100)))
    return _TEMPLATES.TemplateResponse(
        request,
        "archive.html",
        {"rows": _record_rows(rows), "total": len(rows), "q": q, "settings": get_settings()},
    )


def _archive_uri(path: str) -> str:
    """Rebuild the ``at://`` URI addressed by an ``/archive/...`` path.

    Links are minted as ``/archive/{did}/{collection}/{rkey}`` because a proxy
    that normalises request paths (Cloudflare does) decodes the ``%3A`` of an
    encoded ``at://`` URI and then collapses the ``//``, so the handler used to
    look up an ``at:/…`` key that no row has. Links minted before that — and
    the collapsed form they now arrive as — still resolve here.
    """
    path = path.lstrip("/")
    if path.startswith("at:"):
        path = path[3:].lstrip("/")
    return f"at://{path}"


@app.get("/archive/{at_uri:path}", response_class=HTMLResponse)
async def archive_detail(request: Request, at_uri: str) -> Response:
    at_uri = _archive_uri(at_uri)
    with session_scope() as session:
        record = session.get(Record, at_uri)
        if record is None:
            return HTMLResponse("<h1>404</h1><p>No such record.</p>", status_code=404)
        author = session.get(BridgedActor, record.did)
        if author is not None and author.no_unauthenticated:
            # Same answer as a record we do not hold: this page shows the raw
            # source record, and there is no signed-in reader to show it to.
            return HTMLResponse("<h1>404</h1><p>No such record.</p>", status_code=404)
        handle = _handle_of(record.did)
        work = None
        if record.work_key:
            w = session.get(Work, record.work_key)
            if w is not None:
                wt, _, wid = w.work_key.partition(":")
                work = {
                    "work_key": w.work_key,
                    "work_type": wt,
                    "work_id": wid,
                    "title": w.title,
                }
        ctx = {
            "record": {
                "at_uri": record.at_uri,
                "rkey": record.rkey,
                "collection": record.collection,
                "op": record.op,
                "handle": handle,
                "updated_at": record.updated_at,
                "deleted_at": record.deleted_at,
            },
            "work": work,
            "source_pretty": json.dumps(json.loads(record.source_json), indent=2),
            "ap_pretty": json.dumps(
                json.loads(record.ap_activity_json) if record.ap_activity_json else {},
                indent=2,
            ),
            "settings": get_settings(),
        }
    return _TEMPLATES.TemplateResponse(request, "record.html", ctx)


@app.get("/catalog", response_class=HTMLResponse)
async def catalog(request: Request) -> Response:
    with session_scope() as session:
        rows = list(session.scalars(select(Work).order_by(Work.first_seen.desc()).limit(200)))
        works = []
        for w in rows:
            wt, _, wid = w.work_key.partition(":")
            works.append(
                {
                    "work_key": w.work_key,
                    "work_type": wt,
                    "work_id": wid,
                    "title": w.title,
                    "creative_work_type": w.creative_work_type,
                    "identifiers_json": w.identifiers_json,
                }
            )
    return _TEMPLATES.TemplateResponse(
        request, "catalog.html", {"works": works, "settings": get_settings()}
    )
