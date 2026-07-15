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
import secrets
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select

from skybridge import optout, sessions, telemetry
from skybridge.activitypub import nodeinfo, objects, webfinger
from skybridge.activitypub.actors import RELAY_DID, get_relay_keys, person_actor, relay_actor
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.activitypub.inbox import handle_inbox
from skybridge.activitypub.relays import reconcile_relays
from skybridge.atproto import backfill, oauth
from skybridge.config import get_settings
from skybridge.db import init_db, session_scope
from skybridge.models import BridgedActor, Record, Work
from skybridge.stats import collect_stats

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

    ingest_task: asyncio.Task | None = None
    if os.environ.get("SKYBRIDGE_INGEST") == "1":
        from skybridge.atproto.jetstream import run as jetstream_run

        ingest_task = asyncio.create_task(jetstream_run(worker), name="ingest")
    app.state.ingest_task = ingest_task
    try:
        yield
    finally:
        if ingest_task is not None:
            ingest_task.cancel()
        if not relay_task.done():
            relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await relay_task
        # Imports must stop enqueueing before worker.stop() awaits the queue
        # drain, or a long replay stalls shutdown / feeds a dead queue.
        await backfill.cancel_all_imports()
        await worker.stop()


app = FastAPI(title="Skybridge", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _wants_ap(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "activity+json" in accept or "ld+json" in accept


def ap_response(doc: dict[str, Any], status: int = 200) -> Response:
    return JSONResponse(doc, status_code=status, media_type=AP_CONTENT_TYPE)


def _handle_of(did: str) -> str:
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        return row.handle if row else did


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
    # Bridged content belongs to its atproto authors; keep crawlers out.
    return PlainTextResponse("User-agent: *\nDisallow: /\n")


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
        actor = session.scalar(
            select(BridgedActor).where(BridgedActor.handle == ident, BridgedActor.did != RELAY_DID)
        )
        # An opted-out actor is Gone: don't serve its profile.
        if actor is not None and actor.opted_out:
            return JSONResponse({"error": "gone"}, status_code=410)
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
            }
    if doc is None or profile is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if _wants_ap(request):
        return ap_response(doc)
    return _TEMPLATES.TemplateResponse(
        request,
        "profile.html",
        {**profile, "actor_id": doc["id"], "settings": get_settings()},
    )


@app.post("/users/{ident}/inbox")
async def user_inbox(ident: str, request: Request) -> Response:
    activity = await request.json()
    status = await handle_inbox(
        activity,
        target_actor_id=get_settings().actor_id(ident),
        worker=getattr(app.state, "worker", None),
    )
    return Response(status_code=status)


@app.get("/users/{ident}/followers")
async def user_followers(ident: str) -> Response:
    settings = get_settings()
    actor_id = settings.actor_id(ident)
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
    actor_id = settings.actor_id(ident)
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == ident))
        items: list[str] = []
        if actor is not None:
            rows = session.execute(
                select(Record.rkey)
                .where(
                    Record.did == actor.did,
                    Record.deleted_at.is_(None),
                    # Exclude archive-only records that were never published.
                    Record.ap_object_json.isnot(None),
                )
                .order_by(Record.created_at.desc())
            ).all()
            items = [settings.post_id(ident, r[0]) for r in rows]
    return ap_response(
        {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": f"{actor_id}/outbox",
            "type": "OrderedCollection",
            "totalItems": len(items),
            "orderedItems": items,
        }
    )


@app.get("/users/{ident}/posts/{rkey}")
async def get_post(ident: str, rkey: str) -> Response:
    obj = objects.get_post_object(ident, rkey)
    if obj is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    status = 410 if obj.get("type") == "Tombstone" else 200
    return ap_response(obj, status=status)


@app.get("/objects/{ident}/{rkey}")
async def get_object(ident: str, rkey: str) -> Response:
    return await get_post(ident, rkey)


@app.get("/catalog/{work_type}/{work_id}")
async def get_catalog(work_type: str, work_id: str, request: Request) -> Response:
    doc = objects.get_work_object(work_type, work_id)
    if doc is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if _wants_ap(request):
        return ap_response(doc)
    identifiers = [(k, doc[key]) for k, key in (("IMDb", "imdb"), ("ISBN", "isbn")) if doc.get(key)]
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
    }


def _optout_page(
    request: Request,
    *,
    session: sessions.Session | None = None,
    message: str | None = None,
    q: str = "",
    status_code: int = 200,
) -> Response:
    """Render the self-service page: sign-in form, or the account view."""
    return _TEMPLATES.TemplateResponse(
        request,
        "optout.html",
        {
            "message": message,
            "q": q,
            "signed_in": session is not None,
            "status": _status_ctx(session.did, session.handle) if session else None,
            "csrf": session.csrf if session else None,
            "settings": get_settings(),
        },
        status_code=status_code,
    )


def _current_session(request: Request) -> sessions.Session | None:
    return sessions.get(request.cookies.get(sessions.COOKIE_NAME))


@app.get("/optout", response_class=HTMLResponse)
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


@app.post("/optout")
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


@app.post("/optout/opt-out", response_class=HTMLResponse)
async def optout_action_opt_out(request: Request, csrf: str = Form("")) -> Response:
    session = _action_session(request, csrf)
    if session is None:
        return _optout_error(request, True, _SESSION_EXPIRED, "session_expired", 401)
    purged = await optout.opt_out(session.did, worker=getattr(app.state, "worker", None))
    msg = f"{session.handle} opted out; {purged} bridged record(s) deleted."
    return _optout_page(request, session=session, message=msg)


@app.post("/optout/opt-in", response_class=HTMLResponse)
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


@app.post("/optout/import", response_class=HTMLResponse)
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


def _session_cookie_attrs() -> dict[str, Any]:
    # delete_cookie must repeat the attributes set_cookie used, or browsers
    # may treat it as a different cookie and keep the original.
    return {
        "path": "/",
        "httponly": True,
        "samesite": "lax",
        "secure": get_settings().scheme == "https",
    }


@app.post("/optout/signout")
async def optout_signout(request: Request) -> Response:
    sessions.drop(request.cookies.get(sessions.COOKIE_NAME))
    resp = RedirectResponse("/optout", status_code=303)
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
    resp = RedirectResponse("/optout", status_code=303)
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


def _record_rows(rows: list[Record]) -> list[dict[str, Any]]:
    handles = {}
    with session_scope() as session:
        for did in {r.did for r in rows}:
            row = session.get(BridgedActor, did)
            handles[did] = row.handle if row else did
    out = []
    for r in rows:
        title = None
        if r.ap_object_json:
            title = json.loads(r.ap_object_json).get("name")
        out.append(
            {
                "at_uri": r.at_uri,
                "rkey": r.rkey,
                "collection": r.collection,
                "op": r.op,
                "updated_at": r.updated_at,
                "handle": handles.get(r.did, r.did),
                "title": title,
            }
        )
    return out


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


@app.get("/archive", response_class=HTMLResponse)
async def archive(request: Request, q: str = "") -> Response:
    with session_scope() as session:
        stmt = select(Record).order_by(Record.updated_at.desc())
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


@app.get("/archive/{at_uri:path}", response_class=HTMLResponse)
async def archive_detail(request: Request, at_uri: str) -> Response:
    at_uri = unquote(at_uri)
    with session_scope() as session:
        record = session.get(Record, at_uri)
        if record is None:
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
