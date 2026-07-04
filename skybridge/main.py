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
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_, select

from skybridge import optout
from skybridge.activitypub import nodeinfo, objects, webfinger
from skybridge.activitypub.actors import RELAY_DID, person_actor, relay_actor
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.activitypub.inbox import handle_inbox
from skybridge.atproto import auth
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
    init_db()
    worker = DeliveryWorker()
    worker.start()
    app.state.worker = worker

    ingest_task: asyncio.Task | None = None
    if os.environ.get("SKYBRIDGE_INGEST") == "1":
        from skybridge.atproto.jetstream import run as jetstream_run

        ingest_task = asyncio.create_task(jetstream_run(worker), name="ingest")
    try:
        yield
    finally:
        if ingest_task is not None:
            ingest_task.cancel()
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
    status = await handle_inbox(activity, target_actor_id=get_settings().relay_actor_id)
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
    if doc is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if _wants_ap(request):
        return ap_response(doc)
    return HTMLResponse(
        f"<h1>{doc['name']}</h1><p>{doc['summary']}</p>"
        f"<p>ActivityPub actor: <code>{doc['id']}</code></p>"
    )


@app.post("/users/{ident}/inbox")
async def user_inbox(ident: str, request: Request) -> Response:
    activity = await request.json()
    status = await handle_inbox(activity, target_actor_id=get_settings().actor_id(ident))
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
    attrs = "".join(f"<li>{a['name']}: {a['value']}</li>" for a in doc.get("attachment", []))
    return HTMLResponse(f"<h1>{doc['name']}</h1><p>type: {doc['category']}</p><ul>{attrs}</ul>")


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
        "unresolved": False,
    }


# The lookup is unauthenticated, so bound the outbound resolution it can
# trigger: a small TTL cache absorbs repeats and a semaphore caps concurrency.
_RESOLVE_CACHE: dict[str, tuple[float, str | None]] = {}
_RESOLVE_CACHE_TTL = 300.0
_RESOLVE_CACHE_MAX = 1024
_RESOLVE_SEM = asyncio.Semaphore(4)


async def _resolve_did_bounded(ident: str) -> str | None:
    cached = _RESOLVE_CACHE.get(ident)
    if cached is not None and time.monotonic() - cached[0] < _RESOLVE_CACHE_TTL:
        return cached[1]
    async with _RESOLVE_SEM:
        did = await asyncio.to_thread(auth.resolve_did, ident)
    if len(_RESOLVE_CACHE) >= _RESOLVE_CACHE_MAX:
        _RESOLVE_CACHE.clear()
    _RESOLVE_CACHE[ident] = (time.monotonic(), did)
    return did


async def _lookup_status(query: str) -> dict[str, Any]:
    """Resolve a handle-or-DID query to a bridging-status template context."""
    ident = query.strip().lstrip("@")
    if ident.startswith("did:"):
        return _status_ctx(ident)
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == ident))
        did = actor.did if actor is not None else None
    if did is None:
        # Pre-emptive opt-outs are keyed by DID only, so an unknown handle
        # needs a network handle→DID resolve (best effort, off the event loop).
        did = await _resolve_did_bounded(ident)
    if did is None:
        return {
            "did": None,
            "handle": ident,
            "bridged": False,
            "opted_out": False,
            "record_count": 0,
            "recent": [],
            "unresolved": True,
        }
    return _status_ctx(did, fallback_handle=ident)


@app.get("/optout", response_class=HTMLResponse)
async def optout_form(request: Request, q: str = "") -> Response:
    status = await _lookup_status(q) if q.strip() else None
    return _TEMPLATES.TemplateResponse(
        request,
        "optout.html",
        {
            "message": None,
            "q": q.strip().lstrip("@"),
            "status": status,
            "settings": get_settings(),
        },
    )


@app.post("/optout")
async def optout_submit(
    request: Request,
    identifier: str = Form(...),
    app_password: str = Form(...),
    action: str = Form("opt-out"),
) -> Response:
    """Authenticate the atproto user, then opt them out (or back in)."""
    wants_html = "text/html" in request.headers.get("accept", "")
    if action not in ("opt-out", "opt-in"):
        # Never let a malformed action fall through to the destructive default.
        if wants_html:
            return _TEMPLATES.TemplateResponse(
                request,
                "optout.html",
                {
                    "message": "Unknown action.",
                    "q": identifier,
                    "status": None,
                    "settings": get_settings(),
                },
                status_code=400,
            )
        return JSONResponse({"ok": False, "error": "invalid_action"}, status_code=400)

    result = await asyncio.to_thread(auth.verify_credentials, identifier, app_password)
    if result is None:
        msg = "Authentication failed — check your handle and app password."
        if wants_html:
            return _TEMPLATES.TemplateResponse(
                request,
                "optout.html",
                {"message": msg, "q": identifier, "status": None, "settings": get_settings()},
                status_code=401,
            )
        return JSONResponse({"ok": False, "error": "authentication_failed"}, status_code=401)

    worker = getattr(app.state, "worker", None)
    if action == "opt-in":
        was_out = optout.opt_in(result.did)
        msg = (
            f"{result.handle} is opted back in; future activity will bridge again."
            if was_out
            else f"{result.handle} was not opted out."
        )
        payload = {"ok": True, "did": result.did, "opted_out": False, "rebridged": was_out}
    else:
        purged = await optout.opt_out(result.did, worker=worker)
        msg = f"{result.handle} opted out; {purged} bridged record(s) deleted."
        payload = {"ok": True, "did": result.did, "opted_out": True, "deleted": purged}

    if wants_html:
        return _TEMPLATES.TemplateResponse(
            request,
            "optout.html",
            {
                "message": msg,
                "q": result.handle,
                "status": _status_ctx(result.did, result.handle),
                "settings": get_settings(),
            },
        )
    return JSONResponse(payload)


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
    with session_scope() as session:
        recent = list(session.scalars(select(Record).order_by(Record.updated_at.desc()).limit(15)))
    return _TEMPLATES.TemplateResponse(
        request,
        "dashboard.html",
        {"stats": stats, "recent": _record_rows(recent), "settings": get_settings()},
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
        rows = list(session.scalars(stmt.limit(200)))
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
