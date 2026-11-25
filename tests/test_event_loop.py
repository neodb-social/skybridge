"""The event loop stays free: no request waits on another request's query.

One loop runs every request, the Jetstream consumer and delivery. A route that
queries SQLite on it holds all of them up for as long as the query runs, and a
whole-table count on a multi-gigabyte archive runs for most of a minute. So
the stats are served from a cache, and a route that reads the database runs on
the thread pool.
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
from dataclasses import replace
from typing import Any

import httpx
import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from skybridge import admin, main, sessions, stats
from skybridge.atproto import archive as archive_replay
from skybridge.atproto import backfill, identity, oauth
from skybridge.atproto.replay import replay_file
from skybridge.config import Settings, set_settings
from skybridge.db import get_engine, session_scope
from skybridge.main import app
from skybridge.models import Record, Work
from sqlalchemy import event, select

DID = "did:plc:i6k6scfcdaup4e2va33nkprb"  # the fixture author

# A request that should answer at once, even while another one waits on a
# second-long query. Generous, so a slow CI runner does not flake it.
PROMPT = 0.5
SLOW = 1.5


def _asgi_client() -> httpx.AsyncClient:
    """A client that runs the app on the test's own event loop, so a blocked
    loop shows up as a slow answer (TestClient hides it on a private loop)."""
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://bridge.test")


async def _timed_get(client: httpx.AsyncClient, path: str) -> tuple[int, float]:
    started = time.monotonic()
    response = await client.get(path)
    return response.status_code, time.monotonic() - started


# --------------------------------------------------------------------------- #
# The stats cache
# --------------------------------------------------------------------------- #
def test_stats_are_served_before_the_first_refresh(monkeypatch):
    """A cold cache answers at once with every key, the counts null, and runs
    no query to do it."""

    keys = {*stats.collect_stats(), "stats_updated_at"}

    def no_counting() -> dict[str, Any]:
        raise AssertionError("a request must never count")

    monkeypatch.setattr(stats, "count_stats", no_counting)
    client = TestClient(app)

    r = client.get("/stats")
    assert r.status_code == 200
    doc = r.json()
    assert set(doc) == keys
    for key in ("bridged_actors", "records_total", "records_active", "works", "likes"):
        assert doc[key] is None, key
    assert doc["records_by_collection"] == {}
    assert doc["deliveries_by_status"] == {}
    assert doc["stats_updated_at"] is None
    assert doc["relays_configured"] == 0  # from settings, not counted

    page = client.get("/")
    assert page.status_code == 200
    assert "None" not in page.text
    assert "..." in page.text


def test_stats_keys_match_the_full_count(fixture_path):
    """/stats keeps the shape monitors already read: the keys of a full count,
    plus when the counts were taken."""
    asyncio.run(replay_file(fixture_path, allow_network=False))
    assert set(stats.cached_stats()) == {*stats.collect_stats(), "stats_updated_at"}


def test_refresh_publishes_the_counts(fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    full = stats.collect_stats()

    asyncio.run(stats.refresh_stats())
    cached = stats.cached_stats()
    assert cached["stats_updated_at"] is not None
    assert cached["records_total"] == full["records_total"] > 0
    assert cached["records_by_collection"] == full["records_by_collection"]
    # The active count needs the full-table pass, which is the usage loop's.
    assert cached["records_active"] is None

    asyncio.run(stats.refresh_usage())
    assert stats.cached_stats()["records_active"] == full["records_active"]
    assert {k: v for k, v in stats.cached_stats().items() if k != "stats_updated_at"} == full


def test_cached_counts_age_until_the_next_refresh(fixture_path):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    asyncio.run(stats.refresh_stats())
    before = stats.cached_stats()["records_total"]
    with session_scope() as session:
        session.add(Record(at_uri="at://did:plc:x/c/1", did="did:plc:x", collection="c", rkey="1"))
    assert stats.cached_stats()["records_total"] == before
    asyncio.run(stats.refresh_stats())
    assert stats.cached_stats()["records_total"] == before + 1


def test_a_failed_refresh_keeps_the_previous_counts(fixture_path, monkeypatch):
    asyncio.run(replay_file(fixture_path, allow_network=False))
    asyncio.run(stats.refresh_stats())
    good = stats.cached_stats()

    def broken() -> dict[str, Any]:
        raise RuntimeError("database is locked")

    monkeypatch.setattr(stats, "count_stats", broken)

    async def one_pass() -> None:
        task = asyncio.create_task(stats.stats_refresh_loop(interval=3600))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(one_pass())
    assert stats.cached_stats() == good


# --------------------------------------------------------------------------- #
# Nothing waits behind a slow query
# --------------------------------------------------------------------------- #
def test_a_slow_count_does_not_delay_other_requests(monkeypatch):
    """The production failure: a stats pass that reads the whole archive must
    not hold up /robots.txt, nor /stats and / themselves."""
    started = threading.Event()

    def slow_count() -> dict[str, Any]:
        started.set()
        time.sleep(SLOW)
        return {**stats._cold_stats(), "records_total": 7}

    monkeypatch.setattr(stats, "count_stats", slow_count)

    async def scenario() -> None:
        refresh = asyncio.create_task(stats.refresh_stats())
        # Wait until the count is underway in its thread.
        while not started.is_set():
            await asyncio.sleep(0.01)
        async with _asgi_client() as client:
            for path in ("/robots.txt", "/stats", "/", "/.well-known/nodeinfo"):
                status, elapsed = await _timed_get(client, path)
                assert status == 200, path
                assert elapsed < PROMPT, (path, elapsed)
        assert not refresh.done()  # the answers above did not wait for it
        await refresh

    asyncio.run(scenario())
    assert stats.cached_stats()["records_total"] == 7


def test_a_slow_page_query_does_not_delay_other_requests(monkeypatch):
    """A profile page whose query is slow runs on the thread pool; the loop
    keeps answering in the meantime."""
    started = threading.Event()

    def slow_lookup(session, ident):
        started.set()
        time.sleep(SLOW)
        return None

    monkeypatch.setattr(main, "_actor_for_ident", slow_lookup)

    async def scenario() -> None:
        async with _asgi_client() as client:
            profile = asyncio.create_task(client.get("/users/somebody.test"))
            while not started.is_set():
                await asyncio.sleep(0.01)
            status, elapsed = await _timed_get(client, "/robots.txt")
            assert status == 200
            assert elapsed < PROMPT, elapsed
            assert not profile.done()
            assert (await profile).status_code == 404

    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# No route queries on the event loop
# --------------------------------------------------------------------------- #
def _on_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


class _LoopQueries:
    """Records each SQL statement executed on an event-loop thread."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def __call__(self, conn, cursor, statement, parameters, context, executemany) -> None:
        if _on_event_loop():
            self.statements.append(statement)

    def __enter__(self) -> _LoopQueries:
        event.listen(get_engine(), "before_cursor_execute", self)
        return self

    def __exit__(self, *exc: object) -> None:
        event.remove(get_engine(), "before_cursor_execute", self)


def test_the_detector_sees_a_query_on_the_loop():
    """The check below is only as good as this: a query run straight from a
    coroutine is caught, and one run from a worker thread is not."""

    def query() -> None:
        with session_scope() as session:
            session.scalar(select(Work.work_key).limit(1))

    async def on_loop_then_off() -> None:
        query()
        await asyncio.to_thread(query)

    with _LoopQueries() as seen:
        asyncio.run(on_loop_then_off())
    assert len(seen.statements) == 1


# Routes that still query on the loop, through code the request awaits rather
# than code in the route itself: the inbox handlers, the opt-out purge and the
# fanout of a refreshed profile. They run on every ingested event too, and move
# off the loop with the ingest pipeline, separately.
_STILL_ON_THE_LOOP = {
    ("POST", "/inbox"),
    ("POST", "/actor/inbox"),
    ("POST", "/users/{ident}/inbox"),
    ("POST", "/manage/opt-out"),
    ("GET", "/oauth/callback"),
}


@pytest.fixture
def operator(settings: Settings, fixture_path, monkeypatch) -> tuple[TestClient, str]:
    """A client signed in as an operator of a v2 deployment with an API key,
    over the fixture archive, with every network call stubbed out."""
    asyncio.run(replay_file(fixture_path, allow_network=False))
    set_settings(replace(settings, admins=(DID,), jetstream_api_key="key"))
    admin.reset_cache()
    sessions._SESSIONS.clear()
    monkeypatch.setattr(
        oauth, "finish_flow", lambda state, code, iss: oauth.FlowResult(did=DID, handle="x")
    )
    monkeypatch.setattr(oauth, "start_flow", lambda identifier: None)
    monkeypatch.setattr(identity, "_http_json", lambda url, timeout=8.0: None)
    monkeypatch.setattr(archive_replay, "start", lambda job_id, worker=None: True)
    monkeypatch.setattr(backfill, "launch_import", lambda did, worker=None: True)

    async def no_estimate(**kwargs):
        raise archive_replay.ArchiveError("no archive in tests")

    monkeypatch.setattr(archive_replay, "estimate", no_estimate)
    client = TestClient(app, base_url="https://bridge.test")
    client.get("/oauth/callback", params={"state": "s", "code": "c"}, follow_redirects=False)
    match = re.search(r'name="csrf" value="([^"]+)"', client.get("/manage").text)
    assert match, "not signed in"
    return client, match.group(1)


def test_no_route_queries_on_the_event_loop(operator):
    client, csrf = operator
    with session_scope() as session:
        post = session.scalar(
            select(Record).where(Record.ap_object_json.isnot(None), Record.deleted_at.is_(None))
        )
        assert post is not None
        author = identity.actor_by_ident(post.did)
        assert author is not None
        handle = author.handle
        work_type, _, work_id = (post.work_key or "").partition(":")
        record_path = f"{post.did}/{post.collection}/{post.rkey}"
    ap = {"accept": "application/activity+json"}
    form = {"csrf": csrf}

    requests: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = {
        ("GET", "/"): [("/", {})],
        ("GET", "/stats"): [("/stats", {})],
        ("GET", "/robots.txt"): [("/robots.txt", {})],
        ("GET", "/.well-known/webfinger"): [
            (f"/.well-known/webfinger?resource=acct:{handle}@bridge.test", {})
        ],
        ("GET", "/.well-known/nodeinfo"): [("/.well-known/nodeinfo", {})],
        ("GET", "/nodeinfo/2.1"): [("/nodeinfo/2.1", {})],
        ("GET", "/actor"): [("/actor", {})],
        ("GET", "/users/{ident}"): [(f"/users/{handle}", {}), (f"/users/{handle}", ap)],
        ("GET", "/users/{ident}/followers"): [(f"/users/{handle}/followers", {})],
        ("GET", "/users/{ident}/outbox"): [(f"/users/{handle}/outbox", {})],
        ("GET", "/users/{ident}/posts/{rkey}"): [
            (f"/users/{handle}/posts/{post.rkey}", {}),
            (f"/users/{handle}/posts/{post.rkey}", ap),
        ],
        ("GET", "/objects/{ident}/{rkey}"): [(f"/objects/{handle}/{post.rkey}", {})],
        ("GET", "/catalog/{work_type}/{work_id}"): [
            (f"/catalog/{work_type}/{work_id}", {}),
            (f"/catalog/{work_type}/{work_id}", ap),
        ],
        ("GET", "/catalog"): [("/catalog", {})],
        ("GET", "/~neodb~/{path:path}"): [(f"/~neodb~/catalog/{work_type}/{work_id}", {})],
        ("GET", "/archive"): [("/archive", {}), ("/archive?q=popfeed", {})],
        ("GET", "/archive/{at_uri:path}"): [(f"/archive/{record_path}", {})],
        ("GET", "/manage"): [("/manage", {})],
        ("GET", "/oauth/client-metadata.json"): [("/oauth/client-metadata.json", {})],
    }
    posts: dict[tuple[str, str], dict[str, Any]] = {
        ("POST", "/manage"): {"identifier": handle},
        ("POST", "/manage/opt-in"): form,
        ("POST", "/manage/import"): form,
        ("POST", "/manage/admin/import/dry-run"): {**form, "after_seq": "0"},
        ("POST", "/manage/admin/import"): {**form, "after_seq": "0"},
        ("POST", "/manage/admin/import/resume"): {**form, "job_id": "1"},
        ("POST", "/manage/admin/import/cancel"): form,
        ("POST", "/manage/signout"): {},
    }

    routes = {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods or ()
        if method != "HEAD"
    }
    # Every route is either exercised here or listed as a known exception, so
    # a new route cannot skip the check unnoticed.
    assert routes == set(requests) | set(posts) | _STILL_ON_THE_LOOP

    with _LoopQueries() as seen:
        for key, calls in requests.items():
            for path, headers in calls:
                r = client.get(path, headers=headers, follow_redirects=False)
                assert r.status_code < 500, (path, r.status_code)
                assert not seen.statements, (key, path, seen.statements)
        # Signing out ends the session, so it goes last.
        for (_, path), data in sorted(posts.items(), key=lambda kv: kv[0][1] == "/manage/signout"):
            r = client.post(path, data=data, follow_redirects=False)
            assert r.status_code < 500, (path, r.status_code)
            assert not seen.statements, (path, seen.statements)
