"""HTTP endpoint tests via FastAPI's TestClient.

The client is constructed WITHOUT entering its context manager so the app
lifespan (which would re-init the DB and start the delivery worker) does not
run — we drive the routes against the conftest in-memory DB seeded by the
fixture replay.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from skybridge.atproto.replay import replay_file
from skybridge.db import session_scope
from skybridge.main import app
from skybridge.models import BridgedActor, Record
from skybridge.pipeline import process_event
from sqlalchemy import select

AP = {"accept": "application/activity+json"}


@pytest.fixture
def client(settings, fixture_path) -> TestClient:
    asyncio.run(replay_file(fixture_path, allow_network=False))
    return TestClient(app)


def _a_bridged_handle() -> str:
    with session_scope() as session:
        actor = session.scalar(
            select(BridgedActor).where(BridgedActor.did != "did:skybridge:relay")
        )
        assert actor is not None
        return actor.handle


def test_webfinger_relay(client, settings):
    r = client.get(
        "/.well-known/webfinger", params={"resource": settings.acct(settings.relay_username)}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["links"][0]["href"] == settings.relay_actor_id


def test_webfinger_unknown_returns_404(client):
    r = client.get("/.well-known/webfinger", params={"resource": "acct:nobody@bridge.test"})
    assert r.status_code == 404


def test_webfinger_wrong_domain(client):
    r = client.get("/.well-known/webfinger", params={"resource": "acct:x@elsewhere.example"})
    assert r.status_code == 404


def test_relay_actor_document(client, settings):
    r = client.get("/actor", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Application"
    assert doc["id"] == settings.relay_actor_id
    assert doc["publicKey"]["id"].endswith("#main-key")
    assert doc["endpoints"]["sharedInbox"] == settings.url("inbox")


def test_person_actor_document(client, settings):
    handle = _a_bridged_handle()
    r = client.get(f"/users/{handle}", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Person"
    assert doc["preferredUsername"] == handle
    assert "publicKey" in doc


def test_webfinger_resolves_bridged_user(client, settings):
    handle = _a_bridged_handle()
    r = client.get("/.well-known/webfinger", params={"resource": settings.acct(handle)})
    assert r.status_code == 200
    assert r.json()["links"][0]["href"] == settings.actor_id(handle)


def test_nodeinfo(client):
    disc = client.get("/.well-known/nodeinfo")
    assert disc.status_code == 200
    href = disc.json()["links"][0]["href"]
    assert href.endswith("/nodeinfo/2.1")
    doc = client.get("/nodeinfo/2.1").json()
    assert doc["software"]["name"] == "neodb-skybridge"
    assert doc["usage"]["users"]["total"] >= 1


def test_review_object_dereferenceable(client, settings):
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(Record.collection == "social.popfeed.feed.review")
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Note"
    assert any(rel["type"] == "Rating" for rel in doc["relatedWith"])


def test_deleted_object_is_tombstone(client, settings):
    # Delete a published record (a review), then expect a 410 Tombstone.
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.review",
                Record.ap_object_json.isnot(None),
            )
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        did, collection, rkey = rec.did, rec.collection, rec.rkey
    event = {
        "did": did,
        "kind": "commit",
        "commit": {"operation": "delete", "collection": collection, "rkey": rkey},
    }
    asyncio.run(process_event(event, allow_network=False))
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 410
    assert r.json()["type"] == "Tombstone"


def test_never_published_deleted_record_is_404(client, settings):
    # The fixture's delete is collection membership: never federated, so its
    # URL was never valid — 404, not a Tombstone.
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.deleted_at.isnot(None)))
        assert rec is not None
        assert rec.ap_object_json is None and rec.ap_activity_json is None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 404


def test_catalog_object_is_neodb_item(client, settings):
    # The fixture review's movie work, in NeoDB ItemSchema shape.
    r = client.get("/catalog/movie/imdbId-tt6710474", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    # catalog/sites/fedi.py requirements: supported type, id == fetched url
    assert doc["type"] == "Movie"
    assert doc["id"] == settings.catalog_id("movie", "imdbId-tt6710474")
    assert doc["display_title"] == "Everything Everywhere All at Once"
    # identifier URLs let the peer merge with its existing catalog
    urls = [e["url"] for e in doc["external_resources"]]
    assert "https://www.imdb.com/title/tt6710474" in urls
    assert "https://www.themoviedb.org/movie/545611" in urls
    assert doc["imdb"] == "tt6710474"
    assert doc["cover_image_url"].startswith("https://")


def test_stats_json(client):
    r = client.get("/stats")
    assert r.status_code == 200
    assert r.json()["records_total"] > 0


def test_dashboard_and_archive_html(client):
    assert client.get("/").status_code == 200
    assert "Skybridge" in client.get("/").text
    assert client.get("/archive").status_code == 200
    assert client.get("/catalog").status_code == 200


def test_robots_txt_rejects_all(client):
    r = client.get("/robots.txt")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "User-agent: *" in r.text
    assert "Disallow: /" in r.text


def test_archive_only_list_records_not_published(client, settings):
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.collection == "social.popfeed.feed.list"))
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    # Never emitted to AP: not dereferenceable (404, not a Tombstone)...
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 404
    # ...and absent from the outbox.
    outbox = client.get(f"/users/{handle}/outbox", headers=AP).json()
    assert outbox["type"] == "OrderedCollection"
    assert settings.post_id(handle, rkey) not in outbox["orderedItems"]
    assert outbox["totalItems"] > 0
