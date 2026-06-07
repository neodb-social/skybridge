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
    assert doc["software"]["name"] == "skybridge"
    assert doc["usage"]["users"]["total"] >= 1


def test_post_object_dereferenceable(client, settings):
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.post",
                Record.deleted_at.is_(None),
            )
        )
        handle = session.get(BridgedActor, rec.did).handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 200
    assert r.json()["type"] == "Note"


def test_deleted_object_is_tombstone(client, settings):
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.deleted_at.isnot(None)))
        assert rec is not None
        handle = session.get(BridgedActor, rec.did).handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 410
    assert r.json()["type"] == "Tombstone"


def test_catalog_object(client):
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.work_key.isnot(None)))
        assert rec is not None
        work_key = rec.work_key
    work_type, _, work_id = work_key.partition(":")
    r = client.get(f"/catalog/{work_type}/{work_id}", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["id"].endswith(f"/catalog/{work_type}/{work_id}")


def test_stats_json(client):
    r = client.get("/stats")
    assert r.status_code == 200
    assert r.json()["records_total"] > 0


def test_dashboard_and_archive_html(client):
    assert client.get("/").status_code == 200
    assert "Skybridge" in client.get("/").text
    assert client.get("/archive").status_code == 200
    assert client.get("/catalog").status_code == 200


def test_outbox_lists_posts(client, settings):
    handle = _a_bridged_handle()
    r = client.get(f"/users/{handle}/outbox", headers=AP)
    assert r.status_code == 200
    assert r.json()["type"] == "OrderedCollection"
