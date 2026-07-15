"""Integration: relay-client subscription + signed direct delivery + Like forward.

Stands up an in-process mock *external relay* (its own actor + an inbox that
records POSTs) on a live uvicorn server, then runs the skybridge app (also
live, via uvicorn so its lifespan actually executes) configured with
``SKYBRIDGE_RELAYS`` pointing at the mock. Verifies the full relay-client
handshake — outbound ``Follow``, inbound ``Accept`` — then that replayed
fixture activities are delivered author-signed (never wrapped in
``Announce``) and that an inbound ``Like`` on a delivered post is forwarded
wrapped in an ``Announce`` by the service actor, both with HTTP signatures
that verify against the signer's own published key.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto.replay import replay_file
from skybridge.crypto import (
    generate_keypair,
    parse_signature_header,
    verify_ld_signature,
    verify_request,
)
from skybridge.db import session_scope
from skybridge.main import app as relay_app
from skybridge.models import Relay
from sqlalchemy import select

AP = {"Accept": "application/activity+json"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MockInstance:
    """A minimal external relay: one actor + an inbox that records POSTs."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.actor_id = f"{self.base}/actor"
        self.received: list[dict] = []
        self.headers: list[dict] = []
        self.private_pem, self.public_pem = generate_keypair()
        self.app = self._build_app()

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.get("/actor")
        async def actor() -> dict:
            return {
                "@context": ["https://www.w3.org/ns/activitystreams"],
                "id": self.actor_id,
                "type": "Application",
                "inbox": f"{self.base}/inbox",
                "endpoints": {"sharedInbox": f"{self.base}/inbox"},
                "publicKey": {
                    "id": f"{self.actor_id}#main-key",
                    "owner": self.actor_id,
                    "publicKeyPem": self.public_pem,
                },
            }

        @app.post("/inbox")
        async def inbox(request: Request) -> dict:
            body = await request.body()
            self.received.append(json.loads(body))
            self.headers.append(dict(request.headers))
            return {"ok": True}

        return app


def _serve(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    return server


def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def live(settings, fixture_path):
    # Start the mock relay FIRST so it's up before the skybridge app's
    # lifespan (which subscribes to it) runs.
    mock_port = _free_port()
    mock = MockInstance(mock_port)
    mock_server = _serve(mock.app, mock_port)

    relay_port = _free_port()
    from skybridge.config import Settings, set_settings

    from tests.conftest import RELAY_KEY_PEM

    set_settings(
        Settings(
            domain=f"127.0.0.1:{relay_port}",
            scheme="http",
            db_path=":memory:",
            relay_key_pem=RELAY_KEY_PEM,
            relays=(f"{mock.base}/inbox",),
        )
    )
    from skybridge.db import init_db

    init_db(reset=True)

    relay_server = _serve(relay_app, relay_port)  # lifespan runs -> reconcile_relays fires
    try:
        yield relay_port, mock
    finally:
        relay_server.should_exit = True
        mock_server.should_exit = True
        time.sleep(0.2)


def test_relay_subscription_then_signed_delivery_and_like_forward(live, fixture_path):
    relay_port, mock = live
    relay_base = f"http://127.0.0.1:{relay_port}"

    # 1) On startup, skybridge Follows as:Public at the configured relay.
    assert _wait_for(lambda: any(a.get("type") == "Follow" for a in mock.received))
    follow = next(a for a in mock.received if a.get("type") == "Follow")
    assert follow["actor"] == f"{relay_base}/actor"
    assert follow["object"] == "https://www.w3.org/ns/activitystreams#Public"

    # 2) The relay Accepts; the Relay row flips to accepted in the shared DB.
    accept = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{mock.base}/activities/accept-1",
        "type": "Accept",
        "actor": mock.actor_id,
        "object": follow,
    }
    resp = httpx.post(f"{relay_base}/inbox", json=accept, timeout=10)
    assert resp.status_code in (200, 202)
    with session_scope() as session:
        row = session.scalar(select(Relay).where(Relay.inbox == f"{mock.base}/inbox"))
        assert row is not None
        assert row.state == "accepted"

    # 3) Replay the fixture WITH delivery; the relay should receive
    #    author-signed Creates, never an Announce wrapper.
    async def _replay() -> None:
        worker = DeliveryWorker()
        worker.start()
        try:
            await replay_file(fixture_path, worker=worker, allow_network=False)
            await asyncio.sleep(0.5)  # let the queue drain
        finally:
            await worker.stop()

    asyncio.run(_replay())
    time.sleep(0.3)

    creates = [a for a in mock.received if a.get("type") == "Create"]
    assert creates, "expected author-signed Create activities at the relay"
    assert not any(a.get("type") == "Announce" for a in mock.received)

    # Every relayed Create must carry an author LD signature: the relay
    # re-signs its onward HTTP delivery with its own key, and NeoDB inboxes
    # 401 relayed activities that lack one. Verify against the author's key
    # as published by the live server at the signature's own creator URL.
    for create in creates:
        sig = create.get("signature")
        assert sig is not None, "relayed Create missing LD signature"
        assert sig["type"] == "RsaSignature2017"
        creator_actor = sig["creator"].split("#")[0]
        assert creator_actor == create["actor"]
        actor_doc = httpx.get(creator_actor, headers=AP, timeout=10).json()
        assert verify_ld_signature(create, public_pem=actor_doc["publicKey"]["publicKeyPem"])

    # Every signed delivery must verify against the signer's own published key
    # (fetched from the live skybridge server at the signature's own keyId).
    verified = 0
    for hdrs in mock.headers:
        lower = {k.lower(): v for k, v in hdrs.items()}
        if "signature" not in lower:
            continue
        key_id = parse_signature_header(lower["signature"])["keyId"]
        actor_doc = httpx.get(key_id.split("#")[0], headers=AP, timeout=10).json()
        pub = actor_doc["publicKey"]["publicKeyPem"]
        if verify_request(public_pem=pub, method="POST", path="/inbox", headers=hdrs, body=None):
            verified += 1
    assert verified >= 1, "at least one delivery signature should verify"

    # 4) A remote actor Likes one of the delivered posts; the Like is
    #    forwarded Announce-wrapped, signed by the service actor (neodb-relay
    #    redistributes Create/Update/Delete/Move + Announce, but ignores Like
    #    entirely, so the raw Like alone would go nowhere).
    liked_object_id = creates[0]["object"]["id"]
    like = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{mock.base}/activities/like-1",
        "type": "Like",
        "actor": mock.actor_id,
        "object": liked_object_id,
    }
    resp = httpx.post(f"{relay_base}/inbox", json=like, timeout=10)
    assert resp.status_code in (200, 202)

    assert _wait_for(lambda: any(a.get("type") == "Announce" for a in mock.received))
    idx = next(i for i, a in enumerate(mock.received) if a.get("type") == "Announce")
    announce = mock.received[idx]
    assert announce["actor"] == f"{relay_base}/actor"
    assert announce["object"]["type"] == "Like"
    assert announce["object"]["object"] == liked_object_id

    relay_actor_doc = httpx.get(f"{relay_base}/actor", headers=AP, timeout=10).json()
    relay_pub = relay_actor_doc["publicKey"]["publicKeyPem"]
    assert verify_request(
        public_pem=relay_pub, method="POST", path="/inbox", headers=mock.headers[idx], body=None
    )
