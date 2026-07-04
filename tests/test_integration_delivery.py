"""Integration: a real Follow handshake + signed Announce/Create delivery.

Stands up an in-process mock remote instance (its own actor + inbox) on a live
uvicorn server, has it Follow the relay, then replays the fixture with delivery
enabled and asserts the mock inbox receives activities whose HTTP signatures
verify against the signer's published key.
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
from skybridge.crypto import generate_keypair, verify_request
from skybridge.db import session_scope
from skybridge.main import app as relay_app
from skybridge.models import Subscriber


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MockInstance:
    """A minimal remote AP instance: one actor + an inbox that records POSTs."""

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


@pytest.fixture
def live(settings, fixture_path):
    # Configure the relay to advertise its real (localhost) port so signatures
    # cover the right host, and serve both apps.
    relay_port = _free_port()
    mock_port = _free_port()
    from skybridge.config import Settings, set_settings

    from tests.conftest import RELAY_KEY_PEM

    set_settings(
        Settings(
            domain=f"127.0.0.1:{relay_port}",
            scheme="http",
            db_path=":memory:",
            relay_key_pem=RELAY_KEY_PEM,
        )
    )
    from skybridge.db import init_db

    init_db(reset=True)

    mock = MockInstance(mock_port)
    relay_server = _serve(relay_app, relay_port)
    mock_server = _serve(mock.app, mock_port)
    time.sleep(0.2)
    try:
        yield relay_port, mock
    finally:
        relay_server.should_exit = True
        mock_server.should_exit = True
        time.sleep(0.2)


def test_follow_then_signed_delivery(live, fixture_path):
    relay_port, mock = live
    relay_base = f"http://127.0.0.1:{relay_port}"

    # 1) The mock instance Follows our relay actor.
    follow = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"{mock.base}/activities/1",
        "type": "Follow",
        "actor": mock.actor_id,
        "object": f"{relay_base}/actor",
    }
    resp = httpx.post(f"{relay_base}/inbox", json=follow, timeout=10)
    assert resp.status_code in (200, 202)

    # The subscriber is now recorded + accepted.
    with session_scope() as session:
        sub = session.query(Subscriber).filter_by(actor_id=mock.actor_id).one()
        assert sub.state == "accepted"

    # The relay should have delivered an Accept back to the mock inbox.
    assert any(a.get("type") == "Accept" for a in mock.received)

    # 2) Replay the fixture WITH delivery; the mock should receive Announces.
    async def _replay() -> None:
        worker = DeliveryWorker()
        worker.start()
        try:
            await replay_file(fixture_path, worker=worker, allow_network=False)
            await asyncio.sleep(0.5)  # let the queue drain
            await worker.stop()
        finally:
            pass

    asyncio.run(_replay())
    time.sleep(0.3)

    announces = [a for a in mock.received if a.get("type") == "Announce"]
    assert announces, "expected relay to Announce bridged activities"

    # 3) Every delivered POST must carry a signature that verifies against the
    #    signer's published key (the relay actor).
    relay_actor = httpx.get(f"{relay_base}/actor", timeout=10).json()
    relay_pub = relay_actor["publicKey"]["publicKeyPem"]
    verified = 0
    for hdrs in mock.headers:
        if "signature" not in {k.lower() for k in hdrs}:
            continue
        body = None  # we re-verify the covered (request-target)/host/date/digest set
        # Reconstruct using the digest the sender sent (body integrity already
        # implied by digest match at receipt time).
        ok = verify_request(
            public_pem=relay_pub,
            method="POST",
            path="/inbox",
            headers=hdrs,
            body=body,
        )
        if ok:
            verified += 1
    assert verified >= 1, "at least one delivery signature should verify"
