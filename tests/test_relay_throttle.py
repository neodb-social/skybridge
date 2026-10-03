"""Per-account relay throttle: Creates over the hourly allowance skip relays."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from skybridge.activitypub import delivery
from skybridge.config import _from_env, get_settings, set_settings

RELAY = "https://relay.test/inbox"
PEER = "https://peer.test/inbox"


@pytest.fixture(autouse=True)
def _wiring(monkeypatch):
    monkeypatch.setattr(delivery, "relay_inboxes", lambda: [RELAY])
    monkeypatch.setattr(delivery, "follower_targets", lambda did: [PEER])
    monkeypatch.setattr(delivery, "_author_key", lambda did: ("pem", "key"))
    monkeypatch.setattr(delivery, "create_ld_signature", lambda *a, **k: {"sig": 1})


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(delivery, "_clock", lambda: now[0])
    return now


def _limit(n: int) -> None:
    set_settings(replace(get_settings(), relay_creates_per_hour=n))


def _send(did: str, kind: str, n: int = 1) -> dict[str, int]:
    """Fan out ``n`` activities of ``kind``; count queued tasks per inbox."""
    worker = delivery.DeliveryWorker()  # never started: tasks stay queued
    for i in range(n):
        activity = {"id": f"https://bridge.test/objects/{did}/{i}#x", "type": kind}
        asyncio.run(delivery.fanout(worker, record_uri=f"at://{i}", did=did, activity=activity))
    counts = {RELAY: 0, PEER: 0}
    while not worker.queue.empty():
        counts[worker.queue.get_nowait().target_inbox] += 1
    return counts


def test_limit_defaults_to_ten_and_reads_env(monkeypatch):
    monkeypatch.delenv("SKYBRIDGE_RELAY_CREATES_PER_HOUR", raising=False)
    assert _from_env().relay_creates_per_hour == 10
    monkeypatch.setenv("SKYBRIDGE_RELAY_CREATES_PER_HOUR", "0")
    assert _from_env().relay_creates_per_hour == 0
    monkeypatch.setenv("SKYBRIDGE_RELAY_CREATES_PER_HOUR", "-1")
    with pytest.raises(ValueError):
        _from_env()


def test_overflow_creates_skip_relays_but_reach_followers(clock):
    _limit(5)
    assert _send("did:plc:a", "Create", 6) == {RELAY: 5, PEER: 6}


def test_updates_and_deletes_are_not_counted_or_held(clock):
    _limit(2)
    _send("did:plc:a", "Create", 2)
    assert _send("did:plc:a", "Update", 3) == {RELAY: 3, PEER: 3}
    assert _send("did:plc:a", "Delete", 3) == {RELAY: 3, PEER: 3}
    assert _send("did:plc:a", "Create") == {RELAY: 0, PEER: 1}


def test_accounts_are_counted_separately(clock):
    _limit(1)
    assert _send("did:plc:a", "Create", 2) == {RELAY: 1, PEER: 2}
    assert _send("did:plc:b", "Create") == {RELAY: 1, PEER: 1}


def test_window_slides(clock):
    _limit(2)
    _send("did:plc:a", "Create")
    clock[0] += 1800
    _send("did:plc:a", "Create")
    assert _send("did:plc:a", "Create") == {RELAY: 0, PEER: 1}
    clock[0] += 1800  # the first Create is now an hour old
    assert _send("did:plc:a", "Create") == {RELAY: 1, PEER: 1}
    assert _send("did:plc:a", "Create") == {RELAY: 0, PEER: 1}


def test_zero_means_no_limit(clock):
    _limit(0)
    assert _send("did:plc:a", "Create", 30) == {RELAY: 30, PEER: 30}
    assert not delivery._relayed_creates


def test_no_relays_counts_nothing(monkeypatch, clock):
    _limit(1)
    monkeypatch.setattr(delivery, "relay_inboxes", list)
    _send("did:plc:a", "Create", 3)
    assert not delivery._relayed_creates
