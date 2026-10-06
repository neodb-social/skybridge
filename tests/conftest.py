"""Shared fixtures: an isolated in-memory DB + a fixed test domain per test."""

from __future__ import annotations

from pathlib import Path

import pytest
from skybridge.activitypub import delivery
from skybridge.atproto import identity
from skybridge.config import Settings, set_settings
from skybridge.crypto import generate_keypair
from skybridge.db import init_db
from skybridge.stats import reset_usage

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"

# The relay key is operator-provided (never minted); one throwaway key
# serves every test via Settings.relay_key_pem.
RELAY_KEY_PEM = generate_keypair()[0]


@pytest.fixture
def settings(tmp_path) -> Settings:
    s = Settings(
        domain="bridge.test",
        scheme="https",
        db_path=":memory:",
        relay_key_pem=RELAY_KEY_PEM,
        relay_key_file=str(tmp_path / "relay_key.pem"),
    )
    set_settings(s)
    return s


@pytest.fixture(autouse=True)
def _db(settings: Settings):
    # init_db reads the active settings (db_path=:memory:) installed above.
    init_db(reset=True)
    # The NodeInfo counts are cached in a module global, so they would
    # otherwise outlive the database they were counted from.
    reset_usage()
    # Same for the per-account relay throttle counter.
    delivery.reset_relay_throttle()
    yield
    set_settings(None)


@pytest.fixture(autouse=True)
def _handles_verify(monkeypatch):
    """Treat every PLC-claimed handle as resolving back to its DID.

    The real check does DNS and HTTPS; tests that exercise the failure case
    override this themselves.
    """
    monkeypatch.setattr(identity, "_handle_points_at", lambda handle, did: True)


@pytest.fixture
def fixture_path() -> Path:
    return FIXTURES / "jetstream_sample.jsonl"
