"""Shared fixtures: an isolated in-memory DB + a fixed test domain per test."""

from __future__ import annotations

from pathlib import Path

import pytest
from skybridge.config import Settings, set_settings
from skybridge.db import init_db

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def settings() -> Settings:
    s = Settings(domain="bridge.test", scheme="https", db_path=":memory:")
    set_settings(s)
    return s


@pytest.fixture(autouse=True)
def _db(settings: Settings):
    # init_db reads the active settings (db_path=:memory:) installed above.
    init_db(reset=True)
    yield
    set_settings(None)


@pytest.fixture
def fixture_path() -> Path:
    return FIXTURES / "jetstream_sample.jsonl"
