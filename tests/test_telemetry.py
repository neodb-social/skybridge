"""Sentry telemetry is opt-in and must never call real sentry_sdk in tests."""

from __future__ import annotations

import pytest
from skybridge import telemetry


@pytest.fixture(autouse=True)
def _reset_enabled():
    telemetry._enabled = False
    yield
    telemetry._enabled = False


def test_init_sentry_returns_false_without_dsn(settings):
    # `settings` fixture (conftest.py) installs a Settings with no sentry_dsn.
    assert settings.sentry_dsn is None
    assert telemetry.init_sentry() is False
    assert telemetry._enabled is False


def test_record_ingested_noop_when_disabled(monkeypatch):
    import sentry_sdk

    def _boom(*args, **kwargs):
        raise AssertionError("sentry_sdk.metrics.count must not be called when disabled")

    monkeypatch.setattr(sentry_sdk.metrics, "count", _boom)
    telemetry._enabled = False
    telemetry.record_ingested("social.popfeed.feed.review", "create")


def test_record_ingested_calls_sentry_metrics_count(monkeypatch):
    calls = []

    def _fake_count(name, value, unit=None, attributes=None):
        calls.append((name, value, attributes))

    import sentry_sdk

    monkeypatch.setattr(sentry_sdk.metrics, "count", _fake_count)
    telemetry._enabled = True
    telemetry.record_ingested("social.popfeed.feed.review", "create")
    assert calls == [
        (
            "atproto.record_ingested",
            1,
            {"collection": "social.popfeed.feed.review", "operation": "create"},
        )
    ]


def test_record_ingested_swallows_exceptions(monkeypatch):
    import sentry_sdk

    def _boom(*args, **kwargs):
        raise RuntimeError("sentry is down")

    monkeypatch.setattr(sentry_sdk.metrics, "count", _boom)
    telemetry._enabled = True
    telemetry.record_ingested("social.popfeed.feed.review", "create")  # must not raise
