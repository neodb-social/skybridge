"""Endpoint/cursor handling across the two Jetstream dialects, plus reconnect
backoff."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import pytest
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto import jetstream
from skybridge.config import DEFAULT_JETSTREAM, Settings, set_settings
from skybridge.db import session_scope
from skybridge.models import Cursor

V1_URL = "wss://jetstream2.us-east.bsky.network/subscribe"


def _use(settings: Settings, url: str) -> Settings:
    updated = replace(settings, jetstream_url=url)
    set_settings(updated)
    return updated


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def test_default_endpoint_is_v2(settings):
    assert settings.jetstream_is_v2
    assert DEFAULT_JETSTREAM.endswith("network.bsky.jetstream.subscribeEvents")


def test_v1_endpoint_is_still_recognised(settings):
    assert not _use(settings, V1_URL).jetstream_is_v2


def test_v2_url_uses_the_v2_parameter_names(settings):
    query = _query(jetstream._build_url())
    assert query["collections"] == list(settings.wanted_collections)
    assert query["kinds"] == list(settings.wanted_kinds)
    assert "wantedCollections" not in query


def test_v1_url_keeps_the_legacy_parameter_names(settings):
    updated = _use(settings, V1_URL)
    query = _query(jetstream._build_url())
    assert query["wantedCollections"] == list(updated.wanted_collections)
    assert "collections" not in query and "kinds" not in query


def test_http_base_is_derived_from_the_websocket_url(settings):
    assert settings.jetstream_http_base == "https://jetstream.us-east.bsky.network"
    assert (
        _use(
            settings, "ws://localhost:6008/xrpc/network.bsky.jetstream.subscribeEvents"
        ).jetstream_http_base
        == "http://localhost:6008"
    )


def test_seq_and_time_us_are_stored_in_the_right_column(settings):
    jetstream.save_cursor(24_762_496_440)  # a v2 seq
    jetstream.save_cursor(1_786_809_900_742_261)  # a v1 time_us
    with session_scope() as session:
        row = session.get(Cursor, 1)
        assert row is not None
        assert row.seq == 24_762_496_440
        assert row.time_us == 1_786_809_900_742_261


def test_v2_resumes_from_a_legacy_time_us_cursor(settings):
    """Upgrading a running deployment must not restart from the tip: v2
    accepts a microsecond cursor and resolves it by magnitude."""
    jetstream.save_cursor(1_786_809_900_742_261)
    assert jetstream.load_cursor() == 1_786_809_900_742_261
    assert _query(jetstream._build_url())["cursor"] == ["1786809900742261"]


def test_v2_prefers_seq_once_one_is_recorded(settings):
    jetstream.save_cursor(1_786_809_900_742_261)
    jetstream.save_cursor(24_762_496_440)
    assert jetstream.load_cursor() == 24_762_496_440


def test_v1_never_receives_a_seq_as_a_cursor(settings):
    """A v1 host reads the cursor as microseconds, so a seq would mean 1970."""
    jetstream.save_cursor(24_762_496_440)
    _use(settings, V1_URL)
    assert jetstream.load_cursor() == 0
    assert "cursor" not in _query(jetstream._build_url())


class _StopLoop(Exception):
    """Escape hatch out of the endless reconnect loop, once enough retries ran.

    Raised from inside the loop's ``except Exception`` handler, so the handler
    that swallows transport errors cannot swallow this too.
    """


class _FlappingConnect:
    """A host that accepts the upgrade, then drops with no close frame."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __aenter__(self) -> _FlappingConnect:
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False

    def __aiter__(self) -> _FlappingConnect:
        return self

    async def __anext__(self):
        raise ConnectionError("no close frame received or sent")


def _run_until(monkeypatch, retries: int) -> list[float]:
    """Reconnect ``retries`` times against a flapping host; return the delays.

    Stubbing the jitter is what makes the sequence readable: it records the
    nominal backoff and hands back a zero delay, so the test neither sleeps
    nor has to reason about a random draw.
    """
    delays: list[float] = []

    def fake_uniform(low: float, high: float) -> float:
        delays.append(high)
        if len(delays) >= retries:
            raise _StopLoop
        return 0.0

    monkeypatch.setattr(jetstream.websockets, "connect", _FlappingConnect)
    monkeypatch.setattr(jetstream, "uniform", fake_uniform)
    with pytest.raises(_StopLoop):
        asyncio.run(jetstream.run(DeliveryWorker()))
    return delays


def test_a_flapping_host_backs_off_to_one_retry_a_minute(settings, monkeypatch):
    """A completed handshake is not health. A host that accepts the upgrade
    and drops the stream at once must not be retried once a second forever."""
    assert _run_until(monkeypatch, retries=8) == [1, 2, 4, 8, 16, 32, 60, 60]


def test_a_healthy_connection_resets_the_backoff(settings, monkeypatch):
    """A stream that ran for a while before dropping is an isolated blip, so
    the next attempt starts from one second again rather than from the cap."""
    # Each pass reads the clock twice: on open, then on the drop.
    clock = iter([0.0, jetstream._HEALTHY_AFTER + 1] * 4)
    monkeypatch.setattr(jetstream, "monotonic", lambda: next(clock))
    assert _run_until(monkeypatch, retries=4) == [1, 1, 1, 1]
