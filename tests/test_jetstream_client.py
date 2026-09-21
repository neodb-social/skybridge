"""Endpoint/cursor handling across the two Jetstream dialects, plus reconnect
backoff."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import pytest
from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto import jetstream
from skybridge.config import Settings, set_settings
from skybridge.db import session_scope
from skybridge.models import Cursor, ImportJob
from sqlalchemy import select
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

V1_URL = "wss://jetstream2.us-east.bsky.network/subscribe"


def _use(settings: Settings, url: str) -> Settings:
    updated = replace(settings, jetstream_url=url)
    set_settings(updated)
    return updated


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


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


# --------------------------------------------------------------------------- #
# Stale cursor recovery
# --------------------------------------------------------------------------- #
# The real numbers from the incident this handling was written for: the stored
# cursor had fallen a day and a half behind the host's 36-hour window.
STALE = 25_993_213_538
FLOOR = 26_083_064_123
RESUMED_AT = 26_083_064_500

_TOO_OLD = json.dumps(
    {
        "error": "CursorTooOld",
        "message": (
            f"subscribe: cursor too old: cursor {STALE} below lookback floor {FLOOR}; "
            "re-backfill from your last seq"
        ),
    }
).encode()

_OTHER_400 = json.dumps({"error": "InvalidRequest", "message": "unknown collection"}).encode()


def _account_frame(seq: int) -> str:
    """A v2 frame the pipeline ignores, so only the cursor handling is tested."""
    return json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#account",
                "seq": seq,
                "did": "did:plc:52twwx5hy574zzx3ghaqdita",
                "time_us": 1_789_000_000_000_000,
            },
        }
    )


def _host(monkeypatch, script: list[str], *, body: bytes = _TOO_OLD) -> list[str]:
    """Patch ``connect`` with a host that plays out ``script``, one entry per
    connection attempt: ``refuse`` rejects the upgrade with ``body``, ``drop``
    accepts and closes with nothing, ``event`` serves one event and closes.

    The run ends when the script does, through the stubbed jitter. Returns the
    URLs the host was asked for, in order — a refused cursor never reaches the
    jitter, so the URLs are the only record of what each attempt asked for.
    """
    urls: list[str] = []
    steps = iter(script)
    used = {"n": 0}

    class _Connect:
        def __init__(self, url, **kwargs) -> None:
            urls.append(url)
            self.step = next(steps, "drop")
            used["n"] += 1
            self.served = False

        async def __aenter__(self) -> _Connect:
            if self.step == "refuse":
                raise InvalidStatus(Response(400, "Bad Request", Headers(), body))
            return self

        async def __aexit__(self, *exc_info) -> bool:
            return False

        def __aiter__(self) -> _Connect:
            return self

        async def __anext__(self) -> str:
            if self.step == "event" and not self.served:
                self.served = True
                return _account_frame(RESUMED_AT)
            raise ConnectionError("no close frame received or sent")

    async def _ignore(event, **kwargs):
        return None

    def _stop(low: float, high: float) -> float:
        if used["n"] >= len(script):
            raise _StopLoop
        return 0.0

    monkeypatch.setattr(jetstream.websockets, "connect", _Connect)
    monkeypatch.setattr(jetstream, "process_event", _ignore)
    monkeypatch.setattr(jetstream, "uniform", _stop)
    with pytest.raises(_StopLoop):
        asyncio.run(jetstream.run(DeliveryWorker()))
    return urls


def _queued_jobs() -> list[tuple[int, int, bool]]:
    with session_scope() as session:
        return [
            (job.after_seq, job.before_seq, job.deliver)
            for job in session.scalars(select(ImportJob).order_by(ImportJob.id))
        ]


def test_a_refused_cursor_resumes_at_the_hosts_lookback_floor(settings, monkeypatch):
    """The whole point of reading the floor out of the refusal: those events
    are still on the free live socket, and the archive is metered."""
    set_settings(replace(settings, jetstream_api_key="k"))
    jetstream.save_cursor(STALE)

    urls = _host(monkeypatch, ["refuse", "event"])

    assert _query(urls[0])["cursor"] == [str(STALE)]
    assert _query(urls[1])["cursor"] == [str(FLOOR)]
    # Flushed on the first event rather than at _CURSOR_FLUSH_EVERY: until the
    # new position is stored, a reconnect would be refused all over again.
    assert jetstream.load_cursor() == RESUMED_AT


def test_the_skipped_range_is_queued_for_the_archive(settings, monkeypatch):
    """What the live socket could not serve is exactly what the import covers,
    and it must not fan a day of history out to peers."""
    set_settings(replace(settings, jetstream_api_key="k"))
    jetstream.save_cursor(STALE)

    _host(monkeypatch, ["refuse", "event"])

    assert _queued_jobs() == [(STALE, RESUMED_AT, False)]


def test_a_floor_that_moved_again_falls_back_to_the_live_tip(settings, monkeypatch):
    """A second refusal means the floor moved between the refusal and the
    retry. Only the tip is certain not to be too old, and the gap still has to
    start where ingestion actually stopped."""
    set_settings(replace(settings, jetstream_api_key="k"))
    jetstream.save_cursor(STALE)

    urls = _host(monkeypatch, ["refuse", "refuse", "event"])

    assert "cursor" not in _query(urls[2])
    assert _queued_jobs() == [(STALE, RESUMED_AT, False)]


def test_a_refusal_for_any_other_reason_leaves_the_cursor_alone(settings, monkeypatch):
    """Only a cursor the host names as too old may move it. Anything else is
    an ordinary failure, and moving the cursor would lose records."""
    jetstream.save_cursor(STALE)

    urls = _host(monkeypatch, ["refuse"], body=_OTHER_400)

    assert _query(urls[0])["cursor"] == [str(STALE)]
    assert jetstream.load_cursor() == STALE
    assert _queued_jobs() == []


def test_a_recovery_cut_short_leaves_the_gap_recoverable(settings, monkeypatch):
    """The resume point stays in memory until an event proves it good. Storing
    it first would let a restart in between resume from it with no record of
    where ingestion really stopped, and the skipped range would never be
    imported. Untouched, the next attempt just repeats the recovery."""
    set_settings(replace(settings, jetstream_api_key="k"))
    jetstream.save_cursor(STALE)

    urls = _host(monkeypatch, ["refuse", "drop"])

    assert _query(urls[1])["cursor"] == [str(FLOOR)]
    assert jetstream.load_cursor() == STALE
    assert _queued_jobs() == []


def test_a_drop_mid_recovery_does_not_queue_the_gap_twice(settings, monkeypatch):
    """The refusal repeats when the resumed stream dies before any event, so
    the gap must survive that round without being reported for each attempt:
    one interruption, one import, still starting where ingestion stopped."""
    set_settings(replace(settings, jetstream_api_key="k"))
    jetstream.save_cursor(STALE)

    _host(monkeypatch, ["refuse", "drop", "refuse", "event"])

    assert _queued_jobs() == [(STALE, RESUMED_AT, False)]
