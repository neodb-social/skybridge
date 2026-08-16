"""Jetstream v2 is the internal vocabulary; v1 is an adapter onto it."""

from __future__ import annotations

import asyncio
import json

from skybridge.atproto import events
from skybridge.config import Settings, set_settings
from skybridge.db import init_db
from skybridge.pipeline import process_event
from skybridge.translate import neodb

from tests.conftest import FIXTURES

V2_SAMPLE = FIXTURES / "jetstream_v2_sample.jsonl"


def _v2_frames() -> list[dict]:
    return [json.loads(line) for line in V2_SAMPLE.read_text().splitlines() if line.strip()]


def _to_v2(v1: dict) -> dict:
    """Re-encode a v1 event as the v2 envelope carrying the same content."""
    kind = v1.get("kind")
    payload = {
        "$type": f"network.bsky.jetstream.subscribeEvents#{kind}",
        "did": v1["did"],
        "seq": v1["time_us"] // 1000,
        "time": events.micros_to_iso(v1["time_us"]),
    }
    if kind == "commit":
        payload.update(v1["commit"])
    else:
        payload[kind] = v1.get(kind, {})
    return {"$type": "message", "payload": payload}


def test_v2_commits_normalize_to_flat_fields():
    """No nesting, no unit conversion: v2's own shape is what we keep."""
    frames = [f for f in _v2_frames() if f["payload"]["$type"].endswith("#commit")]
    assert frames, "fixture must contain at least one real v2 commit"
    for frame in frames:
        payload = frame["payload"]
        norm = events.normalize(frame)
        assert norm is not None
        assert norm["kind"] == "commit"
        assert norm["did"] == payload["did"]
        assert norm["seq"] == payload["seq"]
        # ISO-8601 straight through, not re-derived through epoch integers.
        assert norm["time"] == payload["time"]
        assert norm["collection"] == payload["collection"]
        assert norm["rkey"] == payload["rkey"]
        assert norm["operation"] == payload["operation"]
        assert norm["record"] == payload["record"]
        assert "commit" not in norm, "the v1 nesting must be gone"


def test_v1_commits_are_converted_up():
    """v1 is adapted onto v2's shape, so nothing downstream sees two dialects."""
    v1 = {
        "kind": "commit",
        "did": "did:plc:x",
        "time_us": 1_731_900_001_000_000,
        "commit": {
            "collection": "social.popfeed.feed.review",
            "rkey": "r1",
            "operation": "create",
            "rev": "rev1",
            "record": {"text": "hi"},
        },
    }
    norm = events.normalize(v1)
    assert norm is not None
    assert norm["collection"] == "social.popfeed.feed.review"
    assert norm["rkey"] == "r1"
    assert norm["operation"] == "create"
    assert norm["record"] == {"text": "hi"}
    assert norm["rev"] == "rev1"
    assert "commit" not in norm
    # time_us becomes ISO; seq is unavailable on v1 and must not be invented.
    assert norm["time"] == events.micros_to_iso(1_731_900_001_000_000)
    assert norm["seq"] is None


def test_v1_identity_and_account_are_converted_up():
    for kind in ("identity", "account"):
        norm = events.normalize(
            {"kind": kind, "did": "did:plc:x", "time_us": 1, kind: {"did": "did:plc:x"}}
        )
        assert norm is not None
        assert norm["kind"] == kind
        assert norm[kind] == {"did": "did:plc:x"}
        assert norm["seq"] is None


def test_v2_identity_and_account_keep_their_payloads():
    for frame in _v2_frames():
        kind = frame["payload"]["$type"].rsplit("#", 1)[-1]
        if kind not in ("identity", "account"):
            continue
        norm = events.normalize(frame)
        assert norm is not None
        assert norm["kind"] == kind
        assert norm[kind] == frame["payload"][kind]


def test_already_normalized_events_pass_through():
    """The archive decoder emits the internal shape directly."""
    flat = {"kind": "commit", "did": "did:plc:x", "seq": 5, "time": None, "collection": "c"}
    assert events.normalize(flat) is flat


def test_sync_events_are_dropped():
    frame = {
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#sync",
            "did": "did:plc:x",
            "seq": 5,
        },
    }
    assert events.normalize(frame) is None


def test_unknown_kinds_are_dropped_not_misread_as_commits():
    frame = {
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#somethingNew",
            "did": "did:plc:x",
        },
    }
    assert events.normalize(frame) is None
    assert events.normalize({"kind": "somethingNew", "did": "did:plc:x", "time_us": 1}) is None


def test_malformed_envelopes_are_dropped():
    assert events.normalize({"$type": "message"}) is None
    assert events.normalize({"$type": "message", "payload": "nope"}) is None
    assert events.normalize({"$type": "message", "payload": {"$type": "other#commit"}}) is None


def test_unparseable_event_time_falls_back_instead_of_raising():
    """`time` now reaches translate as a string, so it validates there."""
    published = neodb._published({}, "not-a-timestamp")
    assert published, "must fall back to now() rather than raise"
    # A good value is preserved to the microsecond.
    assert neodb._published({}, "2026-08-13T06:47:43.959305Z").startswith(
        "2026-08-13T06:47:43.959305"
    )


def test_record_createdAt_still_wins_over_the_firehose_time():
    assert neodb._published({"createdAt": "2020-01-01T00:00:00Z"}, "2026-08-13T06:47:43Z") == (
        "2020-01-01T00:00:00Z"
    )


def test_both_dialects_produce_identical_results(settings: Settings, fixture_path):
    """The same records, delivered as v1 or v2, must bridge identically."""
    v1_events = [json.loads(line) for line in fixture_path.read_text().splitlines() if line.strip()]

    async def run(stream: list[dict]) -> list[tuple]:
        set_settings(settings)
        init_db(reset=True)
        out = []
        for event in stream:
            result = await process_event(event, allow_network=False)
            if result is not None:
                out.append((result.at_uri, result.operation, result.collection))
        return out

    from_v1 = asyncio.run(run(v1_events))
    from_v2 = asyncio.run(run([_to_v2(e) for e in v1_events]))
    assert from_v1 == from_v2
    assert from_v1, "fixture should bridge at least one record"
