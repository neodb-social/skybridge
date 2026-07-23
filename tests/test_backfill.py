"""Backfill: PDS fetch (pagination + write-time window) and the import guard."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from skybridge import optout
from skybridge.atproto import backfill, identity
from skybridge.db import session_scope
from skybridge.models import BridgedActor
from skybridge.pipeline import Processed

DID = "did:plc:backfilltestuser00000000"
PDS = "https://pds.test"

REVIEWS = "social.popfeed.feed.review"
ITEMS = "social.popfeed.feed.listItem"
LISTS = "social.popfeed.feed.list"
BOOKS = "buzz.bookhive.book"


def _iso(days_ago: float) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).isoformat()


def _tid(days_ago: float) -> str:
    """Encode a TID rkey whose write time lies ``days_ago`` in the past."""
    micros = int((datetime.now(UTC) - timedelta(days=days_ago)).timestamp() * 1_000_000)
    value = micros << 10
    chars = []
    for _ in range(13):
        chars.append(backfill._TID_CHARS[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def _rec(collection: str, rkey: str, value: dict[str, Any]) -> dict[str, Any]:
    return {"uri": f"at://{DID}/{collection}/{rkey}", "cid": f"cid-{rkey}", "value": value}


@pytest.fixture
def pds(settings, monkeypatch) -> dict[str, Any]:
    """Offline PDS: canned newest-first records per collection, served through
    a paginating ``listRecords`` stub (cursor = plain offset). Set the
    ``"profile"`` key to serve a popfeed profile record via ``getRecord``."""
    world: dict[str, Any] = {"calls": []}

    def http(url: str, timeout: float = 8.0) -> dict | None:
        world["calls"].append(url)
        if url == f"{identity.PLC_DIRECTORY}/{DID}":
            return {
                "alsoKnownAs": ["at://author.test"],
                "service": [{"id": "#atproto_pds", "serviceEndpoint": PDS}],
            }
        if url.startswith(f"{PDS}/xrpc/com.atproto.repo.listRecords"):
            q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
            records = world.get(q["collection"], [])
            offset = int(q.get("cursor", "0"))
            limit = int(q.get("limit", "50"))
            page = records[offset : offset + limit]
            out: dict[str, Any] = {"records": page}
            if offset + limit < len(records):
                out["cursor"] = str(offset + limit)
            return out
        if url.startswith(f"{PDS}/xrpc/com.atproto.repo.getRecord") and "profile" in url:
            profile = world.get("profile")
            return {"value": profile} if profile else None
        return None

    monkeypatch.setattr(identity, "_http_json", http)
    return world


@pytest.fixture
def replayed(monkeypatch) -> list[dict[str, Any]]:
    """Capture the synthetic commit events instead of running the pipeline."""
    events: list[dict[str, Any]] = []

    async def fake_process(event, *, worker=None, allow_network=True):
        events.append(event)
        return Processed(event["did"], "create", event["commit"]["collection"], {})

    monkeypatch.setattr(backfill, "process_event", fake_process)
    return events


def _list_calls(world: dict[str, Any], collection: str) -> list[str]:
    return [u for u in world["calls"] if f"collection={collection}" in u]


# --- TID decoding -----------------------------------------------------------


def test_tid_datetime_roundtrip_and_rejects_garbage(settings):
    when = backfill._tid_datetime(_tid(3))
    expected = datetime.now(UTC) - timedelta(days=3)
    assert when is not None and abs((when - expected).total_seconds()) < 5
    assert backfill._tid_datetime("3m5udqziio22w") is not None  # real popfeed rkey
    assert backfill._tid_datetime("r-new") is None  # wrong length
    assert backfill._tid_datetime("self") is None
    assert backfill._tid_datetime("") is None
    assert backfill._tid_datetime("1" * 13) is None  # '1' not in the charset
    # 13 charset chars with the top bit set would decode to year 2645 and
    # bypass any age window — must be rejected, not treated as a write time.
    assert backfill._tid_datetime("mycustomthing") is None


# --- window + ordering ------------------------------------------------------


def test_window_filters_and_replays_oldest_first(settings, pds, replayed):
    # Non-TID rkeys: the window falls back to the content timestamp.
    pds[REVIEWS] = [
        _rec(REVIEWS, "r-new", {"createdAt": _iso(1)}),
        _rec(REVIEWS, "r-mid", {"createdAt": _iso(3)}),
        _rec(REVIEWS, "r-old", {"createdAt": _iso(30)}),
        _rec(REVIEWS, "r-none", {"createdAt": {}}),  # unknown age: dropped
    ]
    pds[ITEMS] = [_rec(ITEMS, "i-mid", {"addedAt": _iso(2)})]

    since = datetime.now(UTC) - timedelta(days=7)
    results = asyncio.run(backfill.backfill_did(DID, since=since))

    assert len(results) == 3  # r-old (too old) and r-none (unknown age) dropped
    assert [e["commit"]["rkey"] for e in replayed] == ["r-mid", "i-mid", "r-new"]
    assert all(e["commit"]["operation"] == "create" for e in replayed)


def test_window_uses_write_time_for_timestampless_records(settings, pds, replayed):
    # popfeed's empty-object createdAt: the TID rkey (write time) governs.
    recent, old = _tid(1), _tid(30)
    pds[REVIEWS] = [_rec(REVIEWS, recent, {"createdAt": {}}), _rec(REVIEWS, old, {"createdAt": {}})]
    since = datetime.now(UTC) - timedelta(days=7)
    results = asyncio.run(backfill.backfill_did(DID, since=since))
    assert len(results) == 1
    assert [e["commit"]["rkey"] for e in replayed] == [recent]


def test_backdated_created_at_kept_when_recently_written(settings, pds, replayed):
    # A watch logged yesterday but dated last month was on the firehose
    # yesterday — write time wins over the user-authored content date.
    rkey = _tid(1)
    pds[REVIEWS] = [_rec(REVIEWS, rkey, {"createdAt": _iso(30)})]
    since = datetime.now(UTC) - timedelta(days=7)
    asyncio.run(backfill.backfill_did(DID, since=since))
    assert [e["commit"]["rkey"] for e in replayed] == [rkey]


def test_without_window_everything_fetched_replays(settings, pds, replayed):
    pds[REVIEWS] = [
        _rec(REVIEWS, "r-new", {"createdAt": _iso(1)}),
        _rec(REVIEWS, "r-old", {"createdAt": _iso(300)}),
        _rec(REVIEWS, "r-none", {}),  # no timestamp: kept in full mode
    ]
    results = asyncio.run(backfill.backfill_did(DID))
    assert len(results) == 3
    # the timestamp-less record sorts as oldest (epoch), then chronological
    assert [e["commit"]["rkey"] for e in replayed] == ["r-none", "r-old", "r-new"]


# --- budget + pagination ----------------------------------------------------


def test_limit_budgets_reviews_before_archive_only_lists(settings, pds, replayed):
    pds[LISTS] = [_rec(LISTS, f"l{i}", {"createdAt": _iso(i + 1)}) for i in range(3)]
    pds[REVIEWS] = [_rec(REVIEWS, f"r{i}", {"createdAt": _iso(i + 1)}) for i in range(3)]
    results = asyncio.run(backfill.backfill_did(DID, limit=4))
    # The shared cap feeds AP-emitting collections first: all 3 reviews make
    # it, archive-only lists only get the leftover slot.
    assert len(results) == 4
    assert sum(e["commit"]["collection"] == REVIEWS for e in replayed) == 3
    assert sum(e["commit"]["collection"] == LISTS for e in replayed) == 1


def test_limit_budgets_bookhive_before_archive_only_lists(settings, pds, replayed):
    # BookHive books emit AP activity, so they must be fetched before
    # archive-only popfeed lists under the shared cap (see _FETCH_PRIORITY).
    pds[LISTS] = [_rec(LISTS, f"l{i}", {"createdAt": _iso(i + 1)}) for i in range(3)]
    pds[BOOKS] = [_rec(BOOKS, f"b{i}", {"createdAt": _iso(i + 1)}) for i in range(3)]
    results = asyncio.run(backfill.backfill_did(DID, limit=4))
    assert len(results) == 4
    assert sum(e["commit"]["collection"] == BOOKS for e in replayed) == 3
    assert sum(e["commit"]["collection"] == LISTS for e in replayed) == 1


def test_pagination_follows_cursor(settings, pds, replayed):
    pds[REVIEWS] = [_rec(REVIEWS, f"r{i}", {"createdAt": _iso(1)}) for i in range(250)]
    results = asyncio.run(backfill.backfill_did(DID, limit=1000))
    assert len(results) == 250
    assert len(_list_calls(pds, REVIEWS)) == 3  # pages of 100 + 100 + 50


def test_pagination_stops_at_limit(settings, pds, replayed):
    pds[REVIEWS] = [_rec(REVIEWS, f"r{i}", {"createdAt": _iso(1)}) for i in range(250)]
    results = asyncio.run(backfill.backfill_did(DID, limit=150))
    assert len(results) == 150
    assert len(_list_calls(pds, REVIEWS)) == 2  # 100 + 50, third page never asked


def test_pagination_stops_once_pages_predate_window(settings, pds, replayed):
    # 300 records whose TID rkeys all predate the window: page 1's oldest
    # rkey suffices to prove later pages are older still.
    pds[REVIEWS] = [_rec(REVIEWS, _tid(100 + i), {"createdAt": {}}) for i in range(300)]
    since = datetime.now(UTC) - timedelta(days=7)
    results = asyncio.run(backfill.backfill_did(DID, since=since))
    assert results == []
    assert len(_list_calls(pds, REVIEWS)) == 1


def test_backdated_timestamps_do_not_stop_pagination(settings, pds, replayed):
    # Page 1: recently WRITTEN records carrying old content dates; page 2
    # holds an in-window record. The early stop must key on rkey TIDs, so
    # page 2 is still fetched.
    page1 = [_rec(REVIEWS, _tid(0.1 + i * 0.001), {"createdAt": _iso(60 + i)}) for i in range(100)]
    wanted = _rec(REVIEWS, _tid(2), {"createdAt": _iso(2)})
    pds[REVIEWS] = [*page1, wanted]
    since = datetime.now(UTC) - timedelta(days=7)
    asyncio.run(backfill.backfill_did(DID, since=since))
    assert len(_list_calls(pds, REVIEWS)) == 2
    # backdated-but-recent writes are in-window too (firehose parity)
    assert len(replayed) == 101
    assert wanted["uri"].endswith(replayed[0]["commit"]["rkey"])  # oldest write first


def test_stale_records_do_not_consume_the_budget(settings, pds, replayed):
    # 120 reviews all far older than the window must not starve in-window
    # shelf items: only records that survive the window count against limit.
    pds[REVIEWS] = [_rec(REVIEWS, _tid(100 + i), {"createdAt": {}}) for i in range(120)]
    pds[ITEMS] = [_rec(ITEMS, _tid(2), {"addedAt": _iso(2)})]
    since = datetime.now(UTC) - timedelta(days=7)
    results = asyncio.run(backfill.backfill_did(DID, limit=100, since=since))
    assert len(results) == 1
    assert [e["commit"]["collection"] for e in replayed] == [ITEMS]


def test_lists_are_unwindowed_and_replay_first(settings, pds, replayed):
    # feed.list is archive-only: exempt from the window (labels for items
    # added to old lists) and replayed before items, so listItem translation
    # finds the list archived locally instead of fetching mid-replay.
    pds[LISTS] = [_rec(LISTS, _tid(300), {"name": "old shelf", "createdAt": _iso(300)})]
    pds[ITEMS] = [_rec(ITEMS, _tid(1), {"addedAt": _iso(1)})]
    since = datetime.now(UTC) - timedelta(days=7)
    results = asyncio.run(backfill.backfill_did(DID, since=since))
    assert len(results) == 2
    assert [e["commit"]["collection"] for e in replayed] == [LISTS, ITEMS]


def test_limit_defaults_to_settings(settings, pds, replayed):
    from dataclasses import replace

    from skybridge.config import set_settings

    set_settings(replace(settings, backfill_limit=2))
    pds[REVIEWS] = [_rec(REVIEWS, f"r{i}", {"createdAt": _iso(i + 1)}) for i in range(5)]
    results = asyncio.run(backfill.backfill_did(DID))
    assert len(results) == 2  # limit=None fell back to SKYBRIDGE_BACKFILL_LIMIT


def test_empty_repo_mints_no_actor(settings, pds, replayed):
    results = asyncio.run(backfill.backfill_did(DID))
    assert results == []
    assert replayed == []
    with session_scope() as session:
        assert session.get(BridgedActor, DID) is None  # nothing bridged, no actor row


def test_unresolvable_pds_yields_nothing(settings, monkeypatch, replayed):
    monkeypatch.setattr(identity, "_http_json", lambda url, timeout=8.0: None)
    assert asyncio.run(backfill.backfill_did(DID)) == []
    assert replayed == []


# --- profile refresh --------------------------------------------------------


def test_import_refreshes_profile(settings, pds, monkeypatch):
    pds[REVIEWS] = [_rec(REVIEWS, _tid(1), {"createdAt": _iso(1)})]
    pds["profile"] = {"displayName": "Fresh Name"}
    events: list[tuple[dict[str, Any], bool]] = []

    async def fake_process(event, *, worker=None, allow_network=True):
        events.append((event, allow_network))
        return Processed(event["did"], "create", event["commit"]["collection"], {})

    monkeypatch.setattr(backfill, "process_event", fake_process)
    results = asyncio.run(backfill.backfill_did(DID))

    profile = [
        (e, net) for e, net in events if e["commit"]["collection"] == backfill._PROFILE_COLLECTION
    ]
    assert len(profile) == 1
    event, allow_network = profile[0]
    assert event["commit"]["record"] == {"displayName": "Fresh Name"}
    assert event["commit"]["rkey"] == "self"
    # The network refresh already ran off-loop (to_thread refresh_actor);
    # the Update-emitting process_event must stay off the network.
    assert allow_network is False
    assert len(results) == 1  # the profile replay is not counted as a record
    with session_scope() as session:
        actor = session.get(BridgedActor, DID)
        assert actor is not None and actor.display_name == "Fresh Name"


# --- the per-DID import guard -----------------------------------------------


def test_start_import_one_run_per_did(settings, monkeypatch):
    release = asyncio.Event()
    calls: list[dict[str, Any]] = []

    async def fake_backfill(did, **kwargs):
        calls.append({"did": did, **kwargs})
        await release.wait()
        return []

    monkeypatch.setattr(backfill, "backfill_did", fake_backfill)

    async def go() -> None:
        assert backfill.start_import(DID) is True
        assert backfill.start_import(DID) is False  # already in progress
        assert backfill.start_import("did:plc:someoneelse0000000000000") is True  # other DIDs fine
        release.set()
        await asyncio.gather(*backfill._IMPORTS.values())
        await asyncio.sleep(0)  # let done-callbacks pop the registry
        assert backfill.start_import(DID) is True  # runnable again once done
        await asyncio.gather(*backfill._IMPORTS.values())
        await asyncio.sleep(0)

    asyncio.run(go())
    assert not backfill._IMPORTS
    # windowed to settings: limit + since derived from backfill_{limit,days}
    assert calls[0]["limit"] == settings.backfill_limit
    expected = datetime.now(UTC) - timedelta(days=settings.backfill_days)
    assert abs((calls[0]["since"] - expected).total_seconds()) < 60


def test_start_import_refused_for_opted_out(settings, monkeypatch):
    called = []

    async def fake_backfill(did, **kwargs):
        called.append(did)
        return []

    monkeypatch.setattr(backfill, "backfill_did", fake_backfill)

    asyncio.run(optout.opt_out(DID))
    # Refused before any task would be created, so no event loop is needed.
    assert backfill.start_import(DID) is False
    assert called == []
    assert not backfill._IMPORTS


def test_import_failure_clears_registry(settings, monkeypatch):
    async def boom(did, **kwargs):
        raise RuntimeError("pds exploded")

    monkeypatch.setattr(backfill, "backfill_did", boom)

    async def go() -> None:
        assert backfill.start_import(DID) is True
        await asyncio.gather(*backfill._IMPORTS.values(), return_exceptions=True)
        await asyncio.sleep(0)

    asyncio.run(go())
    assert not backfill._IMPORTS


# --- cancellation (opt-out race + shutdown) ---------------------------------


def test_cancel_import_stops_running_task(settings, monkeypatch):
    cancelled = []

    async def hang(did, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(did)
            raise

    monkeypatch.setattr(backfill, "backfill_did", hang)

    async def go() -> None:
        assert backfill.start_import(DID) is True
        await asyncio.sleep(0)  # let the import task actually start
        assert await backfill.cancel_import(DID) is True
        await asyncio.sleep(0)
        assert cancelled == [DID]
        assert DID not in backfill._IMPORTS
        assert await backfill.cancel_import(DID) is False  # nothing left

    asyncio.run(go())


def test_opt_out_cancels_running_import(settings, monkeypatch):
    cancelled = []

    async def hang(did, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(did)
            raise

    monkeypatch.setattr(backfill, "backfill_did", hang)

    async def go() -> None:
        assert backfill.start_import(DID) is True
        await asyncio.sleep(0)
        await optout.opt_out(DID)
        assert cancelled == [DID]  # the purge waited for the import to die
        assert DID not in backfill._IMPORTS

    asyncio.run(go())


def test_cancel_all_imports(settings, monkeypatch):
    other = "did:plc:someoneelse0000000000000"

    async def hang(did, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(backfill, "backfill_did", hang)

    async def go() -> None:
        assert backfill.start_import(DID) is True
        assert backfill.start_import(other) is True
        await asyncio.sleep(0)
        await backfill.cancel_all_imports()
        assert not backfill._IMPORTS

    asyncio.run(go())


def test_replay_aborts_for_opted_out_did(settings, pds, replayed):
    pds[REVIEWS] = [_rec(REVIEWS, _tid(1), {"createdAt": _iso(1)})]
    asyncio.run(optout.opt_out(DID))
    results = asyncio.run(backfill.backfill_did(DID))
    assert results == []
    assert replayed == []  # nothing re-published, profile refresh included


def test_cancel_import_propagates_callers_cancellation(settings, monkeypatch):
    # If the CALLER (e.g. an opt-out request during shutdown) is cancelled
    # while cancel_import waits on the dying import, that cancellation must
    # propagate — the suppress() is only for the import task's own demise.
    async def hang(did, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.05)  # linger so the waiter gets cancelled meanwhile
            raise

    monkeypatch.setattr(backfill, "backfill_did", hang)

    async def go() -> bool:
        assert backfill.start_import(DID) is True
        await asyncio.sleep(0)  # let the import task start
        me = asyncio.current_task()
        assert me is not None
        asyncio.get_running_loop().call_later(0.01, me.cancel)
        try:
            await backfill.cancel_import(DID)
        except asyncio.CancelledError:
            me.uncancel()
            return True
        return False

    assert asyncio.run(go()) is True


# --- CLI --------------------------------------------------------------------


def test_cli_days_zero_still_windows(settings, monkeypatch, capsys):
    captured: dict[str, Any] = {}

    async def fake_backfill(did, *, worker=None, limit=None, since=None):
        captured["since"] = since
        return []

    monkeypatch.setattr(backfill, "backfill_did", fake_backfill)
    from skybridge.__main__ import main

    assert main(["backfill", DID, "--days", "0"]) == 0
    # --days 0 must mean "the narrowest window", never "no window at all"
    assert captured["since"] is not None
    assert captured["since"] <= datetime.now(UTC)
