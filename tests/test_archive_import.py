"""Staleness guard and account lifecycle: the rules that let a historical
import run beside live ingest without corrupting current state."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from typing import Any, ClassVar, cast

import pytest
from skybridge.config import set_settings
from skybridge.db import session_scope
from skybridge.models import BridgedActor, Delivery, ImportJob, OptOut, Record, utcnow
from skybridge.pipeline import Processed, process_event

REVIEW = "social.popfeed.feed.review"
DID = "did:plc:staletest0000000000000"


def _commit(seq: int, *, rkey: str = "r1", operation: str = "create", rating: int = 4) -> dict:
    return {
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#commit",
            "did": DID,
            "seq": seq,
            "time": "2026-08-13T06:47:43.959305Z",
            "collection": REVIEW,
            "rkey": rkey,
            "operation": operation,
            "record": {
                "$type": REVIEW,
                "text": f"seq {seq}",
                "rating": rating,
                "createdAt": "2026-08-13T06:47:43.000Z",
                "creativeWorkType": "movie",
                "identifiers": {"imdb": "tt0000001"},
            },
        },
    }


def _run(event: dict, **kw):
    return asyncio.run(process_event(event, allow_network=False, **kw))


def _uri(rkey: str = "r1") -> str:
    return f"at://{DID}/{REVIEW}/{rkey}"


def _row(rkey: str = "r1") -> Record | None:
    with session_scope() as session:
        return session.get(Record, _uri(rkey))


def _stored(rkey: str = "r1") -> Record:
    """The row, asserted present — most tests go on to read its fields."""
    row = _row(rkey)
    assert row is not None, f"expected a record for {_uri(rkey)}"
    return row


def test_redelivered_event_is_ignored(settings):
    _run(_commit(100))
    assert _run(_commit(100)) is None, "an inclusive-cursor replay must be a no-op"


def test_older_event_cannot_regress_a_record(settings):
    _run(_commit(200, rating=5))
    assert _run(_commit(100, rating=1)) is None
    stored = json.loads(_stored().source_json)
    assert stored["rating"] == 5, "the newer content must survive"


def test_newer_event_still_applies(settings):
    _run(_commit(100, rating=3))
    assert _run(_commit(300, rating=5)) is not None
    assert _stored().last_seq == 300
    assert json.loads(_stored().source_json)["rating"] == 5


def test_archive_event_cannot_resurrect_a_deleted_record(settings):
    """A delete advances the mark, so an archived create from before it loses."""
    _run(_commit(100))
    _run(_commit(500, operation="delete"))
    assert _stored().deleted_at is not None
    assert _run(_commit(300), from_archive=True) is None
    assert _stored().deleted_at is not None


def test_archive_never_overwrites_a_row_the_live_path_owns(settings):
    """Rows predating v2 carry no mark; an import must not claim them."""
    _run(_commit(100))
    with session_scope() as session:
        row = session.get(Record, _uri())
        assert row is not None
        row.last_seq = None
    assert _run(_commit(400), from_archive=True) is None
    # ...while a live event for the same row is current by definition.
    assert _run(_commit(400)) is not None


def test_opted_out_dids_are_skipped_by_the_import(settings):
    with session_scope() as session:
        session.add(OptOut(did=DID))
    assert _run(_commit(100), from_archive=True) is None
    assert _row() is None


def _account(seq: int, *, active: bool, status: str | None = None) -> dict:
    payload = {"active": active, "did": DID, "seq": seq, "time": "2026-08-13T06:47:43.959Z"}
    if status:
        payload["status"] = status
    return {
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#account",
            "did": DID,
            "seq": seq,
            "time": "2026-08-13T06:47:43.959305Z",
            "account": payload,
        },
    }


def _actor() -> BridgedActor | None:
    with session_scope() as session:
        return session.get(BridgedActor, DID)


def _bridged() -> BridgedActor:
    """The bridged actor, asserted present."""
    actor = _actor()
    assert actor is not None, f"expected a bridged actor for {DID}"
    return actor


def test_account_events_for_unknown_dids_are_ignored(settings):
    """These arrive for the whole network; an unknown DID is not our business."""
    assert _run(_account(1, active=False, status="deleted")) is None
    assert _actor() is None


def test_deleted_account_is_purged(settings):
    _run(_commit(100))
    assert _row() is not None
    result = _run(_account(200, active=False, status="deleted"))
    assert result is not None and result.operation == "delete"
    assert _stored().deleted_at is not None


def test_deletion_does_not_record_an_opt_out(settings):
    """An opt-out is a standing user choice; a deleted account is just gone."""
    _run(_commit(100))
    _run(_account(200, active=False, status="deleted"))
    with session_scope() as session:
        assert session.get(OptOut, DID) is None


def test_deactivation_gates_without_retracting(settings):
    _run(_commit(100))
    assert _run(_account(200, active=False, status="deactivated")) is not None
    assert _bridged().inactive_status == "deactivated"
    assert _stored().deleted_at is None, "nothing may be retracted for a reversible status"


def test_gated_account_stops_bridging_then_resumes(settings):
    _run(_commit(100))
    _run(_account(200, active=False, status="suspended"))
    assert _run(_commit(300, rkey="r2")) is None, "gated: no new records"
    assert _run(_account(400, active=True)) is not None
    assert _bridged().inactive_status is None
    assert _run(_commit(500, rkey="r2")) is not None, "resumed after reactivation"


def test_a_commit_lifts_a_deactivation_gate_the_events_missed(settings):
    """The reactivation event can be lost for good: the archive carries no
    lifecycle rows, so a gap wider than the host's lookback window drops it
    and the author would never bridge again. A live commit says the repo is
    writing, which settles the question the missing event would have."""
    _run(_commit(100))
    _run(_account(200, active=False, status="deactivated"))

    assert _run(_commit(300, rkey="r2")) is not None, "a live commit proves it is back"
    assert _bridged().inactive_status is None


def test_a_replayed_older_commit_never_lifts_a_gate(settings):
    """Jetstream redelivers after a reconnect, and backfill_did synthesises
    commits with no seq at all. Neither says the repo is writing *now*, so
    only a commit past the gate's own seq may lift it."""
    _run(_commit(100))
    _run(_account(200, active=False, status="deactivated"))

    assert _run(_commit(100)) is None, "a redelivered pre-gate commit proves nothing"
    assert _bridged().inactive_status == "deactivated"

    synthetic = _commit(300, rkey="r3")
    del synthetic["payload"]["seq"]
    assert _run(synthetic) is None, "a backfill commit carries no seq"
    assert _bridged().inactive_status == "deactivated"


def test_an_archived_commit_never_lifts_a_gate(settings):
    """History says nothing about now, and the gap import replays plenty of
    it. Letting it reopen an account the live stream just gated would undo
    that gate from the other end."""
    _run(_commit(100))
    _run(_account(200, active=False, status="deactivated"))

    assert _run(_commit(300, rkey="r2"), from_archive=True) is None
    assert _bridged().inactive_status == "deactivated"


def test_a_commit_does_not_lift_a_moderation_gate(settings):
    """A takedown is somebody else's decision, and a commit is not evidence
    that it was reversed."""
    _run(_commit(100))
    _run(_account(200, active=False, status="takendown"))

    assert _run(_commit(300, rkey="r2")) is None
    assert _bridged().inactive_status == "takendown"


def test_takedown_is_reversible_not_a_purge(settings):
    """Per the configured policy, only `deleted` retracts."""
    _run(_commit(100))
    _run(_account(200, active=False, status="takendown"))
    assert _stored().deleted_at is None
    assert _bridged().inactive_status == "takendown"


# --- import job lifecycle --------------------------------------------------


def test_import_requires_an_api_key(settings):
    """The live socket needs no key; the metered HTTP archive does."""
    from skybridge.atproto import archive

    with pytest.raises(archive.ArchiveError, match="SKYBRIDGE_JETSTREAM_API_KEY"):
        archive._client()


def test_import_requires_a_v2_endpoint(settings):
    from skybridge.atproto import archive

    set_settings(
        replace(
            settings,
            jetstream_api_key="gk_test",
            jetstream_url="wss://jetstream2.us-east.bsky.network/subscribe",
        )
    )
    with pytest.raises(archive.ArchiveError, match="v2"):
        archive._client()


def test_job_is_bounded_by_the_live_cursor(settings):
    """The upper bound is what keeps an import off the live tail's territory."""
    from skybridge.atproto import archive, jetstream

    jetstream.save_cursor(24_762_496_440)
    job_id = archive.create_job(after_seq=0)
    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        assert job is not None
        assert job.before_seq == 24_762_496_440


def test_queued_jobs_are_claimable_and_paused_ones_resume(settings):
    from skybridge.atproto import archive

    job_id = archive.create_job(after_seq=0, before_seq=100)
    assert archive._claimable_job() == job_id
    with session_scope() as session:
        paused = session.get(ImportJob, job_id)
        assert paused is not None
        paused.state = "paused"
    assert archive._claimable_job() == job_id, "a restart must resume an unfinished job"
    with session_scope() as session:
        done = session.get(ImportJob, job_id)
        assert done is not None
        done.state = "done"
    assert archive._claimable_job() is None


def test_opt_out_leaves_a_non_delivering_import_alone(settings):
    """It federates nothing, and its per-event opt-out check already stops it."""
    from skybridge.atproto import archive

    archive.create_job(after_seq=0, before_seq=100, deliver=False)
    assert asyncio.run(archive.cancel_if_delivering()) is False


def test_archive_endpoints_use_their_own_parameter_names(settings, monkeypatch):
    """getBlock takes `segment`, getSegment takes `name` — they differ, and a
    wrong name is a 400 that only shows up on a segment-mode plan entry."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    seen: list[tuple[str, dict]] = []

    class _Response:
        status_code = 200
        headers: ClassVar[dict] = {}
        content = b""

        def raise_for_status(self):
            return None

        async def aiter_bytes(self, _n):
            if False:
                yield b""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None, **kw):
            seen.append((url, params or {}))
            return _Response()

        def stream(self, _method, url, params=None, **kw):
            seen.append((url, params or {}))
            return _Response()

    monkeypatch.setattr(archive, "_client", lambda: _Client())

    async def go():
        async with archive._client() as c:
            await archive._fetch_block(c, "seg_0.jss", 7)
            await archive._fetch_segment(c, "seg_0.jss")

    asyncio.run(go())
    block_params = dict(seen[0][1])
    segment_params = dict(seen[1][1])
    assert block_params == {"segment": "seg_0.jss", "blockIndex": 7}
    assert segment_params == {"name": "seg_0.jss"}


def test_a_status_error_mid_segment_does_not_retry_forever(settings, monkeypatch):
    """Only a dropped connection is resumable; a 4xx must propagate."""
    import httpx
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    calls = 0

    class _Response:
        status_code = 400
        headers: ClassVar[dict] = {}

        def raise_for_status(self):
            request = httpx.Request("GET", "https://example.test/seg")
            raise httpx.HTTPStatusError(
                "bad", request=request, response=httpx.Response(400, request=request)
            )

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Client:
        def stream(self, *a, **kw):
            nonlocal calls
            calls += 1
            return _Response()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(archive._fetch_segment(cast(httpx.AsyncClient, _Client()), "seg_0.jss"))
    assert calls == 1, "a status error must not be retried"


def test_a_transport_blip_is_retried_not_fatal(settings, monkeypatch):
    """One DNS hiccup aborted a real popfeed run at 79%; it must not."""
    import httpx
    from skybridge.atproto import archive

    monkeypatch.setattr(archive, "_RETRY_BACKOFF_BASE", 0)
    calls = 0

    async def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ConnectError("nodename nor servname provided")
        return httpx.Response(
            200, request=httpx.Request("GET", "https://example.test/x"), content=b"ok"
        )

    result = asyncio.run(archive._send(flaky, what="test"))
    assert result.content == b"ok"
    assert calls == 3, "should have retried past the two failures"


def test_transport_retries_are_bounded(settings, monkeypatch):
    import httpx
    from skybridge.atproto import archive

    monkeypatch.setattr(archive, "_RETRY_BACKOFF_BASE", 0)

    async def always_fails():
        raise httpx.ConnectError("down")

    with pytest.raises(httpx.ConnectError):
        asyncio.run(archive._send(always_fails, what="test"))


def test_status_errors_are_not_retried(settings, monkeypatch):
    """A 400 would repeat identically, so retrying only wastes the quota."""
    import httpx
    from skybridge.atproto import archive

    monkeypatch.setattr(archive, "_RETRY_BACKOFF_BASE", 0)
    calls = 0

    async def bad_request():
        nonlocal calls
        calls += 1
        return httpx.Response(
            400, request=httpx.Request("GET", "https://example.test/x"), content=b"nope"
        )

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(archive._send(bad_request, what="test"))
    assert calls == 1


def test_resume_fast_forwards_past_applied_segments(settings, monkeypatch):
    """planned_through_seq alone is not a resume: one plan page can hold the
    whole archive, which is how a real run lost 1288 segments of progress."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    plan = {
        "sealedTipSeq": 100,
        "plannedThroughSeq": 100,
        "segments": [{"name": f"seg_{i}.jss", "mode": "blocks", "blocks": []} for i in range(5)],
    }
    applied: list[str] = []

    async def fake_plan(_client, *, after_seq, before_seq):
        return plan

    async def fake_apply(_client, segment, _job_id, *, worker, deliver):
        applied.append(segment["name"])
        return 0

    class _NullClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(archive, "_client", lambda: _NullClient())
    monkeypatch.setattr(archive, "_plan_page", fake_plan)
    monkeypatch.setattr(archive, "_apply_segment", fake_apply)

    job_id = archive.create_job(after_seq=0, before_seq=100)
    with session_scope() as s:
        job = s.get(ImportJob, job_id)
        assert job is not None
        job.last_segment = "seg_2.jss"  # first three already done

    asyncio.run(archive.run_import(job_id))
    assert applied == ["seg_3.jss", "seg_4.jss"], "must not redo applied segments"

    with session_scope() as s:
        job = s.get(ImportJob, job_id)
        assert job is not None
        assert job.state == "done"
        assert job.segments_done == 5, "progress counts the skipped ones"
        assert job.last_segment is None, "marker cleared once the page completes"


def test_resume_restarts_the_page_if_the_plan_changed(settings, monkeypatch):
    """Compaction rewrites segments, so an unknown marker must not skip data."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    applied: list[str] = []

    async def fake_plan(_client, *, after_seq, before_seq):
        return {
            "sealedTipSeq": 100,
            "plannedThroughSeq": 100,
            "segments": [{"name": "seg_a.jss", "mode": "blocks", "blocks": []}],
        }

    async def fake_apply(_client, segment, _job_id, *, worker, deliver):
        applied.append(segment["name"])
        return 0

    class _NullClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(archive, "_client", lambda: _NullClient())
    monkeypatch.setattr(archive, "_plan_page", fake_plan)
    monkeypatch.setattr(archive, "_apply_segment", fake_apply)

    job_id = archive.create_job(after_seq=0, before_seq=100)
    with session_scope() as s:
        job = s.get(ImportJob, job_id)
        assert job is not None
        job.last_segment = "seg_vanished.jss"

    asyncio.run(archive.run_import(job_id))
    assert applied == ["seg_a.jss"], "re-apply is safe; skipping would lose data"


def test_archive_filter_reads_the_internal_event_shape(settings):
    """Guards the seam between the decoder and the collection filter: when the
    internal shape went flat this silently matched nothing and imported zero."""
    from skybridge.atproto import archive

    wanted = settings.wanted_collections[0]
    assert archive._wanted({"kind": "commit", "collection": wanted, "rkey": "r"})
    assert not archive._wanted({"kind": "commit", "collection": "app.bsky.feed.like"})
    assert not archive._wanted({"kind": "commit"})
    # The pre-refactor nesting must not be silently accepted either.
    assert not archive._wanted({"kind": "commit", "commit": {"collection": wanted}})


def test_segment_mode_actually_fetches_its_index_selected_blocks(settings, monkeypatch):
    """A segment-mode entry resolves blocks via the collection index and must
    then FETCH them. This loop once sat inside the blocks-mode branch, so every
    whole-file entry silently contributed zero records to a full import."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    fetched: list[int] = []

    async def fake_blocks_from_index(_client, _name):
        return [7, 9], 7423

    async def fake_fetch_block(_client, _name, index):
        fetched.append(index)
        return b"frame"

    def fake_decode(_frame):
        return [{"kind": "commit", "collection": settings.wanted_collections[0], "rkey": "r"}]

    applied: list[dict] = []

    async def fake_apply(events, *, worker, deliver):
        applied.extend(events)
        return len(events)

    monkeypatch.setattr(archive, "_blocks_from_index", fake_blocks_from_index)
    monkeypatch.setattr(archive, "_fetch_block", fake_fetch_block)
    monkeypatch.setattr(archive.jss, "decode_block", fake_decode)
    monkeypatch.setattr(archive, "_apply", fake_apply)

    job_id = archive.create_job(after_seq=0, before_seq=100)
    count = asyncio.run(
        archive._apply_segment(
            cast(Any, None),
            {"name": "seg_0.jss", "mode": "segment"},
            job_id,
            worker=None,
            deliver=False,
        )
    )
    assert fetched == [7, 9], "index-selected blocks must be downloaded"
    assert count == 2 and len(applied) == 2


def test_segment_mode_falls_back_to_whole_file_when_the_index_is_unreadable(settings, monkeypatch):
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    whole_downloads: list[str] = []

    async def no_index(_client, _name):
        return None, 0

    async def fake_fetch_segment(_client, name):
        whole_downloads.append(name)
        return b"data"

    async def fake_apply(events, *, worker, deliver):
        return 0

    monkeypatch.setattr(archive, "_blocks_from_index", no_index)
    monkeypatch.setattr(archive, "_fetch_segment", fake_fetch_segment)
    monkeypatch.setattr(archive.jss, "iter_segment", lambda _d: iter(()))
    monkeypatch.setattr(archive, "_apply", fake_apply)

    job_id = archive.create_job(after_seq=0, before_seq=100)
    asyncio.run(
        archive._apply_segment(
            cast(Any, None),
            {"name": "seg_0.jss", "mode": "segment"},
            job_id,
            worker=None,
            deliver=False,
        )
    )
    assert whole_downloads == ["seg_0.jss"], "must not silently import nothing"


def test_an_explicit_cancel_is_not_downgraded_to_paused(settings, monkeypatch):
    """cancel() marks the job then cancels the task; the CancelledError handler
    must not overwrite that with `paused`, or watch_jobs restarts it."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))

    async def hang(_client, **_kw):
        await asyncio.sleep(3600)

    class _NullClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(archive, "_client", lambda: _NullClient())
    monkeypatch.setattr(archive, "_plan_page", hang)

    async def go():
        job_id = archive.create_job(after_seq=0, before_seq=100)
        archive.start(job_id)
        await asyncio.sleep(0)
        await archive.cancel()
        return job_id

    job_id = asyncio.run(go())
    with session_scope() as s:
        job = s.get(ImportJob, job_id)
        assert job is not None
        assert job.state == "cancelled", "an operator's cancel must stick"
    assert archive._claimable_job() != job_id, "a cancelled job must not be reclaimed"


def _mark_running(job_id: int, *, heartbeat_age: timedelta = timedelta(0)) -> None:
    with session_scope() as session:
        job = session.get(ImportJob, job_id)
        assert job is not None
        job.state = "running"
        job.updated_at = utcnow() - heartbeat_age


def test_a_hard_stop_leaves_the_job_reclaimable(settings):
    """A SIGKILL leaves state=running with a heartbeat that stops advancing."""
    from skybridge.atproto import archive

    job_id = archive.create_job(after_seq=0, before_seq=100)
    _mark_running(job_id, heartbeat_age=timedelta(hours=1))
    assert archive._claimable_job() is None, "a running job is not claimable as-is"
    assert archive.reclaim_orphaned_jobs() == 1
    assert archive._claimable_job() == job_id


def test_reclaim_leaves_a_live_runner_alone(settings):
    """`import --run` works a job from another process; stealing it would mean
    two runners downloading the same metered blocks."""
    from skybridge.atproto import archive

    job_id = archive.create_job(after_seq=0, before_seq=100)
    _mark_running(job_id)  # heartbeat is fresh
    assert archive.reclaim_orphaned_jobs() == 0
    with session_scope() as s:
        job = s.get(ImportJob, job_id)
        assert job is not None and job.state == "running"


def test_only_one_runner_can_claim_a_job(settings):
    """The CLI and the server's watcher can reach for the same row at once."""
    from skybridge.atproto import archive

    job_id = archive.create_job(after_seq=0, before_seq=100)
    assert archive._claim_job(job_id) is True
    assert archive._claim_job(job_id) is False, "a second runner must lose"


def test_run_import_yields_to_whoever_owns_the_job(settings, monkeypatch):
    """Losing the claim must be a clean no-op, not duplicated work."""
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    planned = False

    async def should_not_run(*a, **kw):
        nonlocal planned
        planned = True
        return {}

    monkeypatch.setattr(archive, "_plan_page", should_not_run)

    job_id = archive.create_job(after_seq=0, before_seq=100)
    _mark_running(job_id)  # another process already owns it
    assert asyncio.run(archive.run_import(job_id)) == 0
    assert not planned, "must not touch the archive for a job it does not own"


def test_run_import_still_raises_for_a_missing_job(settings):
    from skybridge.atproto import archive

    with pytest.raises(archive.ArchiveError, match="not found"):
        asyncio.run(archive.run_import(9999))


def test_cancel_if_delivering_survives_two_running_rows(settings):
    """A stranded row plus a live one must not raise inside opt_out."""
    from skybridge.atproto import archive

    for _ in range(2):
        job_id = archive.create_job(after_seq=0, before_seq=100)
        with session_scope() as s:
            job = s.get(ImportJob, job_id)
            assert job is not None
            job.state = "running"
    assert asyncio.run(archive.cancel_if_delivering()) is False


def test_done_callback_does_not_unregister_a_newer_run(settings, monkeypatch):
    """The callback arrives via call_soon, so a newer task may already be
    installed; clearing blindly would let a second import start alongside it."""
    from skybridge.atproto import archive

    async def go():
        async def noop() -> None:
            return None

        finished = asyncio.create_task(noop())
        await finished
        newer = asyncio.create_task(asyncio.sleep(3600))
        archive._TASK = newer
        archive._clear_task(finished)  # the stale callback fires late
        assert archive.is_running(), "a live import must stay registered"
        newer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await newer

    asyncio.run(go())
    archive._TASK = None


def test_the_cdn_redirect_is_followed_without_handing_over_the_key(settings, monkeypatch):
    """getBlock and getSegment answer 307 to a signed CDN URL. Not following
    it turned every block fetch into a fatal status error; following it must
    not carry the API key off the origin, since the CDN authenticates the
    token in the URL and has no business seeing the key."""
    import httpx
    from skybridge.atproto import archive

    set_settings(replace(settings, jetstream_api_key="gk_test"))
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "jetstream.us-east.bsky.network":
            return httpx.Response(
                307,
                headers={"location": "https://cdn.test/xrpc/getBlock?token=HS256-t&expires=1"},
            )
        return httpx.Response(200, content=b"block bytes")

    async def go() -> bytes:
        client = archive._client()
        # The redirect handling under test is httpx's own, so the client has
        # to be the real one _client() builds; only its transport is faked.
        client._transport = httpx.MockTransport(handle)
        async with client:
            return await archive._fetch_block(client, "seg_00000005uw.jss", 236)

    assert asyncio.run(go()) == b"block bytes"
    assert [r.url.host for r in seen] == ["jetstream.us-east.bsky.network", "cdn.test"]
    assert "authorization" in seen[0].headers
    assert "authorization" not in seen[1].headers


def test_a_non_delivering_import_still_retracts_what_peers_hold(settings, monkeypatch):
    """Silence is right for history, but a delete is not history arriving: it
    is an author withdrawing something peers already have. Staying silent
    would tombstone it here and leave it standing everywhere else, with no
    second chance — the replay advances last_seq, so the same event with
    delivery on is dropped as stale."""
    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.atproto import archive

    with session_scope() as db:
        db.add(
            Delivery(
                record_uri=_uri("r1"),
                target_inbox="https://peer.test/inbox",
                activity_type="Create",
                status="sent",
            )
        )

    seen: list[tuple[str, bool]] = []
    fanned: list[tuple[str, str]] = []

    async def fake_process(event, *, worker=None, from_archive=False, **kw):
        seen.append((f"{event['operation']} {event['rkey']}", worker is not None))
        kind = "Delete" if event["operation"] == "delete" else "Create"
        at_uri = f"at://{event['did']}/{event['collection']}/{event['rkey']}"
        return Processed(at_uri, event["operation"], event["collection"], {"type": kind})

    async def fake_fanout(_worker, *, record_uri, did, activity):
        fanned.append((record_uri, activity["type"]))
        return 1

    monkeypatch.setattr(archive, "process_event", fake_process)
    monkeypatch.setattr(archive, "fanout", fake_fanout)

    def _event(rkey: str, operation: str) -> dict:
        return {
            "kind": "commit",
            "did": DID,
            "collection": REVIEW,
            "rkey": rkey,
            "operation": operation,
        }

    asyncio.run(
        archive._apply(
            [_event("r1", "delete"), _event("r9", "delete"), _event("r1", "create")],
            worker=DeliveryWorker(),
            deliver=False,
        )
    )

    # The pipeline itself is never handed a worker, so nothing it derives from
    # a delete can escape — a teal.fm session re-anchored by one would fan out
    # a Create of its own.
    assert seen == [("delete r1", False), ("delete r9", False), ("create r1", False)]
    # Only the record peers actually hold is retracted, and only the Delete.
    assert fanned == [(_uri("r1"), "Delete")]
