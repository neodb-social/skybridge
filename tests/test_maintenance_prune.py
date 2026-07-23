"""repair2 / prune_stale_deliveries: drop delivery-log rows for inboxes that
left the current relay/follower audience, keep the rest."""

from __future__ import annotations

from skybridge.db import session_scope
from skybridge.maintenance import prune_stale_deliveries
from skybridge.models import Delivery, Follow, Relay
from sqlalchemy import func, select


def _delivery(inbox: str, n: int) -> None:
    with session_scope() as session:
        for i in range(n):
            session.add(
                Delivery(
                    record_uri=f"at://did:plc:x/c/{inbox[-4:]}{i}",
                    target_inbox=inbox,
                    activity_type="Create",
                    status="sent",
                )
            )


def _seed(settings) -> None:
    with session_scope() as session:
        session.add(Relay(inbox="https://relay.live/inbox", state="accepted"))
        session.add(Relay(inbox="https://relay.gone/inbox", state="unsubscribed"))
        session.add(
            Follow(
                local_did="did:plc:a",
                follower_actor_id="https://f.example/actor",
                follower_inbox="https://follower.live/inbox",
                follower_shared_inbox="https://follower.live/shared",
                state="accepted",
            )
        )
    _delivery("https://relay.live/inbox", 5)  # accepted relay -> keep
    _delivery("https://relay.gone/inbox", 4)  # non-accepted relay -> stale
    _delivery("https://follower.live/shared", 3)  # accepted follower (shared) -> keep
    _delivery("https://eggplant.place/inbox/", 7)  # unknown/removed -> stale


def test_prune_dry_run_reports_without_deleting(settings):
    _seed(settings)
    report = prune_stale_deliveries()  # dry-run default
    assert report.dry_run and report.deleted == 0
    stale = dict(report.stale)
    kept = dict(report.kept)
    assert stale == {"https://relay.gone/inbox": 4, "https://eggplant.place/inbox/": 7}
    assert kept == {"https://relay.live/inbox": 5, "https://follower.live/shared": 3}
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(Delivery)) == 19  # untouched


def test_prune_delete_removes_only_stale_rows(settings):
    _seed(settings)
    report = prune_stale_deliveries(delete=True)
    assert not report.dry_run
    assert report.deleted == 11  # 4 + 7
    with session_scope() as session:
        remaining = {
            inbox for (inbox,) in session.execute(select(Delivery.target_inbox).distinct()).all()
        }
    assert remaining == {"https://relay.live/inbox", "https://follower.live/shared"}


def test_prune_keeps_accepted_relay_regardless_of_env_config(settings):
    # relay_inboxes() would intersect with SKYBRIDGE_RELAYS (unset here); the
    # prune must key off the Relay table so the live relay isn't wrongly stale.
    with session_scope() as session:
        session.add(Relay(inbox="https://relay.live/inbox", state="accepted"))
    _delivery("https://relay.live/inbox", 3)
    report = prune_stale_deliveries(delete=True)
    assert report.deleted == 0
    assert dict(report.kept) == {"https://relay.live/inbox": 3}
