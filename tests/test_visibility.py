"""Bluesky's visibility preferences, carried onto the fediverse.

Two toggles, read off the atproto account and never set on this side:

- ``app.bsky.actor.contentVisibilityDeclaration`` ("hide my posts from
  algorithmic recommendations") becomes ``discoverable: false`` on the
  bridged ``Person``.
- The ``!no-unauthenticated`` self-label ("hide my posts from logged-out
  users") additionally makes posts unlisted and strips the HTML pages.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from pyld import jsonld
from skybridge.activitypub.actors import person_actor
from skybridge.atproto import identity
from skybridge.atproto.replay import replay_file
from skybridge.crypto import _ld_document_loader
from skybridge.db import session_scope
from skybridge.main import app
from skybridge.models import BridgedActor, Record
from skybridge.pipeline import process_event
from skybridge.translate import neodb
from sqlalchemy import select

AP = {"Accept": "application/activity+json"}
DID = "did:plc:visibility"
HANDLE = "vis.test"


def _actor(**flags) -> BridgedActor:
    identity.ensure_actor(DID, allow_network=False)
    identity.rename_actor(DID, HANDLE)
    with session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None
        for name, value in flags.items():
            setattr(row, name, value)
        session.flush()
        session.expunge(row)
        return row


# --------------------------------------------------------------------------- #
# The actor document
# --------------------------------------------------------------------------- #
def test_no_flags_emits_nothing(settings):
    doc = person_actor(_actor())
    assert "discoverable" not in doc
    assert "indexable" not in doc


def test_hide_from_recommendations_emits_discoverable_false(settings):
    doc = person_actor(_actor(hide_from_recommendations=True))
    assert doc["discoverable"] is False
    assert doc["toot:discoverable"] is False
    # The preference is about recommendation surfaces, not about search.
    assert "indexable" not in doc


def test_no_unauthenticated_emits_both_flags(settings):
    doc = person_actor(_actor(no_unauthenticated=True))
    assert doc["discoverable"] is False
    assert doc["indexable"] is False
    assert doc["toot:indexable"] is False


# --------------------------------------------------------------------------- #
# Both receivers have to see the flag, and they read it differently
# --------------------------------------------------------------------------- #
def _canonicalise(doc: dict) -> dict:
    """What NeoDB/takahe does to a fetched actor before reading it.

    ``core/ld.py canonicalise()``: expand, then compact against the document's
    OWN context. Run with the same cached context documents takahe uses, so
    this reproduces the receiver rather than approximating it.
    """
    jsonld.set_document_loader(_ld_document_loader)
    context = doc.get("@context", [])
    if not isinstance(context, list):
        context = [context]
    payload = dict(doc)
    payload["@context"] = context
    return jsonld.compact(jsonld.expand(payload), context)


def test_neodb_reads_the_flag_after_its_own_canonicalisation(settings):
    """takahe: ``self.discoverable = document.get("toot:discoverable", True)``.

    The prefixed key has to survive compaction, or the receiver silently falls
    back to its permissive default and the preference is lost.
    """
    doc = person_actor(_actor(hide_from_recommendations=True))

    compacted = _canonicalise(doc)

    assert compacted.get("toot:discoverable", True) is False


def test_a_receiver_reading_raw_keys_sees_the_flag(settings):
    """Mastodon: ``@account.discoverable = @json['discoverable'] || false``.

    It never compacts a fetched actor, so it only ever sees the bare key.
    """
    doc = person_actor(_actor(hide_from_recommendations=True))

    assert doc["discoverable"] is False


def test_the_two_keys_do_not_collide_into_an_array(settings):
    """Only one value survives on either path.

    They would fold together if our context aliased ``discoverable``, and a
    receiver reading ``[false, false]`` would find it truthy.
    """
    doc = person_actor(_actor(hide_from_recommendations=True))

    compacted = _canonicalise(doc)

    assert compacted["toot:discoverable"] is False
    assert compacted["discoverable"] is False


# --------------------------------------------------------------------------- #
# Ingesting the declaration
# --------------------------------------------------------------------------- #
def _declaration_event(hide: bool | None, *, op="create", did=DID, seq=None):
    record = {} if hide is None else {"hideFromAlgorithmicRecommendations": hide}
    if seq is None:
        # v1 shape: no seq to order by, which is the normal offline case.
        return {
            "did": did,
            "time_us": 1_700_000_000_000_000,
            "kind": "commit",
            "commit": {
                "operation": op,
                "collection": identity.VISIBILITY_COLLECTION,
                "rkey": "self",
                "record": record,
            },
        }
    # v2 shape: flat, and the only dialect that carries a sequence number.
    return {
        "did": did,
        "kind": "commit",
        "seq": seq,
        "time": "2026-08-27T19:03:40Z",
        "operation": op,
        "collection": identity.VISIBILITY_COLLECTION,
        "rkey": "self",
        "record": record,
    }


def _run(event):
    return asyncio.run(process_event(event, allow_network=False))


def test_declaration_sets_the_flag_and_publishes_an_actor_update(settings):
    _actor()

    result = _run(_declaration_event(True))

    assert result is not None
    assert result.activity["type"] == "Update"
    assert result.activity["object"]["discoverable"] is False
    with session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None and row.hide_from_recommendations is True


def test_the_update_activity_survives_canonicalisation_too(settings):
    """The activity needs the toot prefix in its OWN context.

    A receiver compacting an inbound activity uses the outer context, so
    without it the embedded actor's prefixed key compacts to a full IRI and
    ``document.get("toot:discoverable", True)`` reads the preference as unset.
    """
    _actor()
    result = _run(_declaration_event(True))
    assert result is not None

    compacted = _canonicalise(result.activity)

    assert compacted["object"].get("toot:discoverable", True) is False
    assert compacted["object"]["discoverable"] is False


def test_declaration_never_mints_an_actor(settings):
    """We bridge people because of what they post, not because they set a
    preference — the same rule identity and profile events follow."""
    result = _run(_declaration_event(True, did="did:plc:stranger"))

    assert result is None
    assert identity.actor_by_ident("did:plc:stranger") is None


@pytest.mark.parametrize(
    "op,record",
    [("update", False), ("delete", None)],
    ids=["set-to-false", "record-deleted"],
)
def test_turning_the_preference_off_clears_the_flag(settings, op, record):
    """A deleted record and an explicit false are the same statement: the
    lexicon requires a missing record to read as false."""
    _actor(hide_from_recommendations=True)

    result = _run(_declaration_event(record, op=op))

    assert result is not None
    assert "discoverable" not in result.activity["object"]
    with session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None and row.hide_from_recommendations is False


def test_an_unchanged_declaration_publishes_nothing(settings):
    _actor(hide_from_recommendations=True)

    assert _run(_declaration_event(True)) is None


def test_a_replayed_declaration_is_not_applied_twice(settings):
    """Its own high-water mark, because the record is never archived and so
    has no Record.last_seq to compare against."""
    _actor()

    assert _run(_declaration_event(True, seq=20)) is not None
    # An older commit redelivered after the newer one must not win.
    assert _run(_declaration_event(False, seq=10)) is None
    with session_scope() as session:
        row = session.get(BridgedActor, DID)
        assert row is not None and row.hide_from_recommendations is True


def test_the_declaration_is_not_archived_as_a_record(settings):
    """It is identity metadata, not content: /archive and the stats stay
    about actual posts."""
    _actor()

    _run(_declaration_event(True))

    with session_scope() as session:
        assert session.scalars(select(Record)).first() is None


# --------------------------------------------------------------------------- #
# `!no-unauthenticated` makes posts unlisted
# --------------------------------------------------------------------------- #
PUBLIC = neodb.PUBLIC


def _note_and_activity(*, unlisted: bool):
    return neodb.translate(
        did=DID,
        handle=HANDLE,
        collection="social.popfeed.feed.review",
        rkey="rk1",
        record={"text": "good", "rating": 8},
        operation="create",
        event_time=None,
        unlisted=unlisted,
    )


def test_public_addressing_by_default(settings):
    note, activity = _note_and_activity(unlisted=False)

    assert note["to"] == [PUBLIC]
    assert activity["to"] == [PUBLIC]


def test_unlisted_swaps_to_and_cc(settings):
    note, activity = _note_and_activity(unlisted=True)

    followers = f"{settings.actor_id(HANDLE)}/followers"
    assert note["to"] == [followers]
    assert note["cc"] == [PUBLIC]
    assert activity["to"] == [followers]
    assert activity["cc"] == [PUBLIC]


def test_unlisted_still_reaches_the_relay_and_neodb(settings):
    """Both receivers accept ``cc``-public.

    neodb-relay redistributes on To OR Cc (``api/handle.go``), and takahe
    files a cc-public post as unlisted rather than dropping it
    (``activities/models/post.py by_ap``). Reach is reduced; delivery is not.
    """
    _note, activity = _note_and_activity(unlisted=True)

    assert PUBLIC in activity["to"] + activity["cc"]


def test_the_pipeline_addresses_a_labelled_author_unlisted(settings):
    _actor(no_unauthenticated=True)
    event = {
        "did": DID,
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "social.popfeed.feed.review",
            "rkey": "rk1",
            "record": {
                "text": "good",
                "rating": 8,
                "subject": {"title": "A Film", "type": "movie", "id": "tt1"},
            },
        },
    }

    result = _run(event)

    assert result is not None
    assert result.activity["cc"] == [PUBLIC]


# --------------------------------------------------------------------------- #
# The web pages we serve ourselves
# --------------------------------------------------------------------------- #
@pytest.fixture
def client(settings, fixture_path) -> TestClient:
    asyncio.run(replay_file(fixture_path, allow_network=False))
    return TestClient(app)


def _bridged_row() -> tuple[str, str, str]:
    """(handle, did, at_uri) of a bridged author with a published post."""
    with session_scope() as session:
        record = session.scalars(
            select(Record).where(Record.ap_object_json.isnot(None)).limit(1)
        ).first()
        assert record is not None
        actor = session.get(BridgedActor, record.did)
        assert actor is not None
        return actor.handle, actor.did, record.at_uri


def _label(did: str) -> None:
    with session_scope() as session:
        row = session.get(BridgedActor, did)
        assert row is not None
        row.no_unauthenticated = True


def test_profile_page_is_gated_and_noindex(client, settings):
    handle, did, _ = _bridged_row()
    _label(did)

    body = client.get(f"/users/{handle}").text

    assert 'content="noindex, nofollow"' in body
    assert "not shown to signed-out readers" in body
    # No link preview either: Bluesky answers oEmbed for these with a 403.
    assert 'property="og:description"' not in body


def test_the_actor_document_is_unaffected(client, settings):
    """Only the HTML view is gated. A peer that follows the author is exactly
    the audience the label still allows."""
    handle, did, _ = _bridged_row()
    _label(did)

    doc = client.get(f"/users/{handle}", headers=AP).json()

    assert doc["type"] == "Person"
    assert doc["discoverable"] is False


def test_post_page_hides_the_content(client, settings):
    handle, did, at_uri = _bridged_row()
    rkey = at_uri.rsplit("/", 1)[-1]
    before = client.get(f"/users/{handle}/posts/{rkey}")
    assert before.status_code == 200

    _label(did)
    body = client.get(f"/users/{handle}/posts/{rkey}").text

    assert 'content="noindex, nofollow"' in body
    assert "not shown to signed-out readers" in body
    assert 'property="og:title"' not in body


def test_the_post_object_is_unaffected(client, settings):
    handle, did, at_uri = _bridged_row()
    rkey = at_uri.rsplit("/", 1)[-1]
    _label(did)

    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)

    assert r.status_code == 200
    assert r.json()["type"] == "Note"


def test_the_profile_post_list_goes_with_the_content(client, settings):
    """The gated profile shows identity only, so the post list goes too."""
    handle, did, _ = _bridged_row()
    assert "Recent posts" in client.get(f"/users/{handle}").text

    _label(did)

    assert "Recent posts" not in client.get(f"/users/{handle}").text


def test_catalog_item_listing_drops_a_labelled_author(client, settings):
    """The catalog page is public, so a hidden author's marks stay off it —
    out of the listing, and so out of the schema.org aggregate built from it."""
    _handle, did, at_uri = _bridged_row()
    item = "/catalog/movie/imdbId-tt6710474"
    rkey = at_uri.rsplit("/", 1)[-1]
    before = client.get(item).text
    assert f"/posts/{rkey}" in before
    assert "aggregateRating" in before

    _label(did)

    after = client.get(item).text
    assert f"/posts/{rkey}" not in after
    assert "aggregateRating" not in after
    # The item itself is still described.
    assert '"@type": "Movie"' in after


def test_the_post_page_withholds_the_source_record(client, settings):
    """The at:// URI names the author's DID and collection: identity-only means
    it is not on the gated page either."""
    handle, did, at_uri = _bridged_row()
    rkey = at_uri.rsplit("/", 1)[-1]
    assert at_uri in client.get(f"/users/{handle}/posts/{rkey}").text

    _label(did)

    assert at_uri not in client.get(f"/users/{handle}/posts/{rkey}").text


def test_archive_views_drop_a_labelled_author(client, settings):
    _handle, did, at_uri = _bridged_row()
    # Listing rows link to /archive/{did}/{collection}/{rkey}, not the at:// URI.
    path = at_uri.removeprefix("at://")
    assert f"/archive/{path}" in client.get("/archive").text

    _label(did)

    assert f"/archive/{path}" not in client.get("/archive").text
    # The detail page shows the raw source record, so it goes too.
    assert client.get(f"/archive/{path}").status_code == 404
