"""HTTP endpoint tests via FastAPI's TestClient.

The client is constructed WITHOUT entering its context manager so the app
lifespan (which would re-init the DB and start the delivery worker) does not
run — we drive the routes against the conftest in-memory DB seeded by the
fixture replay.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from skybridge.atproto import identity
from skybridge.atproto.replay import replay_file
from skybridge.db import session_scope
from skybridge.main import app
from skybridge.models import BridgedActor, Record, utcnow
from skybridge.pipeline import process_event
from skybridge.stats import refresh_usage, usage_counts
from sqlalchemy import select

AP = {"accept": "application/activity+json"}


@pytest.fixture
def client(settings, fixture_path) -> TestClient:
    asyncio.run(replay_file(fixture_path, allow_network=False))
    return TestClient(app)


def _a_bridged_handle() -> str:
    with session_scope() as session:
        actor = session.scalar(
            select(BridgedActor).where(BridgedActor.did != "did:skybridge:relay")
        )
        assert actor is not None
        return actor.handle


def test_webfinger_relay(client, settings):
    r = client.get(
        "/.well-known/webfinger", params={"resource": settings.acct(settings.relay_username)}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["links"][0]["href"] == settings.relay_actor_id
    assert not any("bsky.app" in alias for alias in body["aliases"])


def test_webfinger_unknown_returns_404(client):
    r = client.get("/.well-known/webfinger", params={"resource": "acct:nobody@bridge.test"})
    assert r.status_code == 404


def test_webfinger_wrong_domain(client):
    r = client.get("/.well-known/webfinger", params={"resource": "acct:x@elsewhere.example"})
    assert r.status_code == 404


def test_relay_actor_document(client, settings):
    r = client.get("/actor", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Application"
    assert doc["id"] == settings.relay_actor_id
    assert doc["publicKey"]["id"].endswith("#main-key")
    assert doc["endpoints"]["sharedInbox"] == settings.url("inbox")


def test_person_actor_document(client, settings):
    handle = _a_bridged_handle()
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))
        assert actor is not None
        did = actor.did
    r = client.get(f"/users/{handle}", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Person"
    assert doc["preferredUsername"] == handle
    assert "publicKey" in doc
    assert doc["alsoKnownAs"] == [f"at://{did}", f"https://bsky.app/profile/{did}"]
    assert {
        "alsoKnownAs": {"@id": "as:alsoKnownAs", "@type": "@id"},
        "toot": "http://joinmastodon.org/ns#",
    } in doc["@context"]
    # No visibility preference set: say nothing rather than volunteer a
    # permissive `discoverable: true` nobody asked for.
    assert "discoverable" not in doc
    assert "toot:discoverable" not in doc
    assert 'rel="me"' in doc["attachment"][0]["value"]
    assert 'rel="me"' in doc["summary"]


def test_person_actor_document_includes_icon_when_avatar_set(client, settings):
    handle = _a_bridged_handle()
    avatar_url = "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did:plc:test&cid=bafkrei"
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))
        assert actor is not None
        actor.avatar = avatar_url
    r = client.get(f"/users/{handle}", headers=AP)
    assert r.status_code == 200
    assert r.json()["icon"] == {"type": "Image", "url": avatar_url}


def test_webfinger_resolves_bridged_user(client, settings):
    handle = _a_bridged_handle()
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))
        assert actor is not None
        did = actor.did
    r = client.get("/.well-known/webfinger", params={"resource": settings.acct(handle)})
    assert r.status_code == 200
    body = r.json()
    assert body["links"][0]["href"] == settings.actor_id(handle)
    assert f"https://bsky.app/profile/{did}" in body["aliases"]


def test_nodeinfo(client):
    disc = client.get("/.well-known/nodeinfo")
    assert disc.status_code == 200
    href = disc.json()["links"][0]["href"]
    assert href.endswith("/nodeinfo/2.1")
    doc = client.get("/nodeinfo/2.1").json()
    assert doc["software"]["name"] == "neodb-skybridge"
    assert doc["metadata"]["nodeEnvironment"] == "production"
    assert "neodb" in doc["protocols"]
    # The counts come from the refresh loop, which the test client never
    # starts: uncounted numbers are left out rather than reported as zero.
    assert doc["usage"] == {}
    assert "relays" not in doc["metadata"]

    asyncio.run(refresh_usage())
    doc = client.get("/nodeinfo/2.1").json()
    assert doc["usage"]["users"]["total"] >= 1
    assert doc["usage"]["users"]["activeMonth"] >= 1
    assert doc["usage"]["localPosts"] >= 1
    assert doc["metadata"]["relays"] == 0


def test_nodeinfo_counts_skip_retracted_records(client):
    """Retracting an author's records drops them from both counts.

    This is the shape an opt-out leaves behind, and it stamps `updated_at` on
    every row it tombstones — so "active this month" has to read the tombstone,
    not the timestamp.
    """
    _handle, at_uri, _rkey, _post_url = _the_review()
    before = usage_counts()
    with session_scope() as session:
        review = session.get(Record, at_uri)
        assert review is not None
        did = review.did
        rows = list(session.scalars(select(Record).where(Record.did == did)))
        published = sum(1 for row in rows if row.ap_object_json and row.deleted_at is None)
        for row in rows:
            row.op = "delete"
            row.deleted_at = row.updated_at = utcnow()

    after = usage_counts()
    assert published >= 1
    assert after["local_posts"] == before["local_posts"] - published
    assert after["active_month"] == before["active_month"] - 1


# --- handle renames: everything already federated keeps resolving ----------


def _rename(handle: str, new_handle: str) -> str:
    """Rename the actor currently holding ``handle``; returns its DID."""
    actor = identity.actor_by_ident(handle)
    assert actor is not None
    did = actor.did
    assert identity.rename_actor(did, new_handle) is not None
    return did


def test_retired_handle_redirects_to_the_live_actor(client, settings):
    handle = _a_bridged_handle()
    _rename(handle, "renamed.test")

    r = client.get(f"/users/{handle}", headers=AP, follow_redirects=False)

    assert r.status_code == 301
    assert r.headers["location"] == settings.actor_id("renamed.test")


def test_retired_handle_still_dereferences_its_posts(client, settings):
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.review",
                Record.ap_object_json.isnot(None),
            )
        )
        assert rec is not None and rec.ap_object_json is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle, rkey = actor.handle, rec.rkey
        published_id = json.loads(rec.ap_object_json)["id"]
        did = rec.did

    identity.rename_actor(did, "renamed.test")
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)

    assert r.status_code == 200
    # The Note keeps the id peers already hold, under either handle.
    assert r.json()["id"] == published_id
    assert client.get(f"/users/renamed.test/posts/{rkey}", headers=AP).json()["id"] == published_id


def test_webfinger_answers_a_retired_handle_with_the_live_one(client, settings):
    handle = _a_bridged_handle()
    _rename(handle, "renamed.test")

    r = client.get("/.well-known/webfinger", params={"resource": settings.acct(handle)})

    assert r.status_code == 200
    body = r.json()
    assert body["subject"] == settings.acct("renamed.test")
    assert body["links"][0]["href"] == settings.actor_id("renamed.test")
    assert settings.acct(handle) in body["aliases"]


def test_review_object_dereferenceable(client, settings):
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(Record.collection == "social.popfeed.feed.review")
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    assert doc["type"] == "Note"
    assert any(rel["type"] == "Rating" for rel in doc["relatedWith"])


def test_deleted_object_is_tombstone(client, settings):
    # Delete a published record (a review), then expect a 410 Tombstone.
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.review",
                Record.ap_object_json.isnot(None),
            )
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        did, collection, rkey = rec.did, rec.collection, rec.rkey
    event = {
        "did": did,
        "kind": "commit",
        "commit": {"operation": "delete", "collection": collection, "rkey": rkey},
    }
    asyncio.run(process_event(event, allow_network=False))
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 410
    assert r.json()["type"] == "Tombstone"


def test_never_published_deleted_record_is_404(client, settings):
    # The fixture's delete is collection membership: never federated, so its
    # URL was never valid — 404, not a Tombstone.
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.deleted_at.isnot(None)))
        assert rec is not None
        assert rec.ap_object_json is None and rec.ap_activity_json is None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 404


def test_catalog_object_is_neodb_item(client, settings):
    # The fixture review's movie work, in NeoDB ItemSchema shape.
    r = client.get("/catalog/movie/imdbId-tt6710474", headers=AP)
    assert r.status_code == 200
    doc = r.json()
    # catalog/sites/fedi.py requirements: supported type, id == fetched url
    assert doc["type"] == "Movie"
    assert doc["id"] == settings.catalog_id("movie", "imdbId-tt6710474")
    assert doc["display_title"] == "Everything Everywhere All at Once"
    # identifier URLs let the peer merge with its existing catalog
    urls = [e["url"] for e in doc["external_resources"]]
    assert "https://www.imdb.com/title/tt6710474" in urls
    assert "https://www.themoviedb.org/movie/545611" in urls
    assert doc["imdb"] == "tt6710474"
    assert doc["cover_image_url"].startswith("https://")


def test_neodb_marker_redirects_to_local_path(client):
    r = client.get("/~neodb~/catalog/movie/tmdbId-1", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/catalog/movie/tmdbId-1"


def test_neodb_marker_guards_against_protocol_relative_open_redirect(client):
    # Browsers normalize "\" to "/" when resolving Location, so a leading
    # backslash is as dangerous as a slash here.
    for bad in ("//evil.com/x", "/\\evil.com/x", "\\evil.com/x", "/\\/evil.com/x"):
        r = client.get(f"/~neodb~/{bad}", follow_redirects=False)
        assert r.status_code == 302
        location = r.headers["location"]
        assert location.startswith("/")
        assert not location.startswith(("//", "/\\")), bad


def test_stats_json(client):
    r = client.get("/stats")
    assert r.status_code == 200
    assert r.json()["records_total"] > 0


def test_catalog_index_renders(client):
    assert client.get("/catalog").status_code == 200


def test_archive_detail_resolves_every_link_shape(client):
    with session_scope() as session:
        rec = session.scalar(select(Record))
        assert rec is not None
        at_uri, did, collection, rkey = rec.at_uri, rec.did, rec.collection, rec.rkey
    # The link the archive/manage tables mint: plain path segments, so no
    # proxy that normalises paths can collapse an "at://" into "at:/".
    paths = [f"/archive/{did}/{collection}/{rkey}"]
    # Links minted before that, and the collapsed form a normalising proxy
    # (Cloudflare) turns them into on the way in.
    paths += [f"/archive/{at_uri}", "/archive/" + at_uri.replace("at://", "at:/")]
    for path in paths:
        r = client.get(path)
        assert r.status_code == 200, path
        assert rkey in r.text

    assert client.get(f"/archive/{did}/{collection}/no-such-rkey").status_code == 404


def test_profile_html_page_with_avatar_and_opengraph(client, settings):
    handle = _a_bridged_handle()
    avatar_url = "https://cdn.example/avatar.jpg"
    with session_scope() as session:
        actor = session.scalar(select(BridgedActor).where(BridgedActor.handle == handle))
        assert actor is not None
        actor.avatar = avatar_url
    r = client.get(f"/users/{handle}")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    page = r.text
    assert handle in page
    # avatar rendered on the page and advertised to link-preview scrapers
    assert f'<img class="avatar" src="{avatar_url}"' in page
    assert f'<meta property="og:image" content="{avatar_url}" />' in page
    assert f'<meta property="og:url" content="{settings.actor_id(handle)}" />' in page


def test_profile_html_page_without_avatar_omits_og_image(client):
    handle = _a_bridged_handle()
    r = client.get(f"/users/{handle}")
    assert r.status_code == 200
    assert "og:image" not in r.text
    assert 'class="avatar-fallback"' in r.text


def test_work_html_page_with_poster_and_opengraph(client, settings):
    doc = client.get("/catalog/movie/imdbId-tt6710474", headers=AP).json()
    poster = doc["cover_image_url"]
    r = client.get("/catalog/movie/imdbId-tt6710474")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    page = r.text
    assert "Everything Everywhere All at Once" in page
    assert f'<img class="poster" src="{poster}"' in page
    assert f'<meta property="og:image" content="{poster}" />' in page
    url = settings.catalog_id("movie", "imdbId-tt6710474")
    assert f'<meta property="og:url" content="{url}" />' in page
    assert "https://www.imdb.com/title/tt6710474" in page


def test_work_html_page_links_neodb_servers(client, settings):
    from urllib.parse import quote

    from skybridge import neodb_servers

    neodb_servers.set_servers(
        [
            {"name": "NeoDB", "host": "neodb.social"},
            {"name": "Self", "host": settings.domain},  # never link to ourselves
        ]
    )
    try:
        page = client.get("/catalog/movie/imdbId-tt6710474").text
    finally:
        neodb_servers.set_servers([])
    url = quote(settings.catalog_id("movie", "imdbId-tt6710474"), safe="")
    assert f'<a href="https://neodb.social/search?q={url}" rel="nofollow">NeoDB</a>' in page
    assert f"https://{settings.domain}/search?q=" not in page


def test_work_html_page_without_servers_has_no_peer_section(client):
    page = client.get("/catalog/movie/imdbId-tt6710474").text
    assert "Find this item on a NeoDB server" not in page


def _a_published_review() -> tuple[str, str]:
    with session_scope() as session:
        rec = session.scalar(
            select(Record).where(
                Record.collection == "social.popfeed.feed.review",
                Record.ap_object_json.isnot(None),
            )
        )
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        return actor.handle, rec.rkey


def test_post_html_page_with_opengraph(client, settings):
    handle, rkey = _a_published_review()
    doc = client.get(f"/users/{handle}/posts/{rkey}", headers=AP).json()
    r = client.get(f"/users/{handle}/posts/{rkey}")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    page = r.text
    assert handle in page
    # the translator's lead-in line is embedded as HTML, not escaped
    assert "Rated " in page or "Reviewed " in page
    post_url = settings.post_id(handle, rkey)
    assert f'<meta property="og:url" content="{post_url}" />' in page
    # the work tag surfaces as a poster card linking to our catalog page
    work_tag = next(t for t in doc["tag"] if "href" in t)
    assert work_tag["href"] in page
    if work_tag.get("image"):
        assert f'<meta property="og:image" content="{work_tag["image"]}" />' in page


def test_post_json_accept_variants_still_get_ap(client):
    handle, rkey = _a_published_review()
    for accept in ("application/activity+json", "application/ld+json", "application/json"):
        r = client.get(f"/users/{handle}/posts/{rkey}", headers={"accept": accept})
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/activity+json")
        assert r.json()["type"] == "Note"


def test_deleted_post_html_page_is_410(client):
    handle, rkey = _a_published_review()
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.rkey == rkey))
        assert rec is not None
        did, collection = rec.did, rec.collection
    event = {
        "did": did,
        "kind": "commit",
        "commit": {"operation": "delete", "collection": collection, "rkey": rkey},
    }
    asyncio.run(process_event(event, allow_network=False))
    r = client.get(f"/users/{handle}/posts/{rkey}")
    assert r.status_code == 410
    assert r.headers["content-type"].startswith("text/html")
    assert "deleted" in r.text


def test_post_html_page_scrubs_unsafe_hrefs_in_stored_content(client):
    # Records translated before render_facets validated link schemes may
    # still carry e.g. javascript: hrefs; the web view must not emit them.
    handle, rkey = _a_published_review()
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.rkey == rkey))
        assert rec is not None
        assert rec.ap_object_json is not None
        obj = json.loads(rec.ap_object_json)
        obj["content"] += '<p><a href="javascript:alert(1)" rel="nofollow">click</a></p>'
        rec.ap_object_json = json.dumps(obj)
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert "javascript:" not in page
    assert ">click</a>" in page


def test_post_html_page_keeps_safe_hrefs_and_href_like_prose(client):
    # The scrub above must not overreach. A sanitized review body keeps the
    # link case its author wrote, and keeps quotes in its text nodes, so a
    # scheme-sensitive or tag-blind scrub would eat both of these.
    handle, rkey = _a_published_review()
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.rkey == rkey))
        assert rec is not None
        assert rec.ap_object_json is not None
        obj = json.loads(rec.ap_object_json)
        obj["content"] += (
            '<p><a href="HTTPS://example.com/x" rel="nofollow noopener">link</a>'
            ' and the tag uses href="/chapter" here.</p>'
        )
        rec.ap_object_json = json.dumps(obj)
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert 'href="HTTPS://example.com/x"' in page
    assert 'uses href="/chapter" here.' in page


def test_unknown_post_html_page_is_404(client):
    handle = _a_bridged_handle()
    r = client.get(f"/users/{handle}/posts/nope")
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("text/html")


def test_dashboard_shows_jetstream_endpoint_only_when_ingesting(client, settings):
    # no ingest task (or a finished one) -> not shown
    assert settings.jetstream_url not in client.get("/").text

    class _RunningTask:
        def done(self) -> bool:
            return False

    app.state.ingest_task = _RunningTask()
    try:
        page = client.get("/").text
        assert "Jetstream endpoint" in page
        assert settings.jetstream_url in page
    finally:
        app.state.ingest_task = None


def test_archive_only_list_records_not_published(client, settings):
    with session_scope() as session:
        rec = session.scalar(select(Record).where(Record.collection == "social.popfeed.feed.list"))
        assert rec is not None
        actor = session.get(BridgedActor, rec.did)
        assert actor is not None
        handle = actor.handle
        rkey = rec.rkey
    # Never emitted to AP: not dereferenceable (404, not a Tombstone)...
    r = client.get(f"/users/{handle}/posts/{rkey}", headers=AP)
    assert r.status_code == 404
    # ...and absent from the outbox.
    outbox = client.get(f"/users/{handle}/outbox", headers=AP).json()
    assert outbox["type"] == "OrderedCollection"
    assert settings.post_id(handle, rkey) not in outbox["orderedItems"]
    assert outbox["totalItems"] > 0


# --------------------------------------------------------------------------- #
# Post pages: listings that link to them, and what one shows
# --------------------------------------------------------------------------- #
def _the_review() -> tuple[str, str, str, str]:
    """(handle, at_uri, rkey, post_url) of the fixture's published review."""
    with session_scope() as session:
        record = session.scalar(
            select(Record).where(Record.collection == "social.popfeed.feed.review")
        )
        assert record is not None and record.ap_object_json is not None
        actor = session.get(BridgedActor, record.did)
        assert actor is not None
        post_url = json.loads(record.ap_object_json)["id"]
        return actor.handle, record.at_uri, record.rkey, post_url


def _patch_note(at_uri: str, **changes) -> None:
    """Rewrite fields on a stored Note, to reach a shape the fixture lacks."""
    with session_scope() as session:
        record = session.get(Record, at_uri)
        assert record is not None and record.ap_object_json is not None
        note = json.loads(record.ap_object_json)
        note.update(changes)
        record.ap_object_json = json.dumps(note)


def test_archive_rows_link_to_the_post_page(client):
    _handle, _at_uri, _rkey, post_url = _the_review()
    page = client.get("/archive").text
    # The published review is reachable as a post...
    assert f'<a href="{post_url}">' in page
    # ...and every row still offers the raw record.
    assert "/archive/did:plc:" in page


def test_archive_rows_for_unpublished_records_have_no_post_link(client):
    """An archive-only record has no post page, so its row must not claim one."""
    with session_scope() as session:
        record = session.scalar(select(Record).where(Record.ap_object_json.is_(None)))
        assert record is not None
        rkey, did, collection = record.rkey, record.did, record.collection
    page = client.get("/archive").text
    assert f"/archive/{did}/{collection}/{rkey}" in page
    assert f"/posts/{rkey}" not in page


def test_profile_lists_recent_posts(client):
    handle, _at_uri, _rkey, post_url = _the_review()
    page = client.get(f"/users/{handle}").text
    assert "Recent posts" in page
    assert f'<a href="{post_url}">' in page
    # Titled by the work it marks: marks and reviews are untitled Notes.
    assert "Everything Everywhere All at Once" in page


def test_catalog_item_lists_recent_posts(client):
    _handle, _at_uri, _rkey, post_url = _the_review()
    page = client.get("/catalog/movie/imdbId-tt6710474").text
    assert "Recent posts" in page
    assert f'<a href="{post_url}">' in page


def test_post_page_shows_the_atproto_uri(client):
    handle, at_uri, rkey, _post_url = _the_review()
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert at_uri.startswith("at://")
    assert f"<code>{at_uri}</code>" in page


def test_post_page_shows_published_and_last_modified(client):
    handle, at_uri, rkey, _post_url = _the_review()
    _patch_note(
        at_uri,
        published="2026-07-03T17:16:24.038Z",
        updated="2026-08-01T09:30:00+00:00",
    )
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert '<time datetime="2026-07-03T17:16:24.038Z">2026-07-03 17:16 UTC</time>' in page
    # The edit time is for machines, so it rides in the head, not the body.
    assert '<meta property="article:modified_time" content="2026-08-01T09:30:00+00:00" />' in page
    assert "last modified" not in page


def test_a_post_never_edited_is_last_modified_when_published(client):
    handle, at_uri, rkey, _post_url = _the_review()
    _patch_note(at_uri, published="2026-07-03T17:16:24.038Z", updated=None)
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert '<time datetime="2026-07-03T17:16:24.038Z">2026-07-03 17:16 UTC</time>' in page
    assert '<meta property="article:modified_time" content="2026-07-03T17:16:24.038Z" />' in page


def _schema_of(page: str) -> dict:
    marker = '<script type="application/ld+json">'
    assert marker in page
    body = page.split(marker, 1)[1].split("</script>", 1)[0]
    return json.loads(body)


def test_post_page_embeds_a_schema_org_review(client, settings):
    handle, _at_uri, rkey, post_url = _the_review()
    doc = _schema_of(client.get(f"/users/{handle}/posts/{rkey}").text)
    assert doc["@context"] == "https://schema.org"
    assert doc["@type"] == "Review"
    assert doc["url"] == post_url
    assert doc["author"] == {
        "@type": "Person",
        "name": handle,
        "url": settings.actor_id(handle),
    }
    # The rating rides in schema.org's own vocabulary, not NeoDB's.
    assert doc["reviewRating"] == {
        "@type": "Rating",
        "ratingValue": 10,
        "bestRating": 10,
        "worstRating": 1,
    }
    assert doc["reviewBody"] == "even better on second thought"
    # NeoDB's catalog type maps onto schema.org's.
    assert doc["itemReviewed"]["@type"] == "Movie"
    assert doc["itemReviewed"]["name"] == "Everything Everywhere All at Once"
    assert doc["itemReviewed"]["url"] == settings.catalog_id("movie", "imdbId-tt6710474")


def test_no_schema_org_without_a_rating_or_review(client):
    """A bare shelf mark states no opinion; there is no Review to publish."""
    handle, at_uri, rkey, _post_url = _the_review()
    _patch_note(at_uri, relatedWith=[{"type": "Status", "status": "complete"}])
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert '<script type="application/ld+json">' not in page


def test_schema_org_withholds_a_spoiler_review_body(client):
    """Spoiler text stays behind the page's disclosure control, so the
    machine-readable copy does not restate it."""
    handle, at_uri, rkey, _post_url = _the_review()
    _patch_note(at_uri, sensitive=True, summary="Spoilers: a movie")
    doc = _schema_of(client.get(f"/users/{handle}/posts/{rkey}").text)
    assert "reviewBody" not in doc
    assert doc["reviewRating"]["ratingValue"] == 10


def test_catalog_item_embeds_schema_org(client, settings):
    page = client.get("/catalog/movie/imdbId-tt6710474").text
    doc = _schema_of(page)
    assert doc["@context"] == "https://schema.org"
    # NeoDB's catalog type ("Movie") maps onto schema.org's.
    assert doc["@type"] == "Movie"
    assert doc["@id"] == settings.catalog_id("movie", "imdbId-tt6710474")
    assert doc["url"] == doc["@id"]
    assert doc["name"] == "Everything Everywhere All at Once"
    assert doc["image"].startswith("https://")
    # The same identifier URLs a NeoDB peer merges on tell a search engine
    # which known thing this is.
    assert "https://www.imdb.com/title/tt6710474" in doc["sameAs"]
    assert "https://www.themoviedb.org/movie/545611" in doc["sameAs"]


def test_catalog_item_aggregates_its_ratings(client):
    _handle, _at_uri, _rkey, post_url = _the_review()
    page = client.get("/catalog/movie/imdbId-tt6710474").text
    doc = _schema_of(page)
    assert doc["aggregateRating"] == {
        "@type": "AggregateRating",
        "ratingValue": 10,
        "ratingCount": 1,
        "bestRating": 10,
        "worstRating": 1,
    }
    # The page shows the average it marks up, and the rating behind it.
    assert "10/10" in page
    assert "1 bridged rating" in page
    (review,) = doc["review"]
    assert review["url"] == post_url
    assert review["reviewBody"] == "even better on second thought"
    # Nested in the item it is about, so it does not repeat itemReviewed.
    assert "itemReviewed" not in review


def test_catalog_item_without_ratings_has_no_aggregate(client):
    """An item nobody rated is still describable; there is just nothing to
    aggregate."""
    _handle, at_uri, _rkey, _post_url = _the_review()
    _patch_note(at_uri, relatedWith=[{"type": "Status", "status": "complete"}])
    doc = _schema_of(client.get("/catalog/movie/imdbId-tt6710474").text)
    assert doc["@type"] == "Movie"
    assert "aggregateRating" not in doc
    assert "review" not in doc


def test_catalog_item_withholds_a_spoiler_review_body(client):
    _handle, at_uri, _rkey, _post_url = _the_review()
    _patch_note(at_uri, sensitive=True, summary="Spoilers: a movie")
    (review,) = _schema_of(client.get("/catalog/movie/imdbId-tt6710474").text)["review"]
    assert "reviewBody" not in review
    assert review["reviewRating"]["ratingValue"] == 10


def test_schema_org_cannot_break_out_of_the_script_element(client):
    handle, at_uri, rkey, _post_url = _the_review()
    _patch_note(
        at_uri,
        relatedWith=[
            {"type": "Comment", "content": "<p>pwned</p></script><script>alert(1)</script>"}
        ],
    )
    page = client.get(f"/users/{handle}/posts/{rkey}").text
    assert "</script><script>alert(1)" not in page
    # Still a single well-formed JSON-LD block.
    assert _schema_of(page)["@type"] == "Review"
