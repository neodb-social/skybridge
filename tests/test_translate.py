"""Translation correctness: popfeed records -> NeoDB-compatible ActivityPub."""

from __future__ import annotations

from skybridge.translate import neodb, works

POST = {
    "$type": "social.popfeed.feed.post",
    "text": "Superman - Official Trailer\nhttps://www.ign.com/videos/superman",
    "title": "Superman - Official Trailer - IGN",
    "facets": [
        {
            "index": {"byteStart": 28, "byteEnd": 51},
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": "https://www.ign.com/videos/superman",
                }
            ],
        }
    ],
    "createdAt": {},
    "identifiers": {"imdbId": "tt5950044", "tmdbId": "1061474"},
    "creativeWorkType": "movie",
}

LIST = {
    "$type": "social.popfeed.feed.list",
    "name": "2025 The Game Awards Nominees",
    "description": "Games nominated in 2025",
    "tags": ["Gaming"],
    "listType": "default",
    "createdAt": "2025-11-17T18:09:54.291Z",
}

LIST_ITEM = {
    "$type": "social.popfeed.feed.listItem",
    "title": "Elden Ring",
    "listType": "complete",
    "posterUrl": "https://images.igdb.com/x.jpg",
    "identifiers": {"igdbId": "119133"},
    "addedAt": "2025-11-17T23:08:47.376Z",
    "creativeWorkType": "video_game",
}


def test_post_becomes_note_with_work_link(settings):
    ref = works.work_ref(POST)
    assert ref is not None
    note, activity = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.post",
        rkey="r1",
        record=POST,
        operation="create",
        time_us=1_700_000_000_000_000,
        ref=ref,
    )
    assert note is not None
    assert note["type"] == "Note"
    assert note["attributedTo"] == settings.actor_id("alice.test")
    # facet link rendered as an anchor
    assert '<a href="https://www.ign.com/videos/superman"' in note["content"]
    # work tagged + NeoDB relatedWith Comment with withRegardTo
    related = note["relatedWith"]
    assert any(r.get("withRegardTo") == ref.url for r in related)
    assert activity["type"] == "Create"
    assert activity["object"] is note
    # published falls back to time_us since createdAt is empty
    assert note["published"].startswith("2023-")


def test_list_becomes_shelf(settings):
    note, _activity = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.list",
        rkey="l1",
        record=LIST,
        operation="create",
        time_us=None,
        ref=None,
    )
    assert note is not None
    shelves = [r for r in note["relatedWith"] if r["type"] == "Shelf"]
    assert shelves and shelves[0]["name"] == LIST["name"]
    assert any(t["type"] == "Hashtag" for t in note.get("tag", []))


def test_list_item_status_mark(settings):
    ref = works.work_ref(LIST_ITEM)
    assert ref is not None
    note, _ = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.listItem",
        rkey="i1",
        record=LIST_ITEM,
        operation="create",
        time_us=None,
        ref=ref,
    )
    assert note is not None
    statuses = [r for r in note["relatedWith"] if r["type"] == "Status"]
    assert statuses and statuses[0]["status"] == "complete"
    assert statuses[0]["withRegardTo"] == ref.url
    # poster surfaced as an attachment
    assert note["attachment"][0]["url"] == LIST_ITEM["posterUrl"]


def test_update_and_delete_activities(settings):
    _, upd = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.post",
        rkey="r1",
        record=POST,
        operation="update",
        time_us=None,
        ref=works.work_ref(POST),
    )
    assert upd["type"] == "Update"

    note, dele = neodb.translate(
        did="did:plc:abc",
        handle="alice.test",
        collection="social.popfeed.feed.post",
        rkey="r1",
        record=None,
        operation="delete",
        time_us=None,
        prior_object_id=settings.post_id("alice.test", "r1"),
    )
    assert note is None
    assert dele["type"] == "Delete"
    assert dele["object"]["type"] == "Tombstone"
    assert dele["object"]["id"] == settings.post_id("alice.test", "r1")


def test_work_ref_namespaces_identifier(settings):
    ref = works.work_ref(POST)
    assert ref is not None
    assert ref.work_type == "movie"
    assert ref.work_id == "imdbId-tt5950044"  # priority picks imdb first
    assert ref.url == settings.catalog_id("movie", "imdbId-tt5950044")


def test_render_facets_plain_text():
    html = neodb.render_facets("just text & <b>", None)
    assert html == "<p>just text &amp; &lt;b&gt;</p>"
