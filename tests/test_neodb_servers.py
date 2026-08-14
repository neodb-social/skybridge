"""Unit tests for the public NeoDB server directory cache."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import quote

import httpx
import pytest
from skybridge import neodb_servers

DIRECTORY = {
    "version": "1.0",
    "servers": [
        {"host": "neodb.social", "description": "Flagship instance."},
        {"name": "ReviewDB", "host": "reviewdb.app"},
        {"description": "no host, skipped"},
    ],
}


@pytest.fixture(autouse=True)
def _clean_cache():
    neodb_servers.set_servers([])
    yield
    neodb_servers.set_servers([])


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _refresh_with(handler) -> bool:
    async def run() -> bool:
        async with _client(handler) as client:
            return await neodb_servers.refresh(client)

    return asyncio.run(run())


def test_refresh_parses_directory():
    assert _refresh_with(lambda request: httpx.Response(200, json=DIRECTORY))
    assert neodb_servers.get_servers() == [
        # "name" falls back to the host when the entry has none
        {"name": "neodb.social", "host": "neodb.social"},
        {"name": "ReviewDB", "host": "reviewdb.app"},
    ]


@pytest.mark.parametrize(
    "handler",
    [
        lambda request: httpx.Response(500),
        lambda request: httpx.Response(200, text="not json"),
        lambda request: httpx.Response(200, json=["wrong shape"]),
    ],
)
def test_failed_refresh_keeps_last_good_list(handler):
    neodb_servers.set_servers([{"name": "NeoDB", "host": "neodb.social"}])
    assert not _refresh_with(handler)
    assert neodb_servers.get_servers() == [{"name": "NeoDB", "host": "neodb.social"}]


def test_refresh_requests_directory_url_with_tagged_user_agent(settings):
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["ua"] = request.headers.get("user-agent", "")
        return httpx.Response(200, json=DIRECTORY)

    assert _refresh_with(handler)
    assert seen["url"] == "https://neodb.net/servers.json"
    assert "neodb/" in seen["ua"]


def test_peer_links_resolve_via_url_lookup(settings):
    neodb_servers.set_servers(
        [
            {"name": "NeoDB", "host": "neodb.social"},
            {"name": "Self", "host": settings.domain},
        ]
    )
    item = settings.catalog_id("movie", "imdbId-tt1")
    links = neodb_servers.peer_links(item)
    assert links == [
        {"name": "NeoDB", "url": f"https://neodb.social/search?q={quote(item, safe='')}"}
    ]


def test_parse_tolerates_missing_and_empty_fields():
    doc = json.loads(json.dumps(DIRECTORY))
    doc["servers"].append({"host": "   "})
    assert neodb_servers._parse(doc) == [
        {"name": "neodb.social", "host": "neodb.social"},
        {"name": "ReviewDB", "host": "reviewdb.app"},
    ]
    assert neodb_servers._parse({}) == []
    assert neodb_servers._parse(None) == []
