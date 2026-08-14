"""Public NeoDB instance directory (https://neodb.net/servers.json).

The directory is fetched at startup and refreshed hourly by
:func:`refresh_loop` (started from the app lifespan). The catalog item page
links each work to the same item on those servers via NeoDB's URL-based
lookup (``https://<host>/search?q=<catalog item url>``): our catalog endpoint
serves the item in NeoDB's ItemSchema shape, so the peer resolves the link to
a local item instead of minting a duplicate.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

import httpx

from skybridge.config import get_settings

log = logging.getLogger("skybridge.neodb_servers")

SERVERS_URL = "https://neodb.net/servers.json"
REFRESH_INTERVAL = 3600.0

# Last successfully fetched directory; empty until the first fetch succeeds,
# and kept as-is when a refresh fails (stale beats empty).
_servers: list[dict[str, str]] = []


def get_servers() -> list[dict[str, str]]:
    """The cached directory: ``[{"name": ..., "host": ...}, ...]``."""
    return list(_servers)


def set_servers(servers: list[dict[str, str]]) -> None:
    """Replace the cache (tests)."""
    global _servers
    _servers = list(servers)


def _parse(doc: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for entry in (doc or {}).get("servers") or []:
        host = (entry.get("host") or "").strip()
        if host:
            out.append({"name": entry.get("name") or host, "host": host})
    return out


async def refresh(client: httpx.AsyncClient | None = None) -> bool:
    """Fetch the directory once; report whether the cache was updated."""
    global _servers
    headers = {"User-Agent": get_settings().user_agent}
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=15.0) as own_client:
                resp = await own_client.get(SERVERS_URL, headers=headers)
        else:
            resp = await client.get(SERVERS_URL, headers=headers)
        resp.raise_for_status()
        servers = _parse(resp.json())
    except (httpx.HTTPError, ValueError, AttributeError, TypeError):
        log.warning(
            "refresh of %s failed; keeping %d cached server(s)",
            SERVERS_URL,
            len(_servers),
            exc_info=True,
        )
        return False
    _servers = servers
    return True


async def refresh_loop() -> None:
    """Refresh at startup, then hourly, until cancelled."""
    while True:
        await refresh()
        await asyncio.sleep(REFRESH_INTERVAL)


def peer_links(item_url: str) -> list[dict[str, str]]:
    """Links resolving *item_url* on each public server (URL-based lookup)."""
    own = get_settings().domain
    return [
        {"name": s["name"], "url": f"https://{s['host']}/search?q={quote(item_url, safe='')}"}
        for s in _servers
        if s["host"] != own
    ]
