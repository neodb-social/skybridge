"""NodeInfo 2.1 discovery + document, reporting basic relay stats."""

from __future__ import annotations

from skybridge.config import get_settings
from skybridge.stats import UNKNOWN, cached_usage


def discovery() -> dict:
    settings = get_settings()
    return {
        "links": [
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.1",
                "href": settings.url("nodeinfo/2.1"),
            }
        ]
    }


def _counted(**numbers: int) -> dict[str, int]:
    """Only the numbers that have been counted at least once.

    A cold cache reports nothing rather than a zero, which a peer would read as
    an empty node instead of an unmeasured one.
    """
    return {name: value for name, value in numbers.items() if value != UNKNOWN}


def document() -> dict:
    # Answered from the cache stats.usage_refresh_loop keeps warm: a scrape
    # runs no queries, and the counts may be up to that interval old.
    counts = cached_usage()
    usage: dict = {}
    users = _counted(total=counts["total"], activeMonth=counts["active_month"])
    if users:
        usage["users"] = users
    usage.update(_counted(localPosts=counts["local_posts"]))

    metadata: dict = {
        # NeoDB's peer discovery (takahe get_neodb_peers) requires
        # metadata.nodeEnvironment == "production" plus "neodb" in protocols
        "nodeEnvironment": "production",
        "nodeName": get_settings().relay_name,
        "nodeDescription": get_settings().relay_summary,
    }
    metadata.update(_counted(relays=counts["relays_accepted"], worksCatalogued=counts["works"]))

    return {
        "version": "2.1",
        "software": {
            "name": "neodb-skybridge",
            "version": "0.1.0",
            "repository": "https://github.com/neodb-social/skybridge",
        },
        "protocols": ["activitypub", "neodb"],
        "services": {"inbound": ["atproto"], "outbound": ["activitypub"]},
        "openRegistrations": False,
        "usage": usage,
        "metadata": metadata,
    }
