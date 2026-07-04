"""NodeInfo 2.1 discovery + document, reporting basic relay stats."""

from __future__ import annotations

from skybridge.config import get_settings
from skybridge.stats import collect_stats


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


def document() -> dict:
    stats = collect_stats()
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
        "usage": {
            "users": {"total": stats["bridged_actors"]},
            "localPosts": stats["records_active"],
        },
        "metadata": {
            "nodeName": get_settings().relay_name,
            "nodeDescription": get_settings().relay_summary,
            "subscribers": stats["subscribers_accepted"],
            "worksCatalogued": stats["works"],
        },
    }
