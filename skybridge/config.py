"""Runtime configuration for Skybridge.

Everything that identifies *this* relay derives from :data:`Settings.domain`
(``SKYBRIDGE_DOMAIN``). Nothing in the codebase hardcodes a hostname — actor
ids, webfinger handles, object URLs and NeoDB ``withRegardTo`` catalog URLs are
all built from it via the ``url_*`` helpers below.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

# atproto collections we bridge. Everything else on the firehose is ignored.
# (app.popsky.post is the app's pre-rebrand "Popsky" collection — deprecated and
# no longer written since March 2025, so we don't bridge it.)
WANTED_COLLECTIONS: tuple[str, ...] = (
    "social.popfeed.feed.post",
    "social.popfeed.feed.list",
    "social.popfeed.feed.listItem",
)

# Default public Jetstream endpoint; only the collections above are requested.
DEFAULT_JETSTREAM = "wss://jetstream2.us-east.bsky.network/subscribe"


@dataclass(frozen=True)
class Settings:
    """Immutable settings snapshot, sourced from the environment."""

    domain: str = "localhost:8000"
    scheme: str = "https"
    db_path: str = "skybridge.db"
    jetstream_url: str = DEFAULT_JETSTREAM
    wanted_collections: tuple[str, ...] = WANTED_COLLECTIONS
    # Relay actor identity.
    relay_username: str = "relay"
    relay_name: str = "Skybridge"
    relay_summary: str = (
        "Skybridge mirrors public popfeed (AT Protocol) activity into the "
        "Fediverse as NeoDB-compatible ActivityPub."
    )
    # Delivery worker retry schedule (seconds).
    retry_backoff: tuple[int, ...] = (2, 4, 8, 16)
    user_agent: str = "skybridge/0.1 (+activitypub-relay)"

    # --- URL builders: the single source of truth for our identity ----------

    @property
    def base_url(self) -> str:
        return f"{self.scheme}://{self.domain}"

    def url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @property
    def relay_actor_id(self) -> str:
        return self.url("actor")

    def actor_id(self, ident: str) -> str:
        """Actor id for a bridged user, keyed by handle-or-did identifier."""
        return self.url(f"users/{ident}")

    def object_id(self, obj_id: str) -> str:
        return self.url(f"objects/{obj_id}")

    def post_id(self, ident: str, rkey: str) -> str:
        return self.url(f"users/{ident}/posts/{rkey}")

    def catalog_id(self, work_type: str, work_id: str) -> str:
        return self.url(f"catalog/{work_type}/{work_id}")

    def acct(self, handle: str) -> str:
        return f"acct:{handle}@{self.domain}"


# Override holder so tests / the CLI can install a custom settings snapshot.
_OVERRIDE: Settings | None = None


def _from_env() -> Settings:
    domain = os.environ.get("SKYBRIDGE_DOMAIN", "localhost:8000")
    scheme = os.environ.get(
        "SKYBRIDGE_SCHEME", "http" if domain.startswith("localhost") else "https"
    )
    return Settings(
        domain=domain,
        scheme=scheme,
        db_path=os.environ.get("SKYBRIDGE_DB", "skybridge.db"),
        jetstream_url=os.environ.get("SKYBRIDGE_JETSTREAM", DEFAULT_JETSTREAM),
    )


@lru_cache(maxsize=1)
def _cached() -> Settings:
    return _from_env()


def get_settings() -> Settings:
    """Return the active settings (override wins, else env-derived + cached)."""
    return _OVERRIDE if _OVERRIDE is not None else _cached()


def set_settings(settings: Settings | None) -> None:
    """Install an override (used by the CLI and tests). Pass ``None`` to clear."""
    global _OVERRIDE
    _OVERRIDE = settings
    _cached.cache_clear()
