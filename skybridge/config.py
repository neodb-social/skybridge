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
#
# Known but deliberately NOT bridged (yet), with observed shapes:
#   social.popfeed.feed.post — LEGACY: popfeed's original "post about a work"
#     (free text + facets + work identifiers, no rating). Last written ~May
#     2025; superseded by feed.review. Existing repos still hold them, but we
#     don't bridge historical content.
#   social.popfeed.feed.reaction — emoji reaction to another popfeed record
#     ({value, subjectUri, subjectType}); would translate to an AP Like /
#     EmojiReact on the bridged note rather than a Note of its own.
#   social.popfeed.challenge.definition — a challenge spec, e.g. a yearly
#     reading goal ({title, description, challenge.readingGoal{startsAt,
#     endsAt, targetBooks, targetPages}}). No per-work activity; nothing to
#     mark on a NeoDB catalog item.
#   social.popfeed.challenge.participation — a user joining a challenge
#     ({title, progress.readingGoalProgress{status, currentBooks,
#     currentPages}, challengeUri -> the definition}). Aggregate progress
#     only, again no per-work activity.
#   social.popfeed.feed.definition — a custom feed spec ({name, description,
#     icon blob, creativeWorkTypes, genres, lists}); app-level curation
#     config, not user activity. (Note: uses creativeWorkTypes like "album"
#     and "ep" — keep translate.works.WORK_TYPE_TO_CATEGORY in sync.)
WANTED_COLLECTIONS: tuple[str, ...] = (
    "social.popfeed.feed.list",
    "social.popfeed.feed.listItem",
    "social.popfeed.feed.review",
    # profile edits refresh the bridged actor's display name/avatar and emit
    # an AP Update(Person); deliberately NOT watching app.bsky.actor.profile
    # on Jetstream (that would stream every profile edit network-wide) — the
    # bsky profile is instead re-fetched as a fallback whenever a popfeed
    # profile event arrives.
    "social.popfeed.actor.profile",
)

# Default public Jetstream endpoint; only the collections above are requested.
DEFAULT_JETSTREAM = "wss://jetstream2.us-east.bsky.network/subscribe"


@dataclass(frozen=True)
class Settings:
    """Immutable settings snapshot, sourced from the environment."""

    domain: str = "localhost:8000"
    scheme: str = "https"
    # All mutable state (SQLite DB, relay key) lives under SKYBRIDGE_DATA.
    # The individual paths below derive from it; only tests set them directly
    # (e.g. db_path=":memory:").
    data_dir: str = "data"
    db_path: str = "data/skybridge.db"
    jetstream_url: str = DEFAULT_JETSTREAM
    wanted_collections: tuple[str, ...] = WANTED_COLLECTIONS
    # Relay actor identity.
    relay_username: str = "relay"
    relay_name: str = "Skybridge"
    # Relay actor signing key: an explicit PEM (SKYBRIDGE_RELAY_KEY) wins;
    # otherwise the PEM file under the data dir, minted on first use, so the
    # secret lives outside the database.
    relay_key_pem: str | None = None
    relay_key_file: str = "data/relay_key.pem"
    relay_summary: str = (
        "Skybridge mirrors activities from Atmosphere (e.g. popfeed) to "
        "the Fediverse in NeoDB-compatible format."
    )
    # External Fediverse relay inboxes we subscribe to as a client (Mastodon-
    # style); empty = pure normal-server mode. See SKYBRIDGE_RELAYS.
    relays: tuple[str, ...] = ()
    # Delivery worker retry schedule (seconds).
    retry_backoff: tuple[int, ...] = (2, 4, 8, 16)
    # neodb-relay (https://github.com/neodb-social/neodb-relay) returns HTTP
    # 418 to any request whose User-Agent lacks "neodb/" — must stay tagged.
    user_agent: str = "skybridge-neodb/0.1 (+activitypub-relay)"
    # "Import recent activity" (user-triggered backfill): fetch at most
    # `backfill_limit` records total from the account's PDS and re-publish
    # only those created within the last `backfill_days` days.
    backfill_limit: int = 1000
    backfill_days: int = 7
    # Optional Sentry DSN: enables error tracking + a per-collection ingest
    # counter metric. Unset (the default) keeps telemetry fully off.
    sentry_dsn: str | None = None

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


def _parse_relays(raw: str) -> tuple[str, ...]:
    """Parse ``SKYBRIDGE_RELAYS``: comma/whitespace-separated, deduped, ordered."""
    return tuple(dict.fromkeys(raw.replace(",", " ").split()))


def _env_int(name: str, default: int, minimum: int) -> int:
    """An integer env var, validated loudly: a malformed or out-of-range
    value must fail at startup, not surface later as a silent no-op."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


# Override holder so tests / the CLI can install a custom settings snapshot.
_OVERRIDE: Settings | None = None


def _from_env() -> Settings:
    domain = os.environ.get("SKYBRIDGE_DOMAIN", "localhost:8000")
    scheme = os.environ.get(
        "SKYBRIDGE_SCHEME", "http" if domain.startswith("localhost") else "https"
    )
    data_dir = os.environ.get("SKYBRIDGE_DATA", "data")
    return Settings(
        domain=domain,
        scheme=scheme,
        data_dir=data_dir,
        db_path=os.path.join(data_dir, "skybridge.db"),
        jetstream_url=os.environ.get("SKYBRIDGE_JETSTREAM", DEFAULT_JETSTREAM),
        relay_key_pem=os.environ.get("SKYBRIDGE_RELAY_KEY") or None,
        relay_key_file=os.path.join(data_dir, "relay_key.pem"),
        relays=_parse_relays(os.environ.get("SKYBRIDGE_RELAYS", "")),
        sentry_dsn=os.environ.get("SKYBRIDGE_SENTRY_DSN") or None,
        backfill_limit=_env_int("SKYBRIDGE_BACKFILL_LIMIT", 1000, minimum=1),
        backfill_days=_env_int("SKYBRIDGE_BACKFILL_DAYS", 7, minimum=0),
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
