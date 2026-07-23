"""Mint / look up catalog "work" objects for NeoDB ``withRegardTo`` links.

A work is identified by an external id from the popfeed record
(``imdbId`` / ``tmdbId`` / ``igdbId``) plus its ``creativeWorkType``. We mint a
stable, dereferenceable URL on our own domain so NeoDB instances can resolve
the catalog item a mark/review refers to.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from sqlalchemy.orm import Session

from skybridge.config import get_settings
from skybridge.db import session_scope
from skybridge.models import Work, WorkIdentifier
from skybridge.translate import bookhive

# popfeed creativeWorkType -> NeoDB catalog category.
WORK_TYPE_TO_CATEGORY: dict[str, str] = {
    "movie": "movie",
    "tv_show": "tv",
    "tv_season": "tv",
    "tv_episode": "tv",
    "video_game": "game",
    "book": "book",
    "music": "music",
    "album": "music",
    "ep": "music",
    "podcast": "podcast",
}

# popfeed creativeWorkType -> NeoDB catalog item AP type. This is the `type`
# NeoDB peers require both on the Note's work tag and on the dereferenced
# catalog object (neodb takahe/ap_handlers._supported_ap_catalog_item_types
# and catalog/sites/fedi.py supported_types); anything else is dropped.
WORK_TYPE_TO_AP_TYPE: dict[str, str] = {
    "movie": "Movie",
    "tv_show": "TVShow",
    "tv_season": "TVSeason",
    "video_game": "Game",
    "book": "Edition",
    "music": "Album",
    "album": "Album",
    "ep": "Album",
    "podcast": "Podcast",
}


def ap_type_for(work_type: str) -> str | None:
    return WORK_TYPE_TO_AP_TYPE.get(work_type)


def external_resource_urls(work_type: str, identifiers: dict) -> list[str]:
    """Canonical external-site URLs for the work's identifiers.

    NeoDB resolves these against its catalog site URL patterns
    (catalog/sites/{imdb,tmdb,igdb,steam,musicbrainz}.py) to merge our work
    with an already-known catalog item instead of minting a duplicate.
    """
    urls: list[str] = []
    imdb = identifiers.get("imdbId")
    if imdb and str(imdb).startswith("tt"):
        urls.append(f"https://www.imdb.com/title/{imdb}")
    tmdb = identifiers.get("tmdbId")
    if tmdb and work_type == "movie":
        urls.append(f"https://www.themoviedb.org/movie/{tmdb}")
    elif tmdb and work_type == "tv_show":
        urls.append(f"https://www.themoviedb.org/tv/{tmdb}")
    series = identifiers.get("tmdbTvSeriesId")
    season = identifiers.get("seasonNumber")
    if work_type == "tv_season" and series and season is not None:
        urls.append(f"https://www.themoviedb.org/tv/{series}/season/{season}")
    slug = identifiers.get("slug")
    if slug and work_type == "video_game":
        # IGDB urls are slug-based; the numeric igdbId is not resolvable.
        urls.append(f"https://www.igdb.com/games/{slug}")
    goodreads = identifiers.get("goodreadsId")
    if goodreads and work_type == "book":
        # NeoDB resolves Goodreads book URLs (catalog/sites/goodreads.py).
        urls.append(f"https://www.goodreads.com/book/show/{goodreads}")
    steam = identifiers.get("steamId")
    if steam:
        urls.append(f"https://store.steampowered.com/app/{steam}")
    if identifiers.get("mbId"):
        urls.append(f"https://musicbrainz.org/release-group/{identifiers['mbId']}")
    if identifiers.get("mbReleaseId"):
        urls.append(f"https://musicbrainz.org/release/{identifiers['mbReleaseId']}")
    return urls


# Preferred identifier per work type (first match wins), then any remaining.
# isbn13 over isbn10 (canonical form); mbId (musicbrainz release group, stable
# across pressings) over mbReleaseId. BookHive's goodreadsId/hiveId rank after
# the ISBNs so a book still merges with popfeed/NeoDB editions by ISBN first,
# falling back to hiveId (always present on a BookHive book) when it has none.
_ID_PRIORITY = (
    "imdbId",
    "tmdbId",
    "igdbId",
    "steamId",
    "isbn",
    "isbn13",
    "isbn10",
    "goodreadsId",
    "musicbrainzId",
    "mbId",
    "mbReleaseId",
    "hiveId",
)

# Keys that describe a work's position or parent, not its identity. Two shows'
# "episode 4" share episodeNumber=4, every episode of a series shares its
# tmdbTvSeriesId — so these must never key a work or act as a merge alias
# (doing so is exactly how distinct episodes were once collapsed into one
# catalog entry). They still ride along in Work.identifiers_json as metadata
# (external_resource_urls builds season URLs from them).
_NON_IDENTIFYING = frozenset({"episodeNumber", "seasonNumber", "tmdbTvSeriesId"})

EPISODE_TYPE = "tv_episode"
SEASON_TYPE = "tv_season"


def _stringified(identifiers: dict) -> dict[str, str]:
    return {str(k): str(v) for k, v in identifiers.items() if v not in (None, "")}


def _season_compound(work_type: str, ids: dict[str, str]) -> tuple[str, str] | None:
    """The derived compound identity of a season: ``tmdbId-<series>-season-<n>``.

    A season is globally identified by (series, seasonNumber) even when the
    record carries no season-level tmdbId (e.g. a season ref derived from an
    episode, see season_view). The compound rides in the ``tmdbId`` namespace
    (real season ids are purely numeric, so the forms can't collide) and is
    registered as an alias on every season work, so both spellings merge into
    one work whichever arrives first.
    """
    if work_type != SEASON_TYPE:
        return None
    series = ids.get("tmdbTvSeriesId")
    season = ids.get("seasonNumber")
    if series and season:
        return ("tmdbId", f"{series}-season-{season}")
    return None


@dataclass
class WorkRef:
    work_key: str
    work_type: str  # popfeed creativeWorkType
    work_id: str
    url: str
    title: str | None = None
    poster_url: str | None = None


def _pick_identifier(identifiers: dict) -> tuple[str, str] | None:
    for key in _ID_PRIORITY:
        if identifiers.get(key):
            return key, str(identifiers[key])
    for key, val in identifiers.items():
        if val and key not in _NON_IDENTIFYING:
            return str(key), str(val)
    return None


# popfeed sometimes labels a show- or season-typed record with the specific
# episode's title ("Baron Noir - S1E5 - Grenelle"). Match "<show> - S<n>E<n>…"
# to recover the show name and season number.
_EPISODE_TITLE = re.compile(r"^(?P<show>.+?)\s+-\s+S(?P<season>\d+)E\d+\b")
# A title that already names its season ("… - Season 1", "Season 1").
_SEASON_TITLE = re.compile(r"\bSeason\s+\d+\b", re.IGNORECASE)


def normalize_title(work_type: str, title: str | None, identifiers: dict) -> str | None:
    """Clean an episode-shaped title down to what the *work* should be named.

    popfeed puts the watched episode's title on show- and season-typed records
    alike. A ``tv_show`` work should carry just the show name; a ``tv_season``
    work should name the season ("<show> - Season <n>"), never a single
    episode. Movies/books/etc. and titles that don't look episodic pass
    through unchanged.
    """
    if not isinstance(title, str) or not title:
        return title
    episode = _EPISODE_TITLE.match(title)
    if work_type == "tv_show":
        return episode.group("show") if episode else title
    if work_type == SEASON_TYPE:
        # The identifiers' seasonNumber is authoritative when present; the
        # S<n> parsed from the title is the fallback.
        season = str(identifiers.get("seasonNumber") or "").strip()
        if episode:
            number = season if season.isdigit() else episode.group("season")
            return f"{episode.group('show')} - Season {int(number)}"
        if _SEASON_TITLE.search(title):
            return title  # already a season label
        if season.isdigit():
            return f"{title} - Season {int(season)}"
    return title


def season_view(record: dict) -> dict | None:
    """A ``tv_episode`` record recast as its parent ``tv_season``.

    NeoDB doesn't federate episode-level marks, so episode list-adds are
    bridged as activity on the season instead (TVSeason is a supported
    catalog type, resolvable via the TMDB season URL). Returns ``None`` when
    the record can't name its season (no series id or season number). The
    episode title is left as-is here; work_ref normalizes it to a season
    title (see normalize_title) once the type is tv_season.
    """
    identifiers = record.get("identifiers") or {}
    series = identifiers.get("tmdbTvSeriesId")
    season = identifiers.get("seasonNumber")
    if not series or season in (None, ""):
        return None
    return {
        **record,
        "creativeWorkType": SEASON_TYPE,
        "identifiers": {"tmdbTvSeriesId": str(series), "seasonNumber": str(season)},
    }


def _effective_record(record: dict) -> dict:
    """The record whose work actually gets minted.

    A BookHive book is normalized to the generic ``book`` work shape (see
    :mod:`skybridge.translate.bookhive`). Episode list-adds become season
    activity (see :func:`season_view`); an episode that can't be resolved to a
    season keeps its own tv_episode work, which the pipeline archives without
    AP emission.
    """
    if bookhive.is_book(record):
        return bookhive.as_work_record(record)
    if record.get("creativeWorkType") == EPISODE_TYPE and str(record.get("$type", "")).endswith(
        "feed.listItem"
    ):
        return season_view(record) or record
    return record


def is_episode_key(work_key: str | None) -> bool:
    return bool(work_key) and work_key.partition(":")[0] == EPISODE_TYPE


def work_ref(record: dict) -> WorkRef | None:
    """Derive a :class:`WorkRef` from a popfeed record, or ``None`` if it has
    no resolvable creative-work identifier."""
    record = _effective_record(record)
    identifiers = _stringified(record.get("identifiers") or {})
    work_type = record.get("creativeWorkType") or "unknown"
    picked = _pick_identifier(identifiers) or _season_compound(work_type, identifiers)
    if picked is None:
        return None
    id_key, id_val = picked
    # Namespacing the id by its source keeps keys unambiguous across providers.
    work_id = f"{id_key}-{id_val}"
    work_key = f"{work_type}:{work_id}"
    settings = get_settings()
    return WorkRef(
        work_key=work_key,
        work_type=work_type,
        work_id=work_id,
        url=settings.catalog_id(work_type, work_id),
        title=normalize_title(work_type, record.get("title"), identifiers),
        poster_url=record.get("posterUrl"),
    )


def _reref(ref: WorkRef, work_key: str) -> WorkRef:
    """Re-point a ref at an already-minted work (same type, different key)."""
    work_id = work_key.split(":", 1)[1]
    settings = get_settings()
    return WorkRef(
        work_key=work_key,
        work_type=ref.work_type,
        work_id=work_id,
        url=settings.catalog_id(ref.work_type, work_id),
        title=ref.title,
        poster_url=ref.poster_url,
    )


def mint(record: dict, *, session: Session | None = None) -> WorkRef | None:
    """Resolve a work ref and upsert its catalog row, returning the ref.

    Every *identifying* identifier the record carries is registered as an
    alias, and an alias hit redirects to the existing work — so records that
    carry different identifier subsets for the same work share one catalog
    entry. Positional keys (episode/season numbers, parent series id) are
    stored as metadata only, never as aliases (see _NON_IDENTIFYING).

    ``session`` joins an existing transaction instead of opening one, so a
    caller minting several records in one transaction sees each new work (and
    its aliases) immediately; passing ``None`` opens a fresh session per call.
    """
    if session is None:
        with session_scope() as own_session:
            return mint(record, session=own_session)
    record = _effective_record(record)
    ref = work_ref(record)
    if ref is None:
        return None
    identifiers = _stringified(record.get("identifiers") or {})
    aliases = [(k, v) for k, v in identifiers.items() if k not in _NON_IDENTIFYING]
    compound = _season_compound(ref.work_type, identifiers)
    if compound is not None and compound not in aliases:
        aliases.append(compound)
    for key, val in aliases:
        alias = session.get(WorkIdentifier, (ref.work_type, key, val))
        if alias is not None:
            if alias.work_key != ref.work_key:
                ref = _reref(ref, alias.work_key)
            break
    row = session.get(Work, ref.work_key)
    if row is None:
        session.add(
            Work(
                work_key=ref.work_key,
                creative_work_type=ref.work_type,
                title=ref.title,
                poster_url=ref.poster_url,
                identifiers_json=json.dumps(identifiers),
            )
        )
        # Make the row (and its aliases below) visible to the next mint()
        # call sharing this session before the transaction commits.
        session.flush()
    else:
        # Backfill metadata we may not have had at first sight.
        if ref.title and not row.title:
            row.title = ref.title
        if ref.poster_url and not row.poster_url:
            row.poster_url = ref.poster_url
        merged = {**json.loads(row.identifiers_json or "{}"), **identifiers}
        row.identifiers_json = json.dumps(merged)
    for key, val in aliases:
        if session.get(WorkIdentifier, (ref.work_type, key, val)) is None:
            session.add(
                WorkIdentifier(
                    creative_work_type=ref.work_type,
                    id_key=key,
                    id_value=val,
                    work_key=ref.work_key,
                )
            )
    session.flush()
    return ref


def category_for(work_type: str) -> str:
    return WORK_TYPE_TO_CATEGORY.get(work_type, "item")
