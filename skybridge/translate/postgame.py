"""Adapter for Postgame (``at.postgame.*``) records.

Postgame (https://postgame.at) is a video game backlog tracker on AT Protocol
with its own lexicons (published under did:plc:crwol3wvv2w2lvvognhvd5cm). Like
a BookHive book, one ``at.postgame.game`` record per (user, game) carries the
collection status, the rating and a free-text note together, and Postgame
edits it in place as the status changes. It therefore bridges to ONE AP
``Note`` (Status + Rating + Comment) through the pipeline's simple, non-paired
translate path.

Postgame reads Popfeed's game records and can "import" one into a new
``at.postgame.game`` record, leaving the Popfeed record untouched. Both carry
the IGDB id, so the two mint the same catalog work, but each record keeps its
own Note: Postgame is never folded into a Popfeed review/listItem pair.

This module isolates the Postgame specifics: the collection name, its status
vocabulary, and normalizing a game record into the generic work shape
:func:`skybridge.translate.works.mint` consumes. The identifier names match
popfeed's game identifiers (``igdbId`` + ``slug``), so a Postgame game and a
popfeed game merge into one catalog entry, and the slug gives NeoDB its IGDB
URL (see works.external_resource_urls).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

# The only Postgame collection we bridge. Others are deliberately skipped:
#   at.postgame.list / at.postgame.list.item — collection membership, like
#     popfeed's status-less lists; NeoDB Collections are not bridged yet.
#   at.postgame.love — a "loved" flag with no shelf status of its own.
#   at.postgame.follow / at.postgame.settings — social graph and preferences.
GAME_COLLECTION = "at.postgame.game"

WORK_TYPE = "video_game"

# Collection status -> NeoDB shelf status. The canonical values are playing /
# wishlisted / backlogged / played; the rest are legacy values the lexicon
# says only older records carry ('started' -> playing, 'wishlist' ->
# wishlisted, 'shelved' -> backlogged+shelved, 'finished'/'abandoned' ->
# played with that playedStatus). A backlogged game is one the user means to
# play, which NeoDB calls wishlist; 'played' is refined by playedStatus.
_STATUS_MAP = {
    "playing": "progress",
    "started": "progress",
    "wishlisted": "wishlist",
    "wishlist": "wishlist",
    "backlogged": "wishlist",
    "shelved": "dropped",
    "finished": "complete",
    "abandoned": "dropped",
}

# How a played game concluded -> NeoDB shelf status. 'shelved' is legacy. A
# played game with no (or an unknown) playedStatus is still complete.
_PLAYED_MAP = {
    "completed": "complete",
    "mastered": "complete",
    "retired": "complete",
    "abandoned": "dropped",
    "shelved": "dropped",
}


def is_game(record: dict[str, Any]) -> bool:
    """Whether *record* is a raw ``at.postgame.game`` record.

    Requires the nested ``game`` ref: the output of :func:`as_work_record`
    keeps the ``$type`` but not the ref, and works.mint normalizes a record
    twice (mint, then work_ref), so the second pass must leave it alone.
    """
    return record.get("$type") == GAME_COLLECTION and isinstance(record.get("game"), dict)


def _token(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def shelf_status(record: dict[str, Any]) -> str | None:
    """Map a game record's status to a NeoDB shelf status, or ``None``.

    ``backloggedStatus: shelved`` means the game was started and is paused;
    it maps to dropped, as popfeed's "shelved" lists do (neodb._LIST_STATUS).
    """
    status = _token(record.get("status"))
    if status == "played":
        return _PLAYED_MAP.get(_token(record.get("playedStatus")), "complete")
    if status == "backlogged" and _token(record.get("backloggedStatus")) == "shelved":
        return "dropped"
    return _STATUS_MAP.get(status)


def rating(record: dict[str, Any]) -> int | None:
    """The rating on NeoDB's 1-10 scale, or ``None``.

    Postgame stores stars x 2 (7 = 3.5 stars), which is already that scale;
    anything outside it is no rating at all.
    """
    value = record.get("rating")
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 10:
        return None
    return value


def notes(record: dict[str, Any]) -> str:
    """The free-text note: a review on a played game, a library note on others."""
    value = record.get("notes")
    return value.strip() if isinstance(value, str) else ""


def _game(record: dict[str, Any]) -> dict[str, Any]:
    game = record.get("game")
    return game if isinstance(game, dict) else {}


def _igdb_slug(url: Any) -> str | None:
    """The slug of an IGDB game URL (``https://www.igdb.com/games/<slug>``)."""
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    if not parsed.netloc.endswith("igdb.com"):
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) == 2 and parts[0] == "games":
        return parts[1]
    return None


def _identifiers(record: dict[str, Any]) -> dict[str, str]:
    game = _game(record)
    ids: dict[str, str] = {}
    igdb_id = game.get("igdbId")
    if isinstance(igdb_id, int | str) and not isinstance(igdb_id, bool) and str(igdb_id):
        ids["igdbId"] = str(igdb_id)
    slug = _igdb_slug(game.get("igdbUrl"))
    if slug:
        ids["slug"] = slug
    return ids


def title(record: dict[str, Any]) -> str | None:
    value = _game(record).get("title")
    return value if isinstance(value, str) and value else None


def as_work_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a game record into the generic shape ``works.mint`` expects.

    The game ref carries the IGDB id (the work's identity), the IGDB URL (for
    its slug), the title and the cover URL. A game with no IGDB id mints no
    work; like a popfeed review without identifiers, its Note then names the
    game but carries no catalog tag, so NeoDB peers ignore it.
    """
    work: dict[str, Any] = {
        "$type": GAME_COLLECTION,
        "creativeWorkType": WORK_TYPE,
        "title": title(record),
        "identifiers": _identifiers(record),
    }
    cover = _game(record).get("coverUrl")
    if isinstance(cover, str) and cover:
        work["posterUrl"] = cover
    return work
