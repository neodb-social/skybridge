"""Adapter for teal.fm (``fm.teal.*``) records.

teal.fm (https://github.com/teal-fm/teal) is a music scrobbler on the AT
Protocol. One ``fm.teal.feed.play`` record is written for every track a user
listens to; it names the track, the artists and the release (album) and, when
the tracker could match them, carries MusicBrainz ids in the form
``mbid:<uuid>``. There is no rating, no review text and no shelf status.

Two NSIDs are live on the network: ``fm.teal.feed.play`` and the pre-July-2026
``fm.teal.alpha.feed.play``, which older trackers still write. The record
shapes are the same for every field bridged here, so both are handled by this
one adapter.

The bridged *work* is the release, mapped to a NeoDB ``Album``. NeoDB has no
track item type, so a play that names no release (only a recording id, or
nothing at all) has nothing to mark and mints no work. Plays are bridged as ONE
Note per listening session of (author, release) rather than one per play — see
``pipeline._process_play``; this module only knows the record shape.
"""

from __future__ import annotations

import re
from typing import Any

PLAY_COLLECTION = "fm.teal.feed.play"
ALPHA_PLAY_COLLECTION = "fm.teal.alpha.feed.play"
# Both are bridged; the alpha namespace is what pre-July-2026 trackers write.
# Other teal collections are deliberately skipped:
#   fm.teal.actor.status (+alpha) — "now playing": rkey `self`, rewritten on
#     every track and expiring ten minutes later. Ephemeral, not a mark.
#   fm.teal.actor.profile — display name/avatar (avatar is a PDS blob); the
#     bridged actor's identity comes from the bsky profile fallback instead.
#   fm.teal.actor.profileStatus — onboarding progress, not content.
PLAY_COLLECTIONS: frozenset[str] = frozenset({PLAY_COLLECTION, ALPHA_PLAY_COLLECTION})

# The generic creativeWorkType the release is minted as: AP type Album,
# NeoDB category music (see works.WORK_TYPE_TO_AP_TYPE).
WORK_TYPE = "music"

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# An Apple Music track URL names its album too: .../album/<slug>/<albumId>?i=<trackId>.
# NeoDB's apple_music site resolves the same shapes, so the album id is a
# usable identity for a play the tracker could not match on MusicBrainz.
_APPLE_ALBUM = re.compile(
    r"^https?://music\.apple\.com/(?:[a-z]{2}/)?album/(?:[^/?#]+/)?(\d+)(?:[/?#]|$)"
)


def is_play(record: dict[str, Any]) -> bool:
    """Whether *record* is a teal play (either NSID), keyed on ``$type``."""
    return record.get("$type") in PLAY_COLLECTIONS


def mbid(value: Any) -> str | None:
    """A bare lowercase MusicBrainz UUID from a teal ``mbid:<uuid>`` value.

    Accepts a bare UUID too. Anything else (empty, malformed, a URL) yields
    ``None`` rather than a garbage identifier that could never merge.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.lower().startswith("mbid:"):
        text = text[5:]
    text = text.lower()
    return text if _UUID.match(text) else None


def apple_album_id(uri: Any) -> str | None:
    """The Apple Music album id named by an ``originUri``, if any."""
    if not isinstance(uri, str):
        return None
    match = _APPLE_ALBUM.match(uri.strip())
    return match.group(1) if match else None


def artist_names(record: dict[str, Any]) -> list[str]:
    """Artist names in credit order.

    Reads the ``artists`` refs and falls back to the deprecated flat
    ``artistNames`` array that alpha-era records may still carry.
    """
    names: list[str] = []
    artists = record.get("artists")
    if isinstance(artists, list):
        for artist in artists:
            name = artist.get("artistName") if isinstance(artist, dict) else None
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    if not names:
        flat = record.get("artistNames")
        if isinstance(flat, list):
            names = [n.strip() for n in flat if isinstance(n, str) and n.strip()]
    return names


def release_title(record: dict[str, Any]) -> str | None:
    """The release (album) name, or ``None``. Never the track name: the work
    is the album, and labelling it with one track would misname it."""
    name = record.get("releaseName")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _identifiers(record: dict[str, Any]) -> dict[str, str]:
    """Identifying keys for the *release*, in the generic identifier vocabulary.

    ``recordingMbId``/``trackMbId``/``isrc`` identify the track, not the album,
    and are left out on purpose: works.mint would otherwise fall back to them
    and mint an "Album" that is really one recording.
    """
    ids: dict[str, str] = {}
    release = mbid(record.get("releaseMbId"))
    if release:
        ids["mbReleaseId"] = release
    apple = apple_album_id(record.get("originUri"))
    if apple:
        ids["appleMusicAlbumId"] = apple
    return ids


def as_work_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a play into the generic shape ``works.mint`` expects.

    Returns a ``music`` work named after the release. With no release
    identifier the identifiers are empty and no work mints, which the pipeline
    treats as archive-only.

    Idempotent: ``works`` normalizes on every entry point, so the result (which
    keeps the play's ``$type``) can be handed back in. A raw play never carries
    ``identifiers``/``creativeWorkType``, so their presence marks a normalized one.
    """
    if "identifiers" in record and "creativeWorkType" in record:
        return record
    return {
        "$type": record.get("$type") or PLAY_COLLECTION,
        "creativeWorkType": WORK_TYPE,
        "title": release_title(record),
        "identifiers": _identifiers(record),
    }
