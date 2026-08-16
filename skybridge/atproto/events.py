"""Normalise Jetstream events to one internal shape.

The bridge reads the same firehose in three dialects:

* **Jetstream v2** (``.../xrpc/network.bsky.jetstream.subscribeEvents``) — an
  ``{"$type": "message", "payload": {...}}`` envelope whose payload is flat
  (collection/rkey/record sit beside ``did``), timestamped with an ISO-8601
  ``time`` and ordered by a monotonic ``seq``.
* **Jetstream v1** (``wss://jetstream2.../subscribe``) — a flat envelope with
  ``kind``/``time_us``, the commit nested under ``commit``, and no ``seq``.
* **Archive segments** (``.jss`` replay, see :mod:`skybridge.atproto.jss`) —
  the same events, decoded from columnar storage.

:func:`normalize` maps all three onto **v2's shape**, which is what the rest
of the bridge speaks: flat fields, ISO-8601 ``time``, ``seq`` for ordering.
A v1 event is converted *up* — its nested commit is flattened and its
``time_us`` rendered as ISO — so supporting the older transport costs one
adapter here rather than a second vocabulary everywhere downstream.

Note that :func:`_from_v1` is not only for legacy hosts:
``skybridge.atproto.backfill._commit_event`` synthesises v1-shaped commits for
the user-facing "Import recent activity" flow, so it must be converted before
this adapter can be deleted.

``seq`` is the one field v1 cannot supply. It stays ``None`` there, which
disables the staleness guard in :mod:`skybridge.pipeline` — correct, since
without a sequence number there is nothing to order events by.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger("skybridge.events")

# Envelope/payload markers of the v2 lexicon.
_MESSAGE_TYPE = "message"
_PAYLOAD_PREFIX = "network.bsky.jetstream.subscribeEvents#"

# Event kinds we hand the pipeline. "sync" is recognised (so it is not logged
# as unknown) but carries no per-record content, so it normalises to None.
KIND_COMMIT = "commit"
KIND_IDENTITY = "identity"
KIND_ACCOUNT = "account"
KIND_SYNC = "sync"

#: Commit fields copied straight through from a v2 payload. A delete carries
#: no ``record`` or ``cid``, and archive rows carry no ``cid``, so each is
#: forwarded only when present rather than filled with a placeholder.
_COMMIT_FIELDS = ("collection", "rkey", "operation", "record", "cid", "rev")


def micros_to_iso(micros: int | None) -> str | None:
    """Unix microseconds → ISO-8601, for sources that store epoch integers.

    Only v1 events and ``.jss`` rows need this; v2 already sends ISO.
    """
    if not micros:
        return None
    return datetime.fromtimestamp(micros / 1_000_000, tz=UTC).isoformat()


def _from_v1(event: dict[str, Any]) -> dict[str, Any] | None:
    """A v1 event in v2's shape.

    v1 has no ``seq``, so ordering-dependent behaviour is simply unavailable
    on that transport rather than faked from ``time_us`` (which measures a
    different thing and is not comparable across the two).
    """
    kind = event.get("kind")
    if kind not in (KIND_COMMIT, KIND_IDENTITY, KIND_ACCOUNT):
        if kind is not None and kind != KIND_SYNC:
            log.debug("ignoring unknown v1 event kind: %s", kind)
        return None

    out: dict[str, Any] = {
        "kind": kind,
        "did": event.get("did") or "",
        "seq": None,
        "time": micros_to_iso(event.get("time_us")),
    }
    if kind == KIND_COMMIT:
        commit = event.get("commit") or {}
        out["collection"] = commit.get("collection", "")
        out["rkey"] = commit.get("rkey", "")
        out["operation"] = commit.get("operation", "create")
        for key in ("record", "cid", "rev"):
            if (value := commit.get(key)) is not None:
                out[key] = value
    else:
        out[kind] = event.get(kind) or {}
    return out


def _from_v2(payload: dict[str, Any], kind: str) -> dict[str, Any] | None:
    """A v2 payload with the envelope stripped, keys left as they are."""
    out: dict[str, Any] = {
        "kind": kind,
        "did": payload.get("did") or "",
        "seq": payload.get("seq"),
        "time": payload.get("time"),
    }
    if kind == KIND_COMMIT:
        out["collection"] = payload.get("collection", "")
        out["rkey"] = payload.get("rkey", "")
        out["operation"] = payload.get("operation", "create")
        for key in ("record", "cid", "rev"):
            if (value := payload.get(key)) is not None:
                out[key] = value
    else:
        # identity/account nest their detail under a field named after the
        # kind; forwarded verbatim for the pipeline to read.
        out[kind] = payload.get(kind) or {}
    return out


def normalize(event: dict[str, Any]) -> dict[str, Any] | None:
    """Return *event* in the internal (v2) shape, or ``None`` to ignore it.

    Already-normalised events (the ``.jss`` decoder emits them directly) pass
    through untouched. Anything unrecognised returns ``None`` so a future
    event kind is skipped rather than misread as a commit.
    """
    if event.get("$type") == _MESSAGE_TYPE:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return None
        ptype = payload.get("$type", "")
        if not ptype.startswith(_PAYLOAD_PREFIX):
            return None
        kind = ptype[len(_PAYLOAD_PREFIX) :]
        if kind == KIND_SYNC:
            # Repo divergence marker: no record content to bridge. Dropped
            # here rather than in the pipeline so it never counts as unknown.
            return None
        if kind not in (KIND_COMMIT, KIND_IDENTITY, KIND_ACCOUNT):
            log.debug("ignoring unknown jetstream event kind: %s", kind)
            return None
        return _from_v2(payload, kind)

    if "commit" in event or "time_us" in event:
        # v1 wire format: nested commit and/or epoch-microsecond timestamp.
        return _from_v1(event)

    # Already flat and v2-shaped (the archive decoder builds these directly).
    return event if event.get("kind") in (KIND_COMMIT, KIND_IDENTITY, KIND_ACCOUNT) else None
