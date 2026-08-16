"""Decoder for Jetstream sealed segment (``.jss``) archive files.

Jetstream v2 keeps the whole network on disk as a series of sealed segments and
serves them over HTTP for Network Replay (see
:mod:`skybridge.atproto.archive`). There is no Python SDK — the official
clients are TypeScript and Go — so the format is decoded here.

Layout, per the Jetstream repository's data-layout docs:

* a 256-byte file header (magic ``jss0``, counts, sequence bounds, footer
  offsets), followed by blocks, followed by a footer written at seal time;
* each block is ``[uint64 LE compressed_size][zstd frame]``;
* inside a decompressed block: a ``uint32`` event count, then fixed-width
  columns (one entry per event), then the variable-length columns concatenated
  in order, ending with each event's raw DAG-CBOR payload.

Everything is little-endian. ``getBlock`` serves a bare zstd frame — the
length prefix belongs to the file layout only — so :func:`decode_block` takes
the frame and :func:`iter_segment` handles whole files.
"""

from __future__ import annotations

import logging
import struct
from base64 import b64encode
from collections.abc import Iterator
from typing import Any

import zstandard
from libipld import decode_cid, decode_dag_cbor, encode_cid

from skybridge.atproto.events import micros_to_iso

log = logging.getLogger("skybridge.jss")

HEADER_SIZE = 256
MAGIC = b"jss0"

# Guard against a corrupt length prefix asking us to allocate absurd memory.
# Blocks are ~1 MB decompressed in practice; segments seal at ~256 MB.
_MAX_BLOCK_BYTES = 512 * 1024 * 1024

# Row kinds, from the Jetstream source (segment/event.go). KindCreateResync is
# a create materialised during a repo resync rather than observed live: it is
# an ordinary create for our purposes, and it dominates real archive data, so
# treating only 1/2/3 as commits would silently discard most of the archive.
KIND_CREATE = 1
KIND_UPDATE = 2
KIND_DELETE = 3
KIND_IDENTITY = 4
KIND_ACCOUNT = 5
KIND_SYNC = 6
KIND_CREATE_RESYNC = 7

_COMMIT_OPERATIONS = {
    KIND_CREATE: "create",
    KIND_UPDATE: "update",
    KIND_DELETE: "delete",
    KIND_CREATE_RESYNC: "create",
}


def _read_column(buf: bytes, offset: int, fmt: str, size: int, count: int) -> tuple[list, int]:
    """Read ``count`` little-endian values of ``fmt``; returns (values, offset)."""
    values = list(struct.unpack_from(f"<{count}{fmt}", buf, offset))
    return values, offset + size * count


def _read_strings(buf: bytes, offset: int, lengths: list[int]) -> tuple[list[str], int]:
    out: list[str] = []
    for length in lengths:
        out.append(buf[offset : offset + length].decode("utf-8", "replace"))
        offset += length
    return out, offset


def _json_safe(value: Any) -> Any:
    """Convert decoded DAG-CBOR into the JSON shapes the live stream sends.

    Segments store each event's *raw CBOR*, where a link is a binary CID and
    the JSON firehose would have rendered it as DAG-JSON's ``{"$link": "bafy…"}``
    (this is how blob references arrive live). Without this an archived record
    carries raw ``bytes``, which neither matches a live record nor survives the
    ``json.dumps`` that persists it.
    """
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, bytes):
        try:
            decode_cid(value)
        except Exception:
            # Not a CID: DAG-JSON renders opaque bytes as base64.
            return {"$bytes": b64encode(value).decode("ascii")}
        return {"$link": encode_cid(value)}
    return value


def decode_block(frame: bytes) -> list[dict[str, Any]]:
    """Decode one zstd block frame into normalised event dicts.

    The dicts match what :func:`skybridge.atproto.events.normalize` produces
    for a live v2 event, so the pipeline cannot tell an archived event from a
    live one apart from the ``from_archive`` flag its caller passes.
    """
    buf = zstandard.ZstdDecompressor().decompress(frame, max_output_size=_MAX_BLOCK_BYTES)
    (count,) = struct.unpack_from("<I", buf, 0)
    offset = 4

    seqs, offset = _read_column(buf, offset, "Q", 8, count)
    witnessed, offset = _read_column(buf, offset, "q", 8, count)
    _indexed, offset = _read_column(buf, offset, "q", 8, count)
    kinds, offset = _read_column(buf, offset, "B", 1, count)
    collection_lens, offset = _read_column(buf, offset, "B", 1, count)
    did_lens, offset = _read_column(buf, offset, "H", 2, count)
    rkey_lens, offset = _read_column(buf, offset, "B", 1, count)
    rev_lens, offset = _read_column(buf, offset, "B", 1, count)
    payload_lens, offset = _read_column(buf, offset, "I", 4, count)

    collections, offset = _read_strings(buf, offset, collection_lens)
    dids, offset = _read_strings(buf, offset, did_lens)
    rkeys, offset = _read_strings(buf, offset, rkey_lens)
    revs, offset = _read_strings(buf, offset, rev_lens)

    events: list[dict[str, Any]] = []
    for i in range(count):
        payload = buf[offset : offset + payload_lens[i]]
        offset += payload_lens[i]
        event = _build_event(
            kind=kinds[i],
            seq=seqs[i],
            witnessed_at=witnessed[i],
            collection=collections[i],
            did=dids[i],
            rkey=rkeys[i],
            rev=revs[i],
            payload=payload,
        )
        if event is not None:
            events.append(event)

    if offset != len(buf):
        # Every byte is accounted for by the column widths, so a mismatch means
        # the layout was misread — surface it instead of yielding partial data.
        raise ValueError(f"jss block: consumed {offset} of {len(buf)} bytes")
    return events


def _build_event(
    *,
    kind: int,
    seq: int,
    witnessed_at: int,
    collection: str,
    did: str,
    rkey: str,
    rev: str,
    payload: bytes,
) -> dict[str, Any] | None:
    """One decoded row → the pipeline's internal event shape, or ``None``.

    Only commits carry per-record content. ``identity``/``account``/``sync``
    rows are dropped: replaying historical lifecycle transitions would apply a
    long-superseded account state on top of current truth, and the live tail
    already delivers the current one.
    """
    operation = _COMMIT_OPERATIONS.get(kind)
    if operation is None:
        return None

    record: dict[str, Any] | None = None
    if operation != "delete" and payload:
        try:
            record = _json_safe(decode_dag_cbor(payload))
        except Exception as exc:
            log.warning("jss: undecodable CBOR at seq %s (%s)", seq, type(exc).__name__)
            return None

    event: dict[str, Any] = {
        "kind": "commit",
        "did": did,
        "seq": seq,
        # Segments store epoch microseconds; the rest of the bridge speaks
        # v2's ISO-8601, so the conversion happens once, here at the source.
        "time": micros_to_iso(witnessed_at),
        "collection": collection,
        "rkey": rkey,
        "operation": operation,
    }
    if record is not None:
        event["record"] = record
    if rev:
        event["rev"] = rev
    return event


#: Byte offsets of the header fields we read (all little-endian).
_OFF_COLLECTION_INDEX = 82
_COLLECTION_INDEX_HEADER = 16  # 4 x uint32, stored uncompressed


def collection_index_offset(header: bytes) -> int:
    """Where the segment's collection index starts, from its 256-byte header."""
    if len(header) < HEADER_SIZE or header[:4] != MAGIC:
        raise ValueError("not a jss segment header")
    return struct.unpack_from("<Q", header, _OFF_COLLECTION_INDEX)[0]


def blocks_for_collections(index_region: bytes, wanted: set[str]) -> list[int]:
    """Block indices that contain any of *wanted*, from the collection index.

    This is what makes a whole-segment plan entry affordable. A segment is
    ~250 MB, but its collection index is a few KB and says exactly which
    blocks hold which NSIDs — so a rare collection can be pulled with a
    handful of ``getBlock`` calls instead of downloading the file. (Measured:
    one segment cost 242 MiB for 11 BookHive records; its index is 7 KB.)

    Layout: a 16-byte uncompressed header (collection_count, block_count,
    bitmask_len, uncompressed_size), then one zstd frame holding a string
    table of ``[len u8][count u32][nsid]`` entries followed by
    ``block_count`` bitmasks of ``bitmask_len`` bytes. Bit *n* of a block's
    mask marks the *n*-th collection in table order.
    """
    collection_count, block_count, bitmask_len, _size = struct.unpack_from("<4I", index_region, 0)
    body = zstandard.ZstdDecompressor().decompress(
        index_region[_COLLECTION_INDEX_HEADER:], max_output_size=_MAX_BLOCK_BYTES
    )

    offset = 0
    ids: list[int] = []
    for index in range(collection_count):
        (name_len,) = struct.unpack_from("<B", body, offset)
        offset += 1 + 4  # length byte + the per-collection event count
        nsid = body[offset : offset + name_len].decode("utf-8", "replace")
        offset += name_len
        if nsid in wanted:
            ids.append(index)
    if not ids:
        return []

    needed = block_count * bitmask_len
    masks = body[offset : offset + needed]
    if len(masks) != needed:
        # Report the format problem rather than letting an IndexError surface
        # from the scan below — the caller treats any error as "unreadable"
        # and falls back to a ~250 MB whole-segment download.
        raise ValueError(
            f"jss collection index: bitmask table is {len(masks)} bytes, expected {needed}"
        )
    hits: list[int] = []
    for block in range(block_count):
        base = block * bitmask_len
        if any(masks[base + (i >> 3)] & (1 << (i & 7)) for i in ids):
            hits.append(block)
    return hits


def iter_segment(data: bytes) -> Iterator[dict[str, Any]]:
    """Decode a whole ``.jss`` file (``mode: "segment"`` downloads).

    Blocks run from the end of the header to ``footer_offset``; the footer
    holds indexes and bloom filters we don't need, since the caller re-applies
    its own exact filters to every decoded row anyway.
    """
    if len(data) < HEADER_SIZE or data[:4] != MAGIC:
        raise ValueError("not a jss segment file")
    (footer_offset,) = struct.unpack_from("<Q", data, 58)
    end = footer_offset if 0 < footer_offset <= len(data) else len(data)

    offset = HEADER_SIZE
    while offset + 8 <= end:
        (size,) = struct.unpack_from("<Q", data, offset)
        offset += 8
        if size == 0 or size > _MAX_BLOCK_BYTES or offset + size > end:
            break
        yield from decode_block(data[offset : offset + size])
        offset += size
