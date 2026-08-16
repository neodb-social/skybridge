"""The .jss archive segment decoder."""

from __future__ import annotations

import json
import struct

import pytest
import zstandard
from skybridge.atproto import jss

from tests.conftest import FIXTURES

BLOCK = FIXTURES / "jetstream_segment_block.jss.zst"


def _block(rows: list[dict]) -> bytes:
    """Encode rows in the .jss columnar block layout, for cases the captured
    fixture can't cover (deletes and updates are rare in a single real block)."""
    n = len(rows)
    out = struct.pack("<I", n)
    out += struct.pack(f"<{n}Q", *[r["seq"] for r in rows])
    out += struct.pack(f"<{n}q", *[r.get("witnessed", 0) for r in rows])
    out += struct.pack(f"<{n}q", *[0] * n)
    out += struct.pack(f"<{n}B", *[r["kind"] for r in rows])
    parts = {k: [r.get(k, b"") for r in rows] for k in ("collection", "did", "rkey", "rev")}
    payloads = [r.get("payload", b"") for r in rows]
    out += struct.pack(f"<{n}B", *[len(v) for v in parts["collection"]])
    out += struct.pack(f"<{n}H", *[len(v) for v in parts["did"]])
    out += struct.pack(f"<{n}B", *[len(v) for v in parts["rkey"]])
    out += struct.pack(f"<{n}B", *[len(v) for v in parts["rev"]])
    out += struct.pack(f"<{n}I", *[len(v) for v in payloads])
    for key in ("collection", "did", "rkey", "rev"):
        out += b"".join(parts[key])
    out += b"".join(payloads)
    return zstandard.ZstdCompressor().compress(out)


def test_decodes_a_real_captured_block():
    events = jss.decode_block(BLOCK.read_bytes())
    assert events
    for event in events:
        assert event["kind"] == "commit"
        assert event["did"].startswith("did:")
        assert isinstance(event["seq"], int)
        assert event["collection"]


def test_create_resync_rows_are_treated_as_creates():
    """Kind 7 dominates real archive data; mapping only 1/2/3 would drop it."""
    events = jss.decode_block(BLOCK.read_bytes())
    assert any(e["operation"] == "create" for e in events)


def test_non_commit_rows_are_skipped():
    """The fixture holds a sync row alongside the commits."""
    raw = zstandard.ZstdDecompressor().decompress(BLOCK.read_bytes(), max_output_size=1 << 20)
    (rows,) = struct.unpack_from("<I", raw, 0)
    assert len(jss.decode_block(BLOCK.read_bytes())) < rows


def test_records_are_json_serializable_with_cids_as_links():
    """DAG-CBOR links decode to bytes; the live stream sends {"$link": ...}."""
    events = jss.decode_block(BLOCK.read_bytes())
    links = []

    def walk(value):
        if isinstance(value, dict):
            if set(value) == {"$link"}:
                links.append(value["$link"])
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for event in events:
        record = event.get("record")
        json.dumps(record)  # must not raise
        walk(record)
    assert links, "fixture includes a blob reference"
    assert all(cid.startswith("baf") for cid in links)


def test_all_commit_kinds_map_to_operations():
    rows = [
        {"seq": 1, "kind": jss.KIND_CREATE, "collection": b"c", "did": b"did:plc:a", "rkey": b"r1"},
        {"seq": 2, "kind": jss.KIND_UPDATE, "collection": b"c", "did": b"did:plc:a", "rkey": b"r2"},
        {"seq": 3, "kind": jss.KIND_DELETE, "collection": b"c", "did": b"did:plc:a", "rkey": b"r3"},
        {"seq": 4, "kind": jss.KIND_IDENTITY, "did": b"did:plc:a"},
        {"seq": 5, "kind": jss.KIND_ACCOUNT, "did": b"did:plc:a"},
        {"seq": 6, "kind": jss.KIND_SYNC, "did": b"did:plc:a"},
        {
            "seq": 7,
            "kind": jss.KIND_CREATE_RESYNC,
            "collection": b"c",
            "did": b"did:plc:a",
            "rkey": b"r7",
        },
    ]
    events = jss.decode_block(_block(rows))
    assert [(e["seq"], e["operation"]) for e in events] == [
        (1, "create"),
        (2, "update"),
        (3, "delete"),
        (7, "create"),
    ]


def test_a_delete_carries_no_record():
    rows = [
        {"seq": 1, "kind": jss.KIND_DELETE, "collection": b"c", "did": b"did:plc:a", "rkey": b"r"}
    ]
    (event,) = jss.decode_block(_block(rows))
    assert "record" not in event


def test_truncated_block_is_rejected_rather_than_partially_decoded():
    raw = zstandard.ZstdDecompressor().decompress(BLOCK.read_bytes(), max_output_size=1 << 20)
    truncated = zstandard.ZstdCompressor().compress(raw[:-8])
    with pytest.raises((ValueError, struct.error)):
        jss.decode_block(truncated)


def test_segment_file_must_have_the_jss_magic():
    with pytest.raises(ValueError):
        list(jss.iter_segment(b"\x00" * 300))


def test_iter_segment_reads_length_prefixed_blocks():
    frame = _block(
        [{"seq": 1, "kind": jss.KIND_CREATE, "collection": b"c", "did": b"did:plc:a", "rkey": b"r"}]
    )
    header = bytearray(jss.MAGIC + b"\x00" * (jss.HEADER_SIZE - 4))
    body = struct.pack("<Q", len(frame)) + frame
    struct.pack_into("<Q", header, 58, jss.HEADER_SIZE + len(body))  # footer_offset
    events = list(jss.iter_segment(bytes(header) + body))
    assert [e["seq"] for e in events] == [1]
