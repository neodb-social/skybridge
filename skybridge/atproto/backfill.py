"""Optional backfill: seed the bridge from a DID's existing popfeed records.

Uses ``com.atproto.repo.listRecords`` against the author's PDS and feeds each
record through the pipeline as a synthetic ``create`` commit.
"""

from __future__ import annotations

import logging
from typing import Any

from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.atproto.identity import _http_json, resolve_remote
from skybridge.config import get_settings
from skybridge.pipeline import Processed, process_event

log = logging.getLogger("skybridge.backfill")

PLC_DIRECTORY = "https://plc.directory"


def _resolve_pds(did: str) -> str | None:
    doc = _http_json(f"{PLC_DIRECTORY}/{did}")
    if not doc:
        return None
    for svc in doc.get("service", []):
        if svc.get("id") == "#atproto_pds":
            return svc.get("serviceEndpoint")
    return None


def _list_records(pds: str, did: str, collection: str, limit: int = 100) -> list[dict[str, Any]]:
    url = (
        f"{pds}/xrpc/com.atproto.repo.listRecords?repo={did}&collection={collection}&limit={limit}"
    )
    data = _http_json(url) or {}
    return data.get("records", [])


async def backfill_did(
    did: str, *, worker: DeliveryWorker | None = None, limit: int = 100
) -> list[Processed]:
    """Pull a DID's popfeed records and run them through the pipeline."""
    settings = get_settings()
    pds = _resolve_pds(did)
    if pds is None:
        log.warning("could not resolve PDS for %s", did)
        return []
    # Make sure we have an identity row before processing.
    resolve_remote(did)
    results: list[Processed] = []
    for collection in settings.wanted_collections:
        for rec in _list_records(pds, did, collection, limit):
            uri = rec.get("uri", "")
            rkey = uri.rsplit("/", 1)[-1] if uri else ""
            event = {
                "did": did,
                "kind": "commit",
                "commit": {
                    "operation": "create",
                    "collection": collection,
                    "rkey": rkey,
                    "record": rec.get("value", {}),
                    "cid": rec.get("cid"),
                },
            }
            processed = await process_event(event, worker=worker)
            if processed is not None:
                results.append(processed)
    log.info("backfilled %d records for %s", len(results), did)
    return results
