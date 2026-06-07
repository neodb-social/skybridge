"""Replay captured Jetstream events from a JSONL fixture through the pipeline.

Used by tests and the ``replay`` CLI subcommand so the whole bridge can run
fully offline against real, captured popfeed records.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from skybridge.activitypub.delivery import DeliveryWorker
from skybridge.pipeline import Processed, process_event


def read_events(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


async def replay_file(
    path: str | Path,
    *,
    worker: DeliveryWorker | None = None,
    allow_network: bool = False,
) -> list[Processed]:
    """Replay every event in ``path``; returns the processed (non-filtered) ones."""
    results: list[Processed] = []
    for event in read_events(path):
        processed = await process_event(event, worker=worker, allow_network=allow_network)
        if processed is not None:
            results.append(processed)
    return results
