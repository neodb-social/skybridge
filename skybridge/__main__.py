"""Skybridge CLI: ``python -m skybridge {serve|ingest|replay|backfill}``.

All subcommands honour ``SKYBRIDGE_DOMAIN`` (and the other env settings); the
domain is never hardcoded.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from skybridge.config import get_settings
from skybridge.db import init_db


def _cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    init_db()
    log_level = os.getenv("SKYBRIDGE_LOG", "INFO").lower()
    if log_level not in {"critical", "error", "warning", "info", "debug", "trace"}:
        log_level = "info"

    # Access logs are noise outside debug/trace (healthcheck polls /stats constantly)
    access_log = log_level in {"debug", "trace"}

    uvicorn.run(
        "skybridge.main:app",
        host=args.host,
        port=args.port,
        log_level=log_level,
        access_log=access_log,
    )
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.activitypub.relays import reconcile_relays
    from skybridge.atproto.jetstream import run as jetstream_run

    init_db()

    async def _go() -> int:
        worker = DeliveryWorker()
        worker.start()
        try:
            # (Re)send Follows for configured relays; Accepts can only be received
            # by a running `serve` process sharing this DB, not by this one-shot run.
            await reconcile_relays(worker)
            return await jetstream_run(worker, stop_after=args.limit)
        finally:
            await worker.stop()

    n = asyncio.run(_go())
    print(f"processed {n} popfeed event(s)")
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.atproto.replay import replay_file

    init_db(reset=args.reset)

    async def _go() -> int:
        worker = DeliveryWorker() if args.deliver else None
        if worker:
            worker.start()
        try:
            results = await replay_file(args.path, worker=worker, allow_network=args.network)
            return len(results)
        finally:
            if worker:
                await worker.stop()

    n = asyncio.run(_go())
    print(f"replayed {n} popfeed record(s) from {args.path}")
    return 0


def _cmd_backfill(args: argparse.Namespace) -> int:
    from datetime import UTC, datetime, timedelta

    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.atproto.backfill import backfill_did

    init_db()
    # `is not None`: --days 0 means the narrowest window, not "no window".
    since = datetime.now(UTC) - timedelta(days=args.days) if args.days is not None else None

    async def _go() -> int:
        worker = DeliveryWorker() if args.deliver else None
        if worker:
            worker.start()
        try:
            results = await backfill_did(args.did, worker=worker, limit=args.limit, since=since)
            return len(results)
        finally:
            if worker:
                await worker.stop()

    n = asyncio.run(_go())
    print(f"backfilled {n} record(s) for {args.did}")
    return 0


def _cmd_discover(args: argparse.Namespace) -> int:
    from skybridge.atproto.discover import known_collections, report, run

    init_db()
    seen = asyncio.run(run(seconds=args.seconds, limit=args.limit))
    known = known_collections()
    print(f"observed {seen} event(s)")
    for row in report():
        mark = "bridged    " if row.nsid in known else "NOT BRIDGED"
        print(f"  {mark} {row.nsid:38} events={row.event_count}")
    return 0


def _cmd_import(args: argparse.Namespace) -> int:
    """Plan, or queue, a historical archive import.

    Queuing writes a job row; the running server picks it up and executes it
    in-process, which is what keeps opt-out able to cancel it. ``--run``
    executes here instead. That is safe beside a running server — jobs are
    claimed atomically, so only one process ever works one — but an
    out-of-process run cannot be cancelled by an opt-out, so prefer queueing
    when delivering.
    """
    from skybridge.atproto import archive

    init_db()

    async def _go() -> int:
        if args.dry_run:
            est = await archive.estimate(after_seq=args.after_seq, before_seq=args.before_seq)
            gib = est.estimated_bytes / 1024**3
            print(
                f"seq {est.after_seq} to {est.before_seq}\n"
                f"  segments      : {est.segments} ({est.whole_segments} whole-file)\n"
                f"  blocks        : {est.blocks} in {est.block_ranges} range(s)\n"
                f"  planner units : {est.planner_entries} "
                f"(work units, NOT a record count)\n"
                f"  download      : ~{gib:.1f} GiB (metered)"
            )
            return 0
        job_id = archive.create_job(
            after_seq=args.after_seq, before_seq=args.before_seq, deliver=args.deliver
        )
        if not args.run:
            print(f"queued import job {job_id}; the running server will start it")
            return 0
        return await archive.run_import(job_id)

    result = asyncio.run(_go())
    if args.run and not args.dry_run:
        print(f"applied {result} event(s)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="skybridge", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="run the ActivityPub + web server")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    p_ingest = sub.add_parser("ingest", help="stream live popfeed activity from Jetstream")
    p_ingest.add_argument("--limit", type=int, default=None, help="stop after N events")
    p_ingest.set_defaults(func=_cmd_ingest)

    p_replay = sub.add_parser("replay", help="replay a captured JSONL fixture")
    p_replay.add_argument("path")
    p_replay.add_argument("--reset", action="store_true", help="reset the DB first")
    p_replay.add_argument("--deliver", action="store_true", help="actually deliver")
    p_replay.add_argument("--network", action="store_true", help="allow identity resolution")
    p_replay.set_defaults(func=_cmd_replay)

    p_backfill = sub.add_parser("backfill", help="seed from a DID's existing records")
    p_backfill.add_argument("did")
    p_backfill.add_argument(
        "--limit",
        type=int,
        default=None,
        help="max records fetched, total across collections (default: SKYBRIDGE_BACKFILL_LIMIT)",
    )
    p_backfill.add_argument(
        "--days",
        type=int,
        default=None,
        help="only replay records written in the last N days (default: no window; "
        "SKYBRIDGE_BACKFILL_DAYS applies to the web import only)",
    )
    p_backfill.add_argument("--deliver", action="store_true")
    p_backfill.set_defaults(func=_cmd_backfill)

    p_discover = sub.add_parser(
        "discover", help="survey collections published under the bridged namespaces"
    )
    p_discover.add_argument("--seconds", type=float, default=60.0, help="how long to watch")
    p_discover.add_argument("--limit", type=int, default=None, help="stop after N events")
    p_discover.set_defaults(func=_cmd_discover)

    p_import = sub.add_parser("import", help="import history from the Jetstream v2 archive")
    p_import.add_argument("--after-seq", type=int, default=0, help="start sequence (default: 0)")
    p_import.add_argument(
        "--before-seq",
        type=int,
        default=None,
        help="end sequence (default: the live ingest cursor, so the import "
        "never covers what the live tail already has)",
    )
    p_import.add_argument(
        "--dry-run", action="store_true", help="plan only; print the estimated byte cost"
    )
    p_import.add_argument(
        "--run",
        action="store_true",
        help="execute here instead of queueing for the running server. Safe to "
        "run alongside one — a job is claimed atomically, so only one process "
        "ever works it — but prefer queueing when combined with --deliver, "
        "since only an in-process import can be cancelled by an opt-out",
    )
    p_import.add_argument(
        "--deliver", action="store_true", help="fan imported records out to peers (off by default)"
    )
    p_import.set_defaults(func=_cmd_import)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    print(f"skybridge @ {settings.base_url} (db={settings.db_path})", file=sys.stderr)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
