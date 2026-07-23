"""Skybridge CLI: ``python -m skybridge {serve|ingest|replay|backfill|repair}``.

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


def _cmd_repair(args: argparse.Namespace) -> int:
    from skybridge.activitypub.delivery import DeliveryWorker
    from skybridge.maintenance import repair

    init_db()

    async def _go():
        # Delivery is not optional here (only --dry-run skips it): the repair
        # destroys the state its own broadcasts are built from (stored Note
        # ids, episode-typed work_keys), so a mutating-but-silent run would
        # strand the mis-mapped Notes on peers with no way to retract them.
        worker = None if args.dry_run else DeliveryWorker()
        if worker:
            worker.start()
        try:
            return await repair(worker, dry_run=args.dry_run)
        finally:
            if worker:
                await worker.stop()

    report = asyncio.run(_go())
    if report.dry_run:
        for at_uri, work_key in report.would_retract:
            print(f"would retract {at_uri} ({work_key})")
        print(f"dry run: would retract {report.retracted} episode note(s); rebuild skipped")
    else:
        print(
            f"retracted {report.retracted} episode note(s) "
            f"(+{report.resent} pending re-sent), "
            f"works {report.works_before} -> {report.works_after}, "
            f"remapped {report.remapped} record(s), re-synced {report.resynced} note(s), "
            f"enqueued {report.deliveries} deliver(ies)"
        )
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

    p_repair = sub.add_parser(
        "repair",
        help="retract published tv_episode notes, rebuild the works catalog from "
        "the archive, and broadcast the corrections (see skybridge.maintenance)",
    )
    p_repair.add_argument(
        "--dry-run", action="store_true", help="list what would be retracted, change nothing"
    )
    p_repair.set_defaults(func=_cmd_repair)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    print(f"skybridge @ {settings.base_url} (db={settings.db_path})", file=sys.stderr)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
