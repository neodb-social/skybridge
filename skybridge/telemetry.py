"""Optional Sentry integration: error tracking plus a per-collection ingest
counter.

Everything here is a no-op unless ``SKYBRIDGE_SENTRY_DSN`` is set, and none of
it may ever raise into callers — telemetry must never take down the relay.
"""

from __future__ import annotations

import logging

from skybridge.config import get_settings

log = logging.getLogger("skybridge.telemetry")

_enabled: bool = False


def init_sentry() -> bool:
    """Initialize Sentry from ``Settings.sentry_dsn``, if set.

    Returns whether telemetry is now enabled. A bad DSN (or any init failure)
    is logged and swallowed rather than killing startup.
    """
    global _enabled
    dsn = get_settings().sentry_dsn
    if not dsn:
        return False
    try:
        import sentry_sdk

        sentry_sdk.init(dsn=dsn, send_default_pii=False)
    except Exception:
        log.warning("Sentry init failed; telemetry disabled", exc_info=True)
        return False
    _enabled = True
    log.info("Sentry telemetry enabled")
    return True


def record_ingested(collection: str, operation: str) -> None:
    """Tick the ``atproto.record_ingested`` counter, if telemetry is on."""
    if not _enabled:
        return
    try:
        import sentry_sdk

        sentry_sdk.metrics.count(
            "atproto.record_ingested",
            1,
            attributes={"collection": collection, "operation": operation},
        )
    except Exception:
        log.warning("Sentry metric emission failed", exc_info=True)
