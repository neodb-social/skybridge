"""In-memory sign-in sessions for the opt-out self-service pages.

A session is created only after the account holder completes the atproto
OAuth flow (see ``skybridge.atproto.oauth``); it records the verified DID so
the follow-up opt-out/opt-in POSTs don't need to re-run the flow. Sessions
live in process memory (single-process server, same trade-off as pending
OAuth flows), are referenced by an opaque cookie token, and expire after
``SESSION_TTL`` seconds. Each session carries a CSRF token that action
forms must echo back.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass

SESSION_TTL = 1800.0  # seconds a signed-in session stays valid
_SESSIONS_MAX = 1024

COOKIE_NAME = "skybridge_session"


@dataclass(frozen=True)
class Session:
    did: str
    handle: str
    csrf: str
    expires_at: float


_SESSIONS: dict[str, Session] = {}


def create(did: str, handle: str) -> str:
    """Open a session for a verified account; returns the cookie token."""
    now = time.time()
    for key in [k for k, s in _SESSIONS.items() if s.expires_at < now]:
        _SESSIONS.pop(key, None)
    if len(_SESSIONS) >= _SESSIONS_MAX:
        _SESSIONS.clear()  # under abuse, drop sessions rather than memory
    token = secrets.token_urlsafe(32)
    _SESSIONS[token] = Session(
        did=did,
        handle=handle,
        csrf=secrets.token_urlsafe(32),
        expires_at=now + SESSION_TTL,
    )
    return token


def get(token: str | None) -> Session | None:
    if not token:
        return None
    session = _SESSIONS.get(token)
    if session is None:
        return None
    if session.expires_at < time.time():
        _SESSIONS.pop(token, None)
        return None
    return session


def drop(token: str | None) -> None:
    if token:
        _SESSIONS.pop(token, None)
