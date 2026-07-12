"""Settings derivation from the environment."""

from __future__ import annotations

from skybridge.config import _from_env


def test_data_dir_drives_state_paths(monkeypatch):
    monkeypatch.setenv("SKYBRIDGE_DATA", "/srv/sb")
    s = _from_env()
    assert s.data_dir == "/srv/sb"
    assert s.db_path == "/srv/sb/skybridge.db"
    assert s.relay_key_file == "/srv/sb/relay_key.pem"


def test_data_dir_defaults_to_local_data(monkeypatch):
    monkeypatch.delenv("SKYBRIDGE_DATA", raising=False)
    s = _from_env()
    assert s.db_path == "data/skybridge.db"
    assert s.relay_key_file == "data/relay_key.pem"


def test_sentry_dsn_from_env(monkeypatch):
    monkeypatch.setenv("SKYBRIDGE_SENTRY_DSN", "https://key@o0.ingest.sentry.io/0")
    s = _from_env()
    assert s.sentry_dsn == "https://key@o0.ingest.sentry.io/0"


def test_sentry_dsn_unset_is_none(monkeypatch):
    monkeypatch.delenv("SKYBRIDGE_SENTRY_DSN", raising=False)
    s = _from_env()
    assert s.sentry_dsn is None


def test_relays_unset_is_empty(monkeypatch):
    monkeypatch.delenv("SKYBRIDGE_RELAYS", raising=False)
    s = _from_env()
    assert s.relays == ()


def test_relays_parses_commas_and_whitespace(monkeypatch):
    monkeypatch.setenv(
        "SKYBRIDGE_RELAYS",
        " https://a.example/inbox, https://b.example/inbox\thttps://c.example/inbox ",
    )
    s = _from_env()
    assert s.relays == (
        "https://a.example/inbox",
        "https://b.example/inbox",
        "https://c.example/inbox",
    )


def test_relays_dedupes_preserving_order(monkeypatch):
    monkeypatch.setenv(
        "SKYBRIDGE_RELAYS",
        "https://a.example/inbox,https://b.example/inbox,https://a.example/inbox",
    )
    s = _from_env()
    assert s.relays == ("https://a.example/inbox", "https://b.example/inbox")


def test_init_db_fails_loudly_on_readonly_database(tmp_path, monkeypatch):
    import os

    import pytest
    from skybridge.config import Settings, set_settings
    from skybridge.db import init_db

    db_file = tmp_path / "ro" / "skybridge.db"
    set_settings(Settings(db_path=str(db_file)))
    try:
        init_db()  # creates + probes fine while writable
        os.chmod(db_file, 0o444)
        os.chmod(db_file.parent, 0o555)
        with pytest.raises(RuntimeError, match="not writable"):
            init_db()
    finally:
        os.chmod(db_file.parent, 0o755)
        os.chmod(db_file, 0o644)
        set_settings(None)
