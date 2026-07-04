"""Settings derivation from the environment."""

from __future__ import annotations

from skybridge.config import _from_env


def test_data_dir_drives_default_paths(monkeypatch):
    monkeypatch.setenv("SKYBRIDGE_DATA", "/srv/sb")
    monkeypatch.delenv("SKYBRIDGE_DB", raising=False)
    monkeypatch.delenv("SKYBRIDGE_RELAY_KEY_FILE", raising=False)
    s = _from_env()
    assert s.data_dir == "/srv/sb"
    assert s.db_path == "/srv/sb/skybridge.db"
    assert s.relay_key_file == "/srv/sb/relay_key.pem"


def test_explicit_paths_override_data_dir(monkeypatch):
    monkeypatch.setenv("SKYBRIDGE_DATA", "/srv/sb")
    monkeypatch.setenv("SKYBRIDGE_DB", ":memory:")
    monkeypatch.setenv("SKYBRIDGE_RELAY_KEY_FILE", "/keys/k.pem")
    s = _from_env()
    assert s.db_path == ":memory:"
    assert s.relay_key_file == "/keys/k.pem"
