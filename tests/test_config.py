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
