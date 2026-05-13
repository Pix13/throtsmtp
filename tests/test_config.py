"""Tests for configuration loading."""

import os
import pytest
from src.config import load_config, Config


def test_default_config():
    """Default config should have sensible values."""
    cfg = load_config("/nonexistent.yaml")
    assert cfg.local.port == 1025
    assert cfg.throttle.min_delay == 30
    assert cfg.throttle.max_delay == 120
    assert cfg.queue.max_size == 10000


def test_validate_missing_upstream():
    """Config without upstream host should fail validation."""
    cfg = Config()
    errors = cfg.validate()
    assert any("upstream.host" in e for e in errors)


def test_validate_auth_mismatch():
    """Username and password must both be set or both empty."""
    cfg = Config()
    cfg.upstream.host = "smtp.example.com"

    # Both empty — valid (no auth)
    assert not cfg.validate()

    # Only username set — invalid
    cfg.upstream.username = "user"
    errors = cfg.validate()
    assert any("upstream.username" in e and "upstream.password" in e for e in errors)

    # Both set — valid
    cfg.upstream.password = "pass"
    assert not cfg.validate()


def test_env_override(tmp_path, monkeypatch):
    """Environment variables should override defaults."""
    monkeypatch.setenv("UPSTREAM_HOST", "test.smtp.com")
    monkeypatch.setenv("UPSTREAM_PORT", "465")
    monkeypatch.setenv("THROT_LOCAL_PORT", "2525")

    cfg = load_config(str(tmp_path / "nonexistent.yaml"))
    assert cfg.upstream.host == "test.smtp.com"
    assert cfg.upstream.port == 465
    assert cfg.local.port == 2525
