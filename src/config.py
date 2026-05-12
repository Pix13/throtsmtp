"""Configuration loader — YAML file with environment variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class LocalConfig:
    host: str = "0.0.0.0"
    port: int = 1025
    hostname: str = "localhost"


@dataclass
class UpstreamConfig:
    host: str = ""
    port: int = 587
    username: str = ""
    password: str = ""
    tls: str = "starttls"  # "starttls" or "ssl"
    timeout: int = 30


@dataclass
class ThrottleConfig:
    min_delay: int = 30
    max_delay: int = 120


@dataclass
class QueueConfig:
    db_path: str = "queue.db"
    max_size: int = 10000
    max_retries: int = 5
    retry_base: int = 60
    retry_cap: int = 3600


@dataclass
class BounceConfig:
    enabled: bool = True
    from_addr: str = "mailer-daemon@localhost"


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "relay.log"
    max_bytes: int = 10_485_760
    backup_count: int = 5


@dataclass
class Config:
    local: LocalConfig = field(default_factory=LocalConfig)
    upstream: UpstreamConfig = field(default_factory=UpstreamConfig)
    throttle: ThrottleConfig = field(default_factory=ThrottleConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    bounce: BounceConfig = field(default_factory=BounceConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    def validate(self) -> list[str]:
        """Return a list of validation errors (empty if valid)."""
        errors: list[str] = []
        if not self.upstream.host:
            errors.append("upstream.host is required")
        if not self.upstream.username:
            errors.append("upstream.username is required")
        if not self.upstream.password:
            errors.append("upstream.password is required")
        if self.upstream.tls not in ("starttls", "ssl"):
            errors.append("upstream.tls must be 'starttls' or 'ssl'")
        if self.throttle.min_delay < 1:
            errors.append("throttle.min_delay must be >= 1")
        if self.throttle.max_delay < self.throttle.min_delay:
            errors.append("throttle.max_delay must be >= min_delay")
        return errors


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("true", "1", "yes")


def load_config(path: Optional[str] = None) -> Config:
    """Load config from YAML file, then override with environment variables."""
    cfg = Config()

    # Load YAML if available
    yaml_path = path or str(Path(__file__).parent.parent / "config.yaml")
    p = Path(yaml_path)
    if p.exists():
        with open(p) as f:
            data = yaml.safe_load(f) or {}

        local = data.get("local", {})
        cfg.local.host = local.get("host", cfg.local.host)
        cfg.local.port = int(local.get("port", cfg.local.port))
        cfg.local.hostname = local.get("hostname", cfg.local.hostname)

        up = data.get("upstream", {})
        cfg.upstream.host = up.get("host", cfg.upstream.host)
        cfg.upstream.port = int(up.get("port", cfg.upstream.port))
        cfg.upstream.username = up.get("username", cfg.upstream.username)
        cfg.upstream.password = up.get("password", cfg.upstream.password)
        cfg.upstream.tls = up.get("tls", cfg.upstream.tls)
        cfg.upstream.timeout = int(up.get("timeout", cfg.upstream.timeout))

        th = data.get("throttle", {})
        cfg.throttle.min_delay = int(th.get("min_delay", cfg.throttle.min_delay))
        cfg.throttle.max_delay = int(th.get("max_delay", cfg.throttle.max_delay))

        q = data.get("queue", {})
        cfg.queue.db_path = q.get("db_path", cfg.queue.db_path)
        cfg.queue.max_size = int(q.get("max_size", cfg.queue.max_size))
        cfg.queue.max_retries = int(q.get("max_retries", cfg.queue.max_retries))
        cfg.queue.retry_base = int(q.get("retry_base", cfg.queue.retry_base))
        cfg.queue.retry_cap = int(q.get("retry_cap", cfg.queue.retry_cap))

        b = data.get("bounce", {})
        cfg.bounce.enabled = b.get("enabled", cfg.bounce.enabled)
        cfg.bounce.from_addr = b.get("from", cfg.bounce.from_addr)

        lg = data.get("logging", {})
        cfg.logging.level = lg.get("level", cfg.logging.level)
        cfg.logging.file = lg.get("file", cfg.logging.file)
        cfg.logging.max_bytes = int(lg.get("max_bytes", cfg.logging.max_bytes))
        cfg.logging.backup_count = int(lg.get("backup_count", cfg.logging.backup_count))

    # Environment variable overrides (always win)
    cfg.local.host = _env_str("THROT_LOCAL_HOST", cfg.local.host)
    cfg.local.port = _env_int("THROT_LOCAL_PORT", cfg.local.port)

    cfg.upstream.host = _env_str("UPSTREAM_HOST", cfg.upstream.host) or cfg.upstream.host
    cfg.upstream.port = _env_int("UPSTREAM_PORT", cfg.upstream.port)
    cfg.upstream.username = _env_str("UPSTREAM_USERNAME", cfg.upstream.username) or cfg.upstream.username
    cfg.upstream.password = _env_str("UPSTREAM_PASSWORD", cfg.upstream.password) or cfg.upstream.password
    cfg.upstream.tls = _env_str("UPSTREAM_TLS", cfg.upstream.tls)

    cfg.throttle.min_delay = _env_int("THROT_MIN_DELAY", cfg.throttle.min_delay)
    cfg.throttle.max_delay = _env_int("THROT_MAX_DELAY", cfg.throttle.max_delay)

    cfg.queue.db_path = _env_str("THROT_DB_PATH", cfg.queue.db_path)
    cfg.queue.max_size = _env_int("THROT_MAX_QUEUE", cfg.queue.max_size)
    cfg.queue.max_retries = _env_int("THROT_MAX_RETRIES", cfg.queue.max_retries)

    cfg.bounce.enabled = _env_bool("THROT_BOUNCE_ENABLED", cfg.bounce.enabled)
    cfg.bounce.from_addr = _env_str("THROT_BOUNCE_FROM", cfg.bounce.from_addr)

    cfg.logging.level = _env_str("THROT_LOG_LEVEL", cfg.logging.level)
    cfg.logging.file = _env_str("THROT_LOG_FILE", cfg.logging.file)

    return cfg
