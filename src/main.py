"""Main entry point — starts the SMTP relay server."""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

from .config import Config, load_config
from .db import init_db
from .queue_manager import QueueManager
from .relay_worker import RelayWorker
from .smtp_handler import RelayHandler, create_controller


def setup_logging(config: Config) -> logging.Logger:
    """Configure logging with rotating file handler + console."""
    log_cfg = config.logging
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_cfg.level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file handler
    fh = logging.handlers.RotatingFileHandler(
        log_cfg.file,
        maxBytes=log_cfg.max_bytes,
        backupCount=log_cfg.backup_count,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return logging.getLogger("throt-relay")


async def run(config: Config) -> None:
    """Run the relay server."""
    logger = logging.getLogger("throt-relay")

    # Validate config
    errors = config.validate()
    if errors:
        logger.error("Configuration errors:")
        for e in errors:
            logger.error("  - %s", e)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Throttled SMTP Relay Server starting")
    logger.info("=" * 60)
    logger.info("Local:    %s:%d", config.local.host, config.local.port)
    logger.info("Upstream: %s:%d (%s)", config.upstream.host, config.upstream.port, config.upstream.tls)
    logger.info("Throttle: %d-%d seconds", config.throttle.min_delay, config.throttle.max_delay)
    logger.info("Queue:    %s (max %d)", config.queue.db_path, config.queue.max_size)
    logger.info("=" * 60)

    # Initialize database
    db = await init_db(config.queue.db_path)
    queue = QueueManager(db, config.queue)

    # Stats
    total = await queue.count()
    queued = await queue.count("queued")
    sent = await queue.count("sent")
    bounced = await queue.count("bounced")
    failed = await queue.count("failed")
    logger.info("Queue stats: total=%d queued=%d sent=%d failed=%d bounced=%d",
                total, queued, sent, failed, bounced)

    # Create SMTP handler and controller
    handler = RelayHandler(queue, config)
    controller = create_controller(handler, config)

    # Start relay worker
    relay = RelayWorker(queue, config)
    relay_task = asyncio.create_task(relay.start())

    # Graceful shutdown
    stop_event = asyncio.Event()

    def handle_signal(sig: signal.Signals) -> None:
        logger.info("Received signal %s, shutting down...", sig.name)
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal, sig)

    # Start SMTP server
    logger.info("SMTP server listening on %s:%d", config.local.host, config.local.port)
    controller.start()
    controller.server.inet_port = config.local.port
    controller.server.hostname = config.local.hostname

    # Wait for shutdown signal
    await stop_event.wait()

    # Shutdown
    logger.info("Shutting down...")
    controller.stop()
    await relay.stop()
    relay_task.cancel()
    try:
        await relay_task
    except asyncio.CancelledError:
        pass

    await db.close()
    logger.info("Shutdown complete.")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Throttled SMTP Relay Server")
    parser.add_argument("--config", "-c", help="Path to config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config)

    asyncio.run(run(config))


if __name__ == "__main__":
    main()
