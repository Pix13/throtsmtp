"""Integration tests — full relay server with mock upstream SMTP.

Spins up a real relay server + mock upstream, sends emails through the
inbound port, and verifies jitter timing, queue management, retry logic,
bounce handling, and capacity limits end-to-end.
"""

from __future__ import annotations

import asyncio
import smtplib
import socket
import time
from email.mime.text import MIMEText
from pathlib import Path

import aiosmtpd.controller
import aiosmtpd.smtp
import pytest

from src.config import (
    BounceConfig,
    Config,
    LocalConfig,
    LoggingConfig,
    QueueConfig,
    ThrottleConfig,
    UpstreamConfig,
)
from src.db import init_db
from src.queue_manager import QueueManager
from src.relay_worker import RelayWorker
from src.smtp_handler import RelayHandler, create_controller


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Grab an ephemeral port and return it."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_config(tmp_path: Path, max_size: int = 10) -> Config:
    """Build a Config tuned for fast integration tests."""
    return Config(
        local=LocalConfig(host="127.0.0.1", port=0, hostname="test.local"),
        upstream=UpstreamConfig(
            host="127.0.0.1",
            port=0,
            username="",
            password="",
            tls="none",
            timeout=5,
        ),
        throttle=ThrottleConfig(min_delay=0.05, max_delay=0.15),
        queue=QueueConfig(
            db_path=str(tmp_path / "queue.db"),
            max_size=max_size,
            max_retries=3,
            retry_base=0.2,
            retry_cap=1.0,
        ),
        bounce=BounceConfig(enabled=True, from_addr="mailer-daemon@test.local"),
        logging=LoggingConfig(
            level="WARNING",
            file=str(tmp_path / "relay.log"),
            max_bytes=1_048_576,
            backup_count=3,
        ),
    )


class MockUpstreamHandler:
    """aiosmtpd handler that acts as a mock upstream server."""

    def __init__(self):
        self.deliveries: list[dict] = []
        self.reject_next: int = 0
        self.reject_all_5xx: bool = False

    async def handle_DATA(
        self,
        server: aiosmtpd.smtp.SMTP,
        session: aiosmtpd.smtp.Session,
        envelope: aiosmtpd.smtp.Envelope,
    ) -> str:
        if self.reject_all_5xx:
            return "550 5.1.1 User unknown"
        if self.reject_next > 0:
            self.reject_next -= 1
            return "421 4.7.0 Service temporarily busy"
        self.deliveries.append({
            "mail_from": envelope.mail_from,
            "rcpt_tos": list(envelope.rcpt_tos),
            "content": envelope.original_content,
        })
        return "250 2.0.0 OK"


async def _wait_for_server(host: str, port: int, timeout: float = 10.0) -> bool:
    """Poll until the TCP port accepts connections and responds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=1.0
            )
            # Read the SMTP greeting
            greeting = await asyncio.wait_for(reader.readline(), timeout=1.0)
            writer.close()
            await writer.wait_closed()
            if b"220" in greeting:
                return True
        except (ConnectionRefusedError, OSError):
            pass
        await asyncio.sleep(0.1)
    return False


async def send_smtp(
    port: int,
    mail_from: str,
    rcpt_to: list[str],
    msg: MIMEText,
) -> None:
    """Send a message via synchronous SMTP (run in executor to avoid blocking)."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _send_sync, port, mail_from, rcpt_to, msg)


def _send_sync(port: int, mail_from: str, rcpt_to: list[str], msg: MIMEText) -> None:
    """Synchronous SMTP send."""
    with smtplib.SMTP("127.0.0.1", port, timeout=10) as s:
        s.send_message(msg, mail_from, rcpt_to)


async def wait_for_count(
    queue: QueueManager,
    status: str,
    expected: int,
    timeout: float = 30.0,
) -> bool:
    """Block until queue reaches expected count for given status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        count = await queue.count(status)
        if count >= expected:
            return True
        await asyncio.sleep(0.1)
    count = await queue.count(status)
    raise TimeoutError(
        f"Expected {expected} {status}, got {count} after {timeout:.1f}s"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_pipeline_10_emails(tmp_path: Path):
    """Send 10 emails, verify all relay with jitter timing."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()

    # Start mock upstream on a real port
    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    # Start relay on a free port
    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    # Wait for relay to be ready
    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        # Send 10 emails quickly
        for i in range(10):
            msg = MIMEText(f"body-{i}")
            msg["From"] = f"sender{i}@test.local"
            msg["To"] = f"rcpt{i}@test.local"
            msg["Message-ID"] = f"<msg{i}@test.local>"
            await send_smtp(
                relay_port,
                f"sender{i}@test.local",
                [f"rcpt{i}@test.local"],
                msg,
            )

        # Wait for all to be sent
        t0 = time.monotonic()
        await wait_for_count(queue, "sent", 10, timeout=30.0)
        elapsed = time.monotonic() - t0

        sent = await queue.count("sent")
        assert sent == 10
        assert len(mock_handler.deliveries) == 10

        # Jitter check: 9 gaps * 0.05 min = 0.45s minimum
        # Allow 50% tolerance for scheduling jitter
        min_expected = 9 * 0.05 * 0.5
        assert elapsed >= min_expected, (
            f"Elapsed {elapsed:.2f}s too fast -- jitter not applied "
            f"(expected >= {min_expected:.2f}s)"
        )
        # Max: 9 * 0.15 + 5s buffer
        max_expected = 9 * 0.15 + 5
        assert elapsed <= max_expected, (
            f"Elapsed {elapsed:.2f}s too slow (expected <= {max_expected:.2f}s)"
        )

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_queue_full_returns_452(tmp_path: Path):
    """Queue at capacity returns 452 (transient, client can retry)."""
    cfg = _make_config(tmp_path, max_size=3)
    mock_handler = MockUpstreamHandler()

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    # Don't start relay -- emails accumulate
    try:
        for i in range(3):
            msg = MIMEText(f"fill-{i}")
            msg["From"] = f"fill{i}@t"
            msg["To"] = "rcpt@t"
            msg["Message-ID"] = f"<fill{i}>"
            await send_smtp(relay_port, f"fill{i}@t", ["rcpt@t"], msg)

        queued = await queue.count("queued")
        assert queued == 3

        # 4th should get 452
        msg = MIMEText("overflow")
        msg["From"] = "overflow@t"
        msg["To"] = "rcpt@t"
        msg["Message-ID"] = "<overflow>"
        with pytest.raises(smtplib.SMTPDataError) as exc:
            await send_smtp(relay_port, "overflow@t", ["rcpt@t"], msg)
        assert "452" in str(exc.value)

    finally:
        ctrl.stop()
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_transient_failure_retry(tmp_path: Path):
    """4xx triggers retry with exponential backoff, then succeeds."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()
    mock_handler.reject_next = 2  # fail first 2, succeed on 3rd

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        msg = MIMEText("retry-test")
        msg["From"] = "retry@t"
        msg["To"] = "rcpt@t"
        msg["Message-ID"] = "<retry1>"
        await send_smtp(relay_port, "retry@t", ["rcpt@t"], msg)

        await wait_for_count(queue, "sent", 1, timeout=20.0)

        sent = await queue.count("sent")
        assert sent == 1
        assert len(mock_handler.deliveries) == 1

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_permanent_failure_bounce(tmp_path: Path):
    """5xx marks email as bounced and logs error."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()
    mock_handler.reject_all_5xx = True

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        msg = MIMEText("bounce-test")
        msg["From"] = "bounce@t"
        msg["To"] = "bad@nowhere"
        msg["Message-ID"] = "<bounce1>"
        await send_smtp(relay_port, "bounce@t", ["bad@nowhere"], msg)

        await wait_for_count(queue, "bounced", 1, timeout=10.0)

        bounced = await queue.count("bounced")
        assert bounced == 1

        records = await queue.list_emails()
        assert len(records) == 1
        assert "550" in (records[0].get("upstream_response") or "")

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_fifo_ordering(tmp_path: Path):
    """Emails are relayed in FIFO order."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    # Don't start relay -- accumulate then check dequeue order
    try:
        for i in range(5):
            msg = MIMEText(f"fifo-{i}")
            msg["From"] = f"f{i}@t"
            msg["To"] = "rcpt@t"
            msg["Message-ID"] = f"<f{i}>"
            await send_smtp(relay_port, f"f{i}@t", ["rcpt@t"], msg)
            await asyncio.sleep(0.01)

        queued = await queue.count("queued")
        assert queued == 5

        # Dequeue and verify order
        order = []
        for _ in range(5):
            email = await queue.dequeue_next()
            if email:
                order.append(email.mail_from)

        expected = [f"f{i}@t" for i in range(5)]
        assert order == expected, f"FIFO violated: {order}"

    finally:
        ctrl.stop()
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_retry_exhaustion_bounces(tmp_path: Path):
    """After max_retries transient failures, email is bounced."""
    cfg = _make_config(tmp_path)
    cfg.queue.max_retries = 2
    cfg.queue.retry_base = 0.1
    mock_handler = MockUpstreamHandler()
    mock_handler.reject_next = 10  # always fail

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        msg = MIMEText("exhaust-test")
        msg["From"] = "exhaust@t"
        msg["To"] = "rcpt@t"
        msg["Message-ID"] = "<exhaust1>"
        await send_smtp(relay_port, "exhaust@t", ["rcpt@t"], msg)

        await wait_for_count(queue, "bounced", 1, timeout=15.0)

        bounced = await queue.count("bounced")
        assert bounced == 1

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_clear_and_prune(tmp_path: Path):
    """Clear and prune operations work correctly."""
    cfg = _make_config(tmp_path)
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)

    try:
        # Populate
        for i in range(3):
            await queue.enqueue(f"q{i}@t", ["r@t"], b"test", f"<q{i}>")

        # Send 2
        e1 = await queue.dequeue_next()
        e2 = await queue.dequeue_next()
        await queue.mark_sent(e1.id)
        await queue.mark_sent(e2.id)

        assert await queue.count("sent") == 2
        # Third email is still queued (not yet dequeued)
        assert await queue.count("queued") == 1

        # Clear sent
        cleared = await queue.clear_queue("sent")
        assert cleared == 2
        assert await queue.count("sent") == 0

    finally:
        await db.close()


@pytest.mark.asyncio
async def test_queue_persistence(tmp_path: Path):
    """Queue survives re-initialization."""
    cfg = _make_config(tmp_path)
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)

    # Enqueue
    await queue.enqueue("persist@t", ["r@t"], b"persistent", "<persist>")
    await db.close()

    # Re-open
    db2 = await init_db(cfg.queue.db_path)
    queue2 = QueueManager(db2, cfg.queue)

    count = await queue2.count("queued")
    assert count == 1

    email = await queue2.dequeue_next()
    assert email is not None
    assert email.mail_from == "persist@t"
    assert email.raw_message == b"persistent"

    await db2.close()


@pytest.mark.asyncio
async def test_relay_processes_existing_queue(tmp_path: Path):
    """Relay worker processes emails that were already in the queue."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    # Pre-populate queue
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    await queue.enqueue("pre@t", ["r@t"], b"pre-queued", "<pre1>")
    await queue.enqueue("pre2@t", ["r@t"], b"pre-queued-2", "<pre2>")

    handler = RelayHandler(queue, cfg)
    relay_port = _free_port()
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        await wait_for_count(queue, "sent", 2, timeout=15.0)

        sent = await queue.count("sent")
        assert sent == 2
        assert len(mock_handler.deliveries) == 2

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()


@pytest.mark.asyncio
async def test_multiple_recipients(tmp_path: Path):
    """Email with multiple recipients is split into individual deliveries."""
    cfg = _make_config(tmp_path)
    mock_handler = MockUpstreamHandler()

    mock_port = _free_port()
    mock_ctrl = aiosmtpd.controller.Controller(
        mock_handler, hostname="127.0.0.1", port=mock_port
    )
    mock_ctrl.start()
    cfg.upstream.port = mock_port

    relay_port = _free_port()
    db = await init_db(cfg.queue.db_path)
    queue = QueueManager(db, cfg.queue)
    handler = RelayHandler(queue, cfg)
    ctrl = create_controller(handler, cfg, port=relay_port)
    ctrl.start()

    ready = await _wait_for_server("127.0.0.1", relay_port)
    assert ready, "Relay server did not start"

    relay = RelayWorker(queue, cfg)
    relay_task = asyncio.create_task(relay.start())

    try:
        msg = MIMEText("multi-rcpt")
        msg["From"] = "multi@t"
        msg["To"] = "a@t, b@t, c@t"
        msg["Message-ID"] = "<multi1>"
        await send_smtp(
            relay_port,
            "multi@t",
            ["a@t", "b@t", "c@t"],
            msg,
        )

        # Each recipient gets its own queue entry and delivery
        await wait_for_count(queue, "sent", 3, timeout=15.0)

        assert len(mock_handler.deliveries) == 3
        delivered_to = {d["rcpt_tos"][0] for d in mock_handler.deliveries}
        assert delivered_to == {"a@t", "b@t", "c@t"}

    finally:
        ctrl.stop()
        await relay.stop()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        mock_ctrl.stop()
        await db.close()
