"""Tests for relay worker."""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.config import Config, QueueConfig, ThrottleConfig
from src.db import init_db
from src.queue_manager import QueueManager
from src.relay_worker import RelayWorker


@pytest.fixture
async def worker(tmp_path):
    """Create a test relay worker."""
    db = await init_db(str(tmp_path / "test.db"))
    config = Config()
    config.queue = QueueConfig(db_path=str(tmp_path / "test.db"))
    config.throttle = ThrottleConfig(min_delay=0.1, max_delay=0.2)  # Fast for tests
    config.upstream.host = "smtp.test.com"
    config.upstream.port = 587
    config.upstream.username = "test"
    config.upstream.password = "test"
    config.upstream.tls = "starttls"
    q = QueueManager(db, config.queue)
    w = RelayWorker(q, config)
    yield w, db
    await db.close()


@pytest.mark.asyncio
async def test_relay_sends_email(worker):
    """Relay worker should send email and mark as sent."""
    w, db = worker
    q = w.queue

    # Enqueue an email
    await q.enqueue("sender@test.com", ["rcpt@test.com"],
                    b"From: sender@test.com\r\nTo: rcpt@test.com\r\n\r\nBody",
                    "<test@localhost>")

    # Mock the SMTP connection
    mock_smtp = AsyncMock()
    mock_smtp.connect = AsyncMock()
    mock_smtp.starttls = AsyncMock()
    mock_smtp.login = AsyncMock()
    mock_smtp.send_message = AsyncMock()
    mock_smtp.quit = AsyncMock()

    with patch("src.relay_worker.aiosmtplib.SMTP", return_value=mock_smtp):
        await w._process_one()

    # Should be marked as sent
    sent = await q.count("sent")
    assert sent == 1


@pytest.mark.asyncio
async def test_relay_handles_transient_failure(worker):
    """Relay worker should retry on transient failure."""
    w, db = worker
    q = w.queue

    await q.enqueue("sender@test.com", ["rcpt@test.com"],
                    b"From: sender@test.com\r\nTo: rcpt@test.com\r\n\r\nBody",
                    "<test@localhost>")

    # Mock SMTP to raise connection error
    mock_smtp = AsyncMock()
    mock_smtp.connect = AsyncMock(side_effect=Exception("Connection refused"))
    mock_smtp.quit = AsyncMock()

    with patch("src.relay_worker.aiosmtplib.SMTP", return_value=mock_smtp):
        await w._process_one()

    # Should be in failed status (scheduled for retry)
    failed = await q.count("failed")
    assert failed == 1


@pytest.mark.asyncio
async def test_relay_paused_skips_processing(worker, tmp_path):
    """Relay worker should skip processing when paused."""
    w, db = worker
    q = w.queue

    # Enqueue an email
    await q.enqueue("sender@test.com", ["rcpt@test.com"],
                    b"From: sender@test.com\r\nTo: rcpt@test.com\r\n\r\nBody",
                    "<test@localhost>")

    # Create pause marker file
    w._pause_file.touch()

    # Mock SMTP so we can verify it's never called
    mock_smtp_class = MagicMock()
    mock_smtp = AsyncMock()
    mock_smtp_class.return_value = mock_smtp

    with patch("src.relay_worker.aiosmtplib.SMTP", mock_smtp_class):
        with patch.object(w, "_pause_file", w._pause_file):
            await w._process_one()

    # SMTP should never have been instantiated
    mock_smtp_class.assert_not_called()

    # Email should still be queued (not processed)
    queued = await q.count("queued")
    assert queued == 1

    # Clean up
    w._pause_file.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_relay_resumes_after_pause_removed(worker):
    """Relay worker should process emails when pause marker is removed."""
    w, db = worker
    q = w.queue

    await q.enqueue("sender@test.com", ["rcpt@test.com"],
                    b"From: sender@test.com\r\nTo: rcpt@test.com\r\n\r\nBody",
                    "<test@localhost>")

    # Mock the SMTP connection
    mock_smtp = AsyncMock()
    mock_smtp.connect = AsyncMock()
    mock_smtp.starttls = AsyncMock()
    mock_smtp.login = AsyncMock()
    mock_smtp.sendmail = AsyncMock()
    mock_smtp.quit = AsyncMock()

    # First, pause
    w._pause_file.touch()
    with patch("src.relay_worker.aiosmtplib.SMTP", return_value=mock_smtp):
        await w._process_one()

    # Email still queued
    assert await q.count("queued") == 1

    # Remove pause marker
    w._pause_file.unlink(missing_ok=True)

    # Reset mock to track fresh calls
    mock_smtp_class = MagicMock(return_value=mock_smtp)

    with patch("src.relay_worker.aiosmtplib.SMTP", mock_smtp_class):
        await w._process_one()

    # Now SMTP was called and email is sent
    mock_smtp_class.assert_called_once()
    assert await q.count("sent") == 1
