"""Tests for SMTP handler."""

import asyncio
from unittest.mock import MagicMock

import pytest
from aiosmtpd.smtp import Envelope

from src.config import Config, QueueConfig
from src.db import init_db
from src.queue_manager import QueueManager
from src.smtp_handler import RelayHandler


@pytest.fixture
async def handler(tmp_path):
    """Create a test handler."""
    db = await init_db(str(tmp_path / "test.db"))
    config = Config()
    config.queue = QueueConfig(db_path=str(tmp_path / "test.db"))
    config.local.hostname = "test.local"
    q = QueueManager(db, config.queue)
    h = RelayHandler(q, config)
    yield h, db
    await db.close()


@pytest.mark.asyncio
async def test_handle_data(handler):
    """handle_DATA should enqueue the message."""
    h, db = handler

    session = MagicMock()
    session.peer = ("127.0.0.1", 12345)

    envelope = Envelope()
    envelope.mail_from = "sender@test.com"
    envelope.rcpt_tos = ["rcpt@test.com"]
    envelope.original_content = b"From: sender@test.com\r\nTo: rcpt@test.com\r\n\r\nBody"

    result = await h.handle_DATA(None, session, envelope)
    assert "250" in result

    count = await h.queue.count()
    assert count == 1


@pytest.mark.asyncio
async def test_handle_data_no_recipients(handler):
    """handle_DATA with no recipients should reject."""
    h, db = handler

    session = MagicMock()
    session.peer = ("127.0.0.1", 12345)

    envelope = Envelope()
    envelope.mail_from = "sender@test.com"
    envelope.rcpt_tos = []
    envelope.original_content = b"test"

    result = await h.handle_DATA(None, session, envelope)
    assert "550" in result


@pytest.mark.asyncio
async def test_extract_message_id(handler):
    """Message-ID extraction should work."""
    h, db = handler

    raw = b"Message-ID: <abc123@example.com>\r\nFrom: test@test.com\r\n\r\nBody"
    mid = h._extract_message_id(raw)
    assert mid == "<abc123@example.com>"

    # No Message-ID
    raw2 = b"From: test@test.com\r\n\r\nBody"
    mid2 = h._extract_message_id(raw2)
    assert mid2 == ""
