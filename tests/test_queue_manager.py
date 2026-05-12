"""Tests for queue manager."""

import asyncio
import pytest
from src.config import QueueConfig
from src.db import init_db
from src.queue_manager import QueueManager


@pytest.fixture
async def queue(tmp_path):
    """Create a test queue."""
    db = await init_db(str(tmp_path / "test.db"))
    config = QueueConfig(max_size=5, max_retries=3, retry_base=60, retry_cap=3600)
    q = QueueManager(db, config)
    yield q
    await db.close()


@pytest.mark.asyncio
async def test_enqueue_dequeue(queue):
    """Basic enqueue and dequeue."""
    raw = b"From: a@b.com\r\nTo: c@d.com\r\n\r\nHello"
    eid, rejected = await queue.enqueue("a@b.com", ["c@d.com"], raw, "<test@localhost>")
    assert not rejected
    assert eid > 0

    email = await queue.dequeue_next()
    assert email is not None
    assert email.mail_from == "a@b.com"
    assert email.rcpt_to == ["c@d.com"]
    assert email.raw_message == raw


@pytest.mark.asyncio
async def test_capacity_limit(queue):
    """Queue should reject when full."""
    for i in range(5):
        _, rejected = await queue.enqueue(
            f"sender{i}@test.com", ["rcpt@test.com"],
            b"test", f"<msg{i}@test>"
        )
        assert not rejected

    # 6th should be rejected
    _, rejected = await queue.enqueue(
        "overflow@test.com", ["rcpt@test.com"],
        b"test", "<msg6@test>"
    )
    assert rejected


@pytest.mark.asyncio
async def test_count(queue):
    """Count should track emails correctly."""
    await queue.enqueue("a@b.com", ["c@d.com"], b"test", "<1>")
    await queue.enqueue("e@f.com", ["g@h.com"], b"test", "<2>")

    total = await queue.count()
    assert total == 2


@pytest.mark.asyncio
async def test_mark_sent(queue):
    """Marking sent should update status."""
    await queue.enqueue("a@b.com", ["c@d.com"], b"test", "<1>")
    email = await queue.dequeue_next()
    await queue.mark_sent(email.id)

    sent = await queue.count("sent")
    assert sent == 1


@pytest.mark.asyncio
async def test_transient_failure_retry(queue):
    """Transient failure should schedule retry."""
    await queue.enqueue("a@b.com", ["c@d.com"], b"test", "<1>")
    email = await queue.dequeue_next()
    await queue.mark_transient_failure(email.id, "421 Service busy", "421")

    failed = await queue.count("failed")
    assert failed == 1

    # Should not be immediately dequeuable (has next_retry_at)
    next_email = await queue.dequeue_next()
    assert next_email is None


@pytest.mark.asyncio
async def test_permanent_failure(queue):
    """Permanent failure should bounce."""
    await queue.enqueue("a@b.com", ["c@d.com"], b"test", "<1>")
    email = await queue.dequeue_next()
    await queue.mark_permanent_failure(email.id, "550 User unknown", "550")

    bounced = await queue.count("bounced")
    assert bounced == 1


@pytest.mark.asyncio
async def test_clear_queue(queue):
    """Clear should remove emails by status."""
    await queue.enqueue("a@b.com", ["c@d.com"], b"test", "<1>")
    await queue.enqueue("e@f.com", ["g@h.com"], b"test", "<2>")

    deleted = await queue.clear_queue("queued")
    assert deleted == 2
    assert await queue.count() == 0
