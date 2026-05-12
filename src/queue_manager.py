"""Persistent FIFO queue manager backed by SQLite."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiosqlite

from .config import QueueConfig

logger = logging.getLogger(__name__)


class EmailRecord:
    """Represents a queued email."""

    def __init__(
        self,
        id: int,
        message_id: str,
        enqueued_at: str,
        mail_from: str,
        rcpt_to: list[str],
        raw_message: bytes,
        status: str,
        retry_count: int,
        next_retry_at: Optional[str],
        error_message: Optional[str],
        upstream_response: Optional[str],
    ):
        self.id = id
        self.message_id = message_id
        self.enqueued_at = enqueued_at
        self.mail_from = mail_from
        self.rcpt_to = rcpt_to
        self.raw_message = raw_message
        self.status = status
        self.retry_count = retry_count
        self.next_retry_at = next_retry_at
        self.error_message = error_message
        self.upstream_response = upstream_response


class QueueManager:
    """Async queue manager with SQLite persistence."""

    def __init__(self, db: aiosqlite.Connection, config: QueueConfig):
        self.db = db
        self.config = config

    async def enqueue(
        self,
        mail_from: str,
        rcpt_to: list[str],
        raw_message: bytes,
        message_id: str,
    ) -> tuple[int, bool]:
        """Add email to queue. Returns (id, rejected)."""
        # Check capacity
        count = await self.count("queued")
        if count >= self.config.max_size:
            logger.warning("Queue full (%d), rejecting from=%s", count, mail_from)
            return 0, True

        rcpt_json = json.dumps(rcpt_to)
        now = datetime.now(timezone.utc).isoformat()

        cursor = await self.db.execute(
            """INSERT INTO emails (message_id, enqueued_at, mail_from, rcpt_to, raw_message, status)
               VALUES (?, ?, ?, ?, ?, 'queued')""",
            (message_id, now, mail_from, rcpt_json, raw_message),
        )
        await self.db.commit()
        email_id = cursor.lastrowid
        logger.info("Enqueued email id=%d from=%s to=%s size=%d", email_id, mail_from, rcpt_to, len(raw_message))
        return email_id, False

    async def dequeue_next(self) -> Optional[EmailRecord]:
        """Get the next email ready for sending. Marks it as 'sending'."""
        now = datetime.now(timezone.utc).isoformat()

        # First, promote any retriable emails whose next_retry_at has passed
        await self.db.execute(
            """UPDATE emails SET status = 'queued'
               WHERE status = 'failed' AND next_retry_at <= ?""",
            (now,),
        )
        await self.db.commit()

        # Get oldest queued email
        row = await self.db.execute(
            """SELECT id, message_id, enqueued_at, mail_from, rcpt_to,
                      raw_message, status, retry_count, next_retry_at,
                      error_message, upstream_response
               FROM emails
               WHERE status = 'queued'
               ORDER BY enqueued_at ASC
               LIMIT 1"""
        )
        record = await row.fetchone()
        if record is None:
            return None

        # Mark as sending
        await self.db.execute("UPDATE emails SET status = 'sending' WHERE id = ?", (record[0],))
        await self.db.commit()

        logger.info("Dequeued email id=%d for relay", record[0])
        rec = EmailRecord(*record)
        rec.rcpt_to = json.loads(rec.rcpt_to) if isinstance(rec.rcpt_to, str) else rec.rcpt_to
        return rec

    async def mark_sent(self, email_id: int) -> None:
        """Mark email as successfully sent."""
        await self.db.execute(
            "UPDATE emails SET status = 'sent' WHERE id = ?",
            (email_id,),
        )
        await self.db.commit()
        logger.info("Email id=%d marked as sent", email_id)

    async def mark_transient_failure(
        self,
        email_id: int,
        error: str,
        upstream_response: str,
    ) -> None:
        """Schedule retry for transient (4xx) failure with exponential backoff."""
        row = await self.db.execute(
            "SELECT retry_count FROM emails WHERE id = ?",
            (email_id,),
        )
        record = await row.fetchone()
        if record is None:
            return
        retry_count = record[0] + 1

        if retry_count >= self.config.max_retries:
            # Give up — mark as bounced
            await self.db.execute(
                """UPDATE emails SET status = 'bounced', retry_count = ?,
                   error_message = ?, upstream_response = ?
                   WHERE id = ?""",
                (retry_count, error, upstream_response, email_id),
            )
            await self.db.commit()
            logger.error(
                "Email id=%d bounced after %d retries: %s [%s]",
                email_id, retry_count, error, upstream_response,
            )
            return

        # Exponential backoff: base * 2^(retry_count-1), capped
        delay = min(self.config.retry_base * (2 ** (retry_count - 1)), self.config.retry_cap)
        next_retry = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()

        await self.db.execute(
            """UPDATE emails SET status = 'failed', retry_count = ?,
               next_retry_at = ?, error_message = ?, upstream_response = ?
               WHERE id = ?""",
            (retry_count, next_retry, error, upstream_response, email_id),
        )
        await self.db.commit()
        logger.warning(
            "Email id=%d transient failure (retry %d/%d in %ds): %s [%s]",
            email_id, retry_count, self.config.max_retries, delay, error, upstream_response,
        )

    async def mark_permanent_failure(
        self,
        email_id: int,
        error: str,
        upstream_response: str,
    ) -> None:
        """Mark email as bounced due to permanent (5xx) failure."""
        await self.db.execute(
            """UPDATE emails SET status = 'bounced',
               error_message = ?, upstream_response = ?
               WHERE id = ?""",
            (error, upstream_response, email_id),
        )
        await self.db.commit()
        logger.error(
            "Email id=%d permanent failure: %s [%s]",
            email_id, error, upstream_response,
        )

    async def count(self, status: Optional[str] = None) -> int:
        """Count emails, optionally filtered by status."""
        if status:
            row = await self.db.execute(
                "SELECT COUNT(*) FROM emails WHERE status = ?",
                (status,),
            )
        else:
            row = await self.db.execute("SELECT COUNT(*) FROM emails")
        record = await row.fetchone()
        return record[0] if record else 0

    async def list_emails(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List emails for admin display."""
        if status:
            query = "SELECT id, message_id, enqueued_at, mail_from, rcpt_to, status, retry_count, error_message, upstream_response FROM emails WHERE status = ? ORDER BY enqueued_at DESC LIMIT ? OFFSET ?"
            rows = await self.db.execute(query, (status, limit, offset))
        else:
            query = "SELECT id, message_id, enqueued_at, mail_from, rcpt_to, status, retry_count, error_message, upstream_response FROM emails ORDER BY enqueued_at DESC LIMIT ? OFFSET ?"
            rows = await self.db.execute(query, (limit, offset))

        records = await rows.fetchall()
        result = []
        for r in records:
            result.append({
                "id": r[0],
                "message_id": r[1],
                "enqueued_at": r[2],
                "mail_from": r[3],
                "rcpt_to": json.loads(r[4]) if r[4] else [],
                "status": r[5],
                "retry_count": r[6],
                "error_message": r[7],
                "upstream_response": r[8],
            })
        return result

    async def clear_queue(self, status: str = "queued") -> int:
        """Clear emails with given status. Returns count of deleted emails."""
        cursor = await self.db.execute(
            "DELETE FROM emails WHERE status = ?",
            (status,),
        )
        await self.db.commit()
        deleted = cursor.rowcount
        logger.info("Cleared %d emails with status=%s", deleted, status)
        return deleted

    async def get_email(self, email_id: int) -> Optional[EmailRecord]:
        """Get a specific email by ID."""
        row = await self.db.execute(
            """SELECT id, message_id, enqueued_at, mail_from, rcpt_to,
                      raw_message, status, retry_count, next_retry_at,
                      error_message, upstream_response
               FROM emails WHERE id = ?""",
            (email_id,),
        )
        record = await row.fetchone()
        if record is None:
            return None
        return EmailRecord(*record)
