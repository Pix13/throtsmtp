"""SQLite database schema and initialization for the email queue."""

from __future__ import annotations

import aiosqlite


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS emails (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      TEXT NOT NULL,
    enqueued_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    mail_from       TEXT NOT NULL,
    rcpt_to         TEXT NOT NULL,       -- JSON array of recipients
    raw_message     BLOB NOT NULL,       -- full raw SMTP DATA payload
    status          TEXT NOT NULL DEFAULT 'queued',  -- queued | sending | sent | failed | bounced
    retry_count     INTEGER NOT NULL DEFAULT 0,
    next_retry_at   TEXT,               -- ISO timestamp, NULL if ready
    error_message   TEXT,
    upstream_response TEXT              -- last upstream response code + message
);

CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(status);
CREATE INDEX IF NOT EXISTS idx_emails_retry ON emails(next_retry_at) WHERE status = 'queued';
"""


async def init_db(db_path: str) -> aiosqlite.Connection:
    """Initialize the database and return a connection."""
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    await db.executescript(SCHEMA_SQL)
    return db
