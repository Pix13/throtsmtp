"""Inbound SMTP handler using aiosmtpd."""

from __future__ import annotations

import logging
import uuid

from aiosmtpd.controller import Controller
from aiosmtpd.smtp import SMTP, Session, Envelope

from .config import Config
from .queue_manager import QueueManager

logger = logging.getLogger(__name__)


class RelayHandler:
    """aiosmtpd handler that enqueues received mail."""

    def __init__(self, queue: QueueManager, config: Config):
        self.queue = queue
        self.config = config

    async def handle_DATA(self, server: SMTP, session: Session, envelope: Envelope) -> str:
        """Receive email data and enqueue it."""
        try:
            mail_from = envelope.mail_from or "unknown"
            rcpt_to = list(envelope.rcpt_tos) if envelope.rcpt_tos else []

            if not rcpt_to:
                logger.warning("No recipients in envelope from %s", mail_from)
                return "550 5.1.1 No recipients specified"

            # Raw message bytes
            raw_message = envelope.original_content or b""

            # Generate or extract Message-ID
            message_id = self._extract_message_id(raw_message)
            if not message_id:
                message_id = f"<{uuid.uuid4()}@{self.config.local.hostname}>"

            # Enqueue
            email_id, rejected = await self.queue.enqueue(
                mail_from=mail_from,
                rcpt_to=rcpt_to,
                raw_message=raw_message,
                message_id=message_id,
            )

            if rejected:
                return "452 4.2.2 Mailbox full — queue at capacity, try again later"

            logger.info(
                "Received email id=%d from=%s to=%s size=%d bytes",
                email_id, mail_from, rcpt_to, len(raw_message),
            )
            return "250 2.0.0 OK: message accepted and queued"

        except Exception:
            logger.exception("Error handling DATA from %s", session.peer)
            return "451 4.3.0 Internal server error"

    @staticmethod
    def _extract_message_id(raw: bytes) -> str:
        """Try to extract Message-ID from raw message headers."""
        try:
            text = raw.decode("utf-8", errors="replace")
            for line in text.split("\n"):
                if line.lower().startswith("message-id:"):
                    mid = line.split(":", 1)[1].strip()
                    if mid and not mid.startswith("<"):
                        mid = f"<{mid}>"
                    return mid
        except Exception:
            pass
        return ""


def create_controller(
    handler: RelayHandler,
    config: Config,
    port: int | None = None,
) -> Controller:
    """Create an aiosmtpd Controller with the given handler.

    If *port* is not given, falls back to *config.local.port*.
    Uses *config.local.host* as the socket bind address and
    *config.local.hostname* as the SMTP greeting hostname.
    """
    ctrl = Controller(
        handler,
        hostname=config.local.host,
        port=port if port is not None else config.local.port,
        ready_timeout=10,
    )
    # Set the SMTP greeting hostname (EHLO/HELO response)
    handler.smtpd_hostname = config.local.hostname
    return ctrl
