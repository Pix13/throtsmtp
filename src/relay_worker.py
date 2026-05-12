"""Single-threaded relay worker with throttling and retry logic."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate
from pathlib import Path

import aiosmtplib

from .config import Config, QueueConfig, ThrottleConfig, UpstreamConfig
from .queue_manager import EmailRecord, QueueManager

logger = logging.getLogger(__name__)


class RelayWorker:
    """Consumes emails from the queue and relays them one-by-one."""

    def __init__(self, queue: QueueManager, config: Config):
        self.queue = queue
        self.config = config
        self._running = False
        self._lock = asyncio.Lock()
        # Pause marker file — created by throt-admin pause, removed by resume
        self._pause_file = Path(config.queue.db_path).with_suffix(".paused")

    async def start(self) -> None:
        """Start the relay loop. Runs until stopped."""
        self._running = True
        logger.info(
            "Relay worker started (throttle: %d-%ds, max_retries: %d)",
            self.config.throttle.min_delay,
            self.config.throttle.max_delay,
            self.config.queue.max_retries,
        )

        while self._running:
            try:
                await self._process_one()
            except asyncio.CancelledError:
                logger.info("Relay worker cancelled")
                break
            except Exception:
                logger.exception("Unexpected error in relay loop")
                await asyncio.sleep(5)

        logger.info("Relay worker stopped")

    async def stop(self) -> None:
        """Signal the relay worker to stop."""
        self._running = False

    async def _process_one(self) -> None:
        """Process a single email through the relay pipeline."""
        # Check pause marker — if set, skip processing and wait
        if self._pause_file.exists():
            await asyncio.sleep(2)
            return

        async with self._lock:
            # Dequeue next email
            email = await self.queue.dequeue_next()
            if email is None:
                # Queue empty — wait before polling again
                await asyncio.sleep(5)
                return

            # Apply throttle jitter
            delay = random.uniform(
                self.config.throttle.min_delay,
                self.config.throttle.max_delay,
            )
            logger.info(
                "Throttle: waiting %.1fs before sending email id=%d",
                delay, email.id,
            )
            await asyncio.sleep(delay)

            # Relay
            await self._relay_email(email)

    async def _relay_email(self, email: EmailRecord) -> None:
        """Send a single email to the upstream server."""
        try:
            await self._send(email)
            await self.queue.mark_sent(email.id)

        except aiosmtplib.errors.SMTPDataError as e:
            # aiosmtplib SMTPDataError has .code (int) and .message (str)
            code_str = str(e.code)
            if code_str.startswith("4"):
                await self.queue.mark_transient_failure(email.id, str(e), code_str)
            else:
                response = f"{e.code} {e.message}"
                await self.queue.mark_permanent_failure(email.id, str(e), response)
                if self.config.bounce.enabled:
                    await self._send_bounce(email, str(e), response)

        except aiosmtplib.errors.SMTPRecipientsRefused as e:
            # Recipient rejected — permanent
            response = str(e)
            await self.queue.mark_permanent_failure(email.id, str(e), response)
            if self.config.bounce.enabled:
                await self._send_bounce(email, str(e), response)

        except aiosmtplib.errors.SMTPConnectError as e:
            # Connection refused — transient
            await self.queue.mark_transient_failure(email.id, str(e), "CONNECT_ERROR")

        except aiosmtplib.errors.TimeoutError as e:
            # Timeout — transient
            await self.queue.mark_transient_failure(email.id, str(e), "TIMEOUT")

        except aiosmtplib.errors.SMTPServerDisconnected as e:
            # Server disconnected — transient
            await self.queue.mark_transient_failure(email.id, str(e), "DISCONNECTED")

        except aiosmtplib.errors.SMTPAuthenticationError as e:
            # Auth failure — log as critical, don't retry individual emails
            logger.critical("Upstream authentication failed: %s", e)
            await self.queue.mark_transient_failure(email.id, str(e), "AUTH_ERROR")

        except aiosmtplib.errors.SMTPException as e:
            # Generic SMTP error — check code
            code = getattr(e, 'code', 0)
            if isinstance(code, (int, str)):
                code_str = str(code)
                if code_str.startswith("4"):
                    await self.queue.mark_transient_failure(email.id, str(e), code_str)
                elif code_str.startswith("5"):
                    await self.queue.mark_permanent_failure(email.id, str(e), code_str)
                    if self.config.bounce.enabled:
                        await self._send_bounce(email, str(e), code_str)
                else:
                    await self.queue.mark_transient_failure(email.id, str(e), str(code))
            else:
                await self.queue.mark_transient_failure(email.id, str(e), "UNKNOWN")

        except Exception as e:
            # Unknown error — treat as transient
            logger.exception("Unexpected relay error for email id=%d", email.id)
            await self.queue.mark_transient_failure(email.id, str(e), "INTERNAL_ERROR")

    async def _send(self, email: EmailRecord) -> None:
        """Connect to upstream and send the raw email."""
        up = self.config.upstream

        if up.tls == "ssl":
            smtp = aiosmtplib.SMTP(
                hostname=up.host,
                port=up.port,
                use_tls=True,
                timeout=up.timeout,
            )
        else:
            smtp = aiosmtplib.SMTP(
                hostname=up.host,
                port=up.port,
                use_tls=False,
                timeout=up.timeout,
            )

        try:
            await smtp.connect()
            if up.tls == "starttls":
                await smtp.starttls()

            if up.username and up.password:
                await smtp.login(up.username, up.password)

            # Send raw message bytes using sendmail
            await smtp.sendmail(email.mail_from, email.rcpt_to, email.raw_message)
            logger.info("Sent email id=%d to upstream", email.id)

        finally:
            await smtp.quit()

    async def _send_bounce(self, email: EmailRecord, error: str, response: str) -> None:
        """Send a Non-Delivery Report to the original sender."""
        bounce_from = self.config.bounce.from_addr
        bounce_to = email.mail_from

        # Parse display name from bounce address
        bounce_addr = bounce_from.split(">")[0].split("<")[-1] if "@" in bounce_from else bounce_from

        subject = f"Mail delivery failed: Message-ID {email.message_id}"
        body = (
            f"This is an automatically generated Non-Delivery Report.\n\n"
            f"The following message could not be delivered:\n\n"
            f"  From: {email.mail_from}\n"
            f"  To: {', '.join(email.rcpt_to)}\n"
            f"  Message-ID: {email.message_id}\n"
            f"  Queued: {email.enqueued_at}\n\n"
            f"Failure details:\n\n"
            f"  Error: {error}\n"
            f"  Upstream response: {response}\n\n"
            f"The message has been removed from the queue.\n"
        )

        msg = MIMEText(body)
        msg["From"] = bounce_from
        msg["To"] = bounce_to
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg["Auto-Submitted"] = "auto-replied"
        msg["X-Failed-Recipients"] = ", ".join(email.rcpt_to)

        try:
            up = self.config.upstream
            if up.tls == "ssl":
                smtp = aiosmtplib.SMTP(hostname=up.host, port=up.port, use_tls=True, timeout=up.timeout)
            else:
                smtp = aiosmtplib.SMTP(hostname=up.host, port=up.port, use_tls=False, timeout=up.timeout)

            await smtp.connect()
            if up.tls == "starttls":
                await smtp.starttls()
            if up.username and up.password:
                await smtp.login(up.username, up.password)
            await smtp.sendmail(bounce_from, [bounce_to], msg.as_string().encode())
            await smtp.quit()
            logger.info("Bounce sent for email id=%d to %s", email.id, bounce_to)

        except Exception:
            logger.exception("Failed to send bounce for email id=%d", email.id)
