"""CLI admin tool for queue inspection and management."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .config import load_config
from .db import init_db
from .queue_manager import QueueManager


async def show_stats(queue: QueueManager) -> None:
    """Display queue statistics."""
    total = await queue.count()
    queued = await queue.count("queued")
    sending = await queue.count("sending")
    sent = await queue.count("sent")
    failed = await queue.count("failed")
    bounced = await queue.count("bounced")

    print(f"\n  Queue Statistics")
    print(f"  {'=' * 40}")
    print(f"  Total:     {total}")
    print(f"  Queued:    {queued}")
    print(f"  Sending:   {sending}")
    print(f"  Sent:      {sent}")
    print(f"  Failed:    {failed}")
    print(f"  Bounced:   {bounced}")
    print()


async def list_emails(queue: QueueManager, status: str | None, limit: int, offset: int) -> None:
    """List emails in the queue."""
    emails = await queue.list_emails(status=status, limit=limit, offset=offset)

    if not emails:
        label = f"with status={status}" if status else ""
        print(f"\n  No emails found {label}.")
        return

    label = f" (status={status})" if status else ""
    print(f"\n  Emails{label} (showing {len(emails)}):")
    print(f"  {'=' * 80}")

    for e in emails:
        rcpt = ", ".join(e["rcpt_to"])
        error = f"  err={e['error_message']}" if e.get("error_message") else ""
        resp = f"  resp={e['upstream_response']}" if e.get("upstream_response") else ""
        print(f"  [{e['id']:>5}] {e['status']:>8} | {e['enqueued_at']}")
        print(f"           from={e['mail_from']}")
        print(f"           to={rcpt}")
        print(f"           msg={e['message_id']} retries={e['retry_count']}{error}{resp}")
        print()


async def clear_queue(queue: QueueManager, status: str, confirm: bool) -> None:
    """Clear emails with the given status."""
    count = await queue.count(status)
    if count == 0:
        print(f"\n  No emails with status={status} to clear.")
        return

    if not confirm:
        print(f"\n  Would delete {count} emails with status={status}.")
        print(f"  Use --yes to confirm.")
        return

    deleted = await queue.clear_queue(status)
    print(f"\n  Deleted {deleted} emails with status={status}.")


async def prune(queue: QueueManager, confirm: bool) -> None:
    """Remove old sent and bounced emails."""
    sent = await queue.count("sent")
    bounced = await queue.count("bounced")
    total = sent + bounced

    if total == 0:
        print("\n  No sent or bounced emails to prune.")
        return

    print(f"\n  Would prune {sent} sent + {bounced} bounced = {total} emails.")
    if not confirm:
        print("  Use --yes to confirm.")
        return

    s = await queue.clear_queue("sent")
    b = await queue.clear_queue("bounced")
    print(f"  Pruned {s} sent + {b} bounced emails.")


def pause_relay(db_path: str) -> None:
    """Create the pause marker file to stop the relay worker."""
    pause_file = Path(db_path).with_suffix(".paused")
    pause_file.touch()
    print(f"  Relay paused. Marker file: {pause_file}")
    print("  The server will continue accepting emails but stop sending them.")
    print("  Run 'throt-admin resume' to resume sending.")


def resume_relay(db_path: str) -> None:
    """Remove the pause marker file to resume the relay worker."""
    pause_file = Path(db_path).with_suffix(".paused")
    if pause_file.exists():
        pause_file.unlink()
        print(f"  Relay resumed. Marker file removed: {pause_file}")
    else:
        print("  Relay was not paused (no marker file found).")


async def run_command(args: argparse.Namespace) -> None:
    """Execute the admin command."""
    config = load_config(args.config)

    # pause/resume don't need the database
    if args.command == "pause":
        pause_relay(config.queue.db_path)
        return

    if args.command == "resume":
        resume_relay(config.queue.db_path)
        return

    db = await init_db(config.queue.db_path)
    queue = QueueManager(db, config.queue)

    if args.command == "stats":
        await show_stats(queue)

    elif args.command == "list":
        await list_emails(queue, args.status, args.limit, args.offset)

    elif args.command == "clear":
        await clear_queue(queue, args.status, args.yes)

    elif args.command == "prune":
        await prune(queue, args.yes)

    await db.close()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="throt-admin",
        description="Admin tool for Throttled SMTP Relay queue management",
    )
    parser.add_argument("--config", "-c", help="Path to config.yaml")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # stats
    subparsers.add_parser("stats", help="Show queue statistics")

    # list
    list_parser = subparsers.add_parser("list", help="List queued emails")
    list_parser.add_argument("--status", "-s", choices=["queued", "sending", "sent", "failed", "bounced"],
                             help="Filter by status")
    list_parser.add_argument("--limit", "-n", type=int, default=50, help="Max results (default: 50)")
    list_parser.add_argument("--offset", "-o", type=int, default=0, help="Skip N results")

    # clear
    clear_parser = subparsers.add_parser("clear", help="Clear emails by status")
    clear_parser.add_argument("--status", "-s", default="queued",
                              choices=["queued", "sending", "sent", "failed", "bounced"],
                              help="Status to clear (default: queued)")
    clear_parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")

    # prune
    prune_parser = subparsers.add_parser("prune", help="Remove old sent and bounced emails")
    prune_parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")

    # pause
    subparsers.add_parser("pause", help="Pause email sending (server keeps accepting emails)")

    # resume
    subparsers.add_parser("resume", help="Resume email sending after pause")

    args = parser.parse_args()
    asyncio.run(run_command(args))


if __name__ == "__main__":
    main()
