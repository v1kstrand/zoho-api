# app/tasks/mail_get_send_info.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable, Optional

from dotenv import load_dotenv

from ..mailgun_util import (
    BATCH_STATS_PATH,
    EMAIL_STATS_PATH,
    MAILGUN_TAGS_EXCLUDE,
    MailgunEventsClient,
    MailgunPerRecipient,
    append_batch_stats_row,
    compute_batch_stats,
)



load_dotenv()

__all__ = [
    "collect_mailgun_day",
    "build_argument_parser",
    "main",
]


def _parse_day(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid day '{value}'. Use YYYY-MM-DD."
        ) from exc


def collect_mailgun_day(
    *,
    day_utc: datetime,
    events_client_factory=MailgunEventsClient,
    per_recipient_factory=MailgunPerRecipient,
    compute_batch_stats=compute_batch_stats,
    append_batch_stats=append_batch_stats_row,
) -> None:
    client = events_client_factory()
    per_recipient = per_recipient_factory(client, emails_path=EMAIL_STATS_PATH)

    rows = per_recipient.compute_rows_for_day(day_utc)
    
    if rows:
        per_recipient.upsert_csv(rows, EMAIL_STATS_PATH)
    else:
        print(f"[info] no per-recipient events for {day_utc.date()}")
        return
    
    seen = set()
    for row in rows:
        tag = row.get("tag")
        if tag and tag not in seen and tag not in MAILGUN_TAGS_EXCLUDE:
            seen.add(tag)
            stats_row = compute_batch_stats(day_utc, tag_label=tag, client=client)
            append_batch_stats(BATCH_STATS_PATH, stats_row)

def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Mailgun delivery details and append CSV logs."
    )
    parser.add_argument(
        "--day",
        metavar="YYYY-MM-DD",
        help="UTC day to collect (defaults to today).",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    day_utc = _parse_day(args.day)
    collect_mailgun_day( day_utc=day_utc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
