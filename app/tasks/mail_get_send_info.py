# app/tasks/mail_get_send_info.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Iterable, Optional

from dotenv import load_dotenv

from ..mailgun_util import (
    BATCH_STATS_PATH,
    EMAIL_STATS_PATH,
    MailgunEventsClient,
    MailgunPerRecipient,
    append_stats_row,
    compute_day_stats,
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
    tag_label: Optional[str],
    emails_path: str,
    stats_path: str,
    events_client_factory=MailgunEventsClient,
    per_recipient_factory=MailgunPerRecipient,
    compute_stats=compute_day_stats,
    append_stats=append_stats_row,
) -> None:
    client = events_client_factory()
    per_recipient = per_recipient_factory(client, emails_path=emails_path)

    rows = per_recipient.compute_rows_for_day(day_utc, tag_label=tag_label)
    if rows:
        per_recipient.upsert_csv(rows, emails_path)
    else:
        print(
            f"[info] no per-recipient events for {day_utc.date()}"
            + (f" tag '{tag_label}'" if tag_label else "")
        )

    stats_row = compute_stats(day_utc, tag_label=tag_label, client=client)
    append_stats(stats_path, stats_row)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Mailgun delivery details and append CSV logs."
    )
    parser.add_argument(
        "--day",
        metavar="YYYY-MM-DD",
        help="UTC day to collect (defaults to today).",
    )
    parser.add_argument(
        "--tag",
        dest="tag_label",
        default=None,
        help="Optional Mailgun tag label to filter recordings.",
    )
    parser.add_argument(
        "--emails-csv",
        dest="emails_csv",
        default=None,
        help="Path for per-recipient CSV (overrides MAIL_UTIL_DATADIR/MAIL_UTIL_BATCH).",
    )
    parser.add_argument(
        "--stats-csv",
        dest="stats_csv",
        default=None,
        help="Path for per-day stats CSV (overrides MAIL_UTIL_DATADIR/MAIL_UTIL_EMAIL).",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    day_utc = _parse_day(args.day)
    emails_path = args.emails_csv or BATCH_STATS_PATH
    stats_path = args.stats_csv or EMAIL_STATS_PATH

    if not emails_path or not stats_path:
        parser.error(
            "Set MAIL_UTIL_DATADIR/MAIL_UTIL_BATCH/MAIL_UTIL_EMAIL or supply --emails-csv/--stats-csv."
        )

    collect_mailgun_day(
        day_utc=day_utc,
        tag_label=args.tag_label,
        emails_path=emails_path,
        stats_path=stats_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
