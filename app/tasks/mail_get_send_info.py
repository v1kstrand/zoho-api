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
]


def collect_mailgun_day(
    day_utc: datetime,
    events_client_factory=MailgunEventsClient,
    per_recipient_factory=MailgunPerRecipient,
    calc_batch_stats=compute_batch_stats,
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
            stats_row = calc_batch_stats(day_utc, tag_label=tag, client=client)
            append_batch_stats(BATCH_STATS_PATH, stats_row)


if __name__ == "__main__":
    collect_mailgun_day(day_utc=datetime.now(timezone.utc))
