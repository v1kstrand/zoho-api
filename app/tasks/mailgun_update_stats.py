# app/tasks/mail_get_send_info.py
from __future__ import annotations

from datetime import datetime, timezone
from dotenv import load_dotenv

from ..mailgun_util import (
    MAILGUN_TAGS_EXCLUDE,
    MailgunEventsClient,
    MailgunPerRecipient,
    append_batch_stats_row,
    compute_batch_stats,
)

load_dotenv()

def collect_mailgun_day(
    day_utc: datetime,
    events_client_factory=MailgunEventsClient,
    per_recipient_factory=MailgunPerRecipient,
    calc_batch_stats=compute_batch_stats,
    append_batch_stats=append_batch_stats_row,
) -> None:
    client = events_client_factory()
    per_recipient = per_recipient_factory(client)

    rows = per_recipient.compute_rows_for_day(day_utc)
    
    if rows:
        per_recipient.upsert_csv(rows)
    else:
        print(f"[info] no per-recipient events for {day_utc.date()}")
        return
    
    seen = set()
    for row in rows:
        tag = row.get("tag")
        if tag and tag not in seen and tag not in MAILGUN_TAGS_EXCLUDE:
            seen.add(tag)
            stats_row = calc_batch_stats(day_utc, tag_label=tag, client=client)
            append_batch_stats(stats_row)


if __name__ == "__main__":
    collect_mailgun_day(day_utc=datetime.now(timezone.utc))
