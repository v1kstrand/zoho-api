from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Optional
import time

import requests
from dotenv import load_dotenv

from app.api_client import get_contact_field

load_dotenv()

BREVO_API_KEY = os.environ["BREVO_API_KEY"]
MAILGUN_API_KEY = os.environ["MAILGUN_API_KEY"]
MAILGUN_DOMAIN = "for.vdsai.se"
MAILGUN_API_BASE = "https://api.eu.mailgun.net"
MAILGUN_TAGS_EXCLUDE = {t.strip() for t in os.environ["MAILGUN_TAGS_EXCLUDE"].split(",") if t}

MAIL_DATA_DIR = os.environ["MAIL_UTIL_DATADIR"]
MAIL_UTIL_BATCH = os.environ["MAIL_UTIL_BATCH"]
MAIL_UTIL_EMAIL = os.environ["MAIL_UTIL_EMAIL"]

def _optional_path(base: Optional[str], name: Optional[str]) -> Optional[str]:
    if not base or not name:
        return None
    return os.path.join(base, name)


BATCH_STATS_PATH = _optional_path(MAIL_DATA_DIR, MAIL_UTIL_BATCH)
EMAIL_STATS_PATH = _optional_path(MAIL_DATA_DIR, MAIL_UTIL_EMAIL)


__all__ = [
    "BREVO_API_KEY",
    "MAILGUN_API_KEY",
    "MAILGUN_DOMAIN",
    "MAILGUN_API_BASE",
    "BATCH_STATS_PATH",
    "EMAIL_STATS_PATH",
    "fetch_brevo_template_html",
    "send_mailgun_message",
    "ensure_dir",
    "rfc2822",
    "MailgunEventsClient",
    "MailgunPerRecipient",
    "compute_batch_stats",
    "append_batch_stats_row",
]


def fetch_brevo_template_html(template_id: int) -> tuple[str, str]:
    """Return subject and HTML content for a Brevo template."""
    if not BREVO_API_KEY:
        raise RuntimeError("BREVO_API_KEY is not configured.")
    url = f"https://api.brevo.com/v3/smtp/templates/{template_id}"
    response = requests.get(
        url,
        headers={"api-key": BREVO_API_KEY, "accept": "application/json"},
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("subject", ""), data.get("htmlContent", "")


def send_mailgun_message(
    recipients: list[str],
    template: tuple[str, dict] | None = None,
    tag: str | None = None,
) -> requests.Response:
    """Send a template-based message through Mailgun."""
    api_key = MAILGUN_API_KEY
    if not api_key:
        raise RuntimeError("MAILGUN_API_KEY is not configured.")
    template_name, template_params = template
    
    filtered_recipients = []
    for recipient in recipients:
        if get_contact_field(recipient, "unsub").lower() == "true":
            print(f"[skip] unsubscribed for {recipient}")
            continue
        filtered_recipients.append(recipient)
    recipients = filtered_recipients
    if not recipients:
        return
    
    data = {
        "from": "Vikstrand Deep Solutions <info@vdsai.se>",
        "to": recipients,
        "template": template_name,
        "t:variables": json.dumps(template_params),
    }
    if tag:
        data["o:tag"] = tag
    response = requests.post(
        f"{MAILGUN_API_BASE}/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", api_key),
        data=data,
        timeout=20,
    )
    response.raise_for_status()
    return response


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def rfc2822(dt: datetime) -> str:
    return format_datetime(dt.astimezone(timezone.utc))


class MailgunEventsClient:
    """Thin wrapper around Mailgun's events API."""

    def __init__(
        self,
        api_base: str | None = None,
        domain: str | None = None,
        api_key: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.api_base = (api_base or MAILGUN_API_BASE).rstrip("/")
        self.domain = domain or MAILGUN_DOMAIN
        self.api_key = api_key or MAILGUN_API_KEY
        self.auth = ("api", self.api_key) if self.api_key else None
        self.timeout = timeout

        if not self.domain:
            raise RuntimeError("MAILGUN_DOMAIN is not configured.")
        if not self.auth:
            raise RuntimeError("MAILGUN_API_KEY is not configured.")

    def fetch_events_single_page(
        self,
        event: str,
        begin_s: str,
        end_s: str,
        *,
        limit: int = 100,
        extra: dict | None = None,
    ) -> list[dict]:
        """Return a single page of events (Mailgun maximum is 100)."""
        url = f"{self.api_base}/v3/{self.domain}/events"
        params = {"event": event, "begin": begin_s, "end": end_s, "limit": min(limit, 100)}
        if extra:
            params.update(extra)
        response = requests.get(
            url,
            auth=self.auth,
            params=params,
            timeout=self.timeout,
        )
        if response.status_code == 401:
            raise PermissionError(
                "Unauthorized: verify Mailgun Private API key and region-specific base URL."
            )
        response.raise_for_status()
        return response.json().get("items", [])


class MailgunPerRecipient:
    """Build and persist per-recipient delivery logs."""

    STATUS_ORDER = {
        "complained": 7,
        "failed_permanent": 6,
        "dropped": 5,
        "rejected": 4,
        "failed_temporary": 3,
        "clicked": 2,
        "opened": 1,
        "delivered": 0,
    }

    def __init__(
        self,
        client: MailgunEventsClient | None = None,
        emails_path: str | None = EMAIL_STATS_PATH,
    ) -> None:
        self.client = client or MailgunEventsClient()
        self.emails_path = emails_path

    @classmethod
    def _pick_higher(cls, existing: str | None, candidate: str | None) -> str | None:
        if existing is None:
            return candidate
        if candidate is None:
            return existing
        return existing if cls.STATUS_ORDER[existing] >= cls.STATUS_ORDER[candidate] else candidate

    def _touch(self, record: dict, event: dict, status: str) -> None:
        timestamp = event.get("timestamp")
        delivery_status = event.get("delivery-status") or {}
        message_headers = (event.get("message") or {}).get("headers") or {}
        message_id = message_headers.get("message-id")

        record["status"] = self._pick_higher(record.get("status"), status)

        if record.get("first_seen") is None or (timestamp is not None and timestamp < record["first_seen"]):
            record["first_seen"] = timestamp
        if record.get("last_seen") is None or (timestamp is not None and timestamp > record["last_seen"]):
            record["last_seen"] = timestamp

        if delivery_status.get("code"):
            record["smtp_code"] = delivery_status.get("code")
        message = delivery_status.get("message")
        if message:
            record["smtp_message"] = " ".join(message.splitlines())
        if message_id:
            record["message_id"] = message_id

        event_tags = event.get("tags") or []
        if event_tags and not record.get("tag"):
            record["tag"] = event_tags[0]

    def compute_rows_for_day(
        self,
        day_utc: datetime,
    ) -> list[dict]:
        """Build per-recipient status for the given UTC day."""
        day = day_utc.date()
        begin = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
        begin_str, end_str = rfc2822(begin), rfc2822(end)

        records: dict[tuple[str, str], dict] = {}

        def record_for(recipient: str, tag_value: str) -> dict:
            key = (recipient, tag_value)
            if key not in records:
                records[key] = {
                    "date_utc": day.strftime("%Y-%m-%d"),
                    "tag": tag_value,
                    "recipient": recipient,
                    "status": None,
                    "smtp_code": "",
                    "smtp_message": "",
                    "message_id": "",
                    "first_seen": None,
                    "last_seen": None,
                }
            return records[key]

        fetch_plan = [
            ("complained", "complained", None),
            ("failed_permanent", "failed", {"severity": "permanent"}),
            ("dropped", "dropped", None),
            ("rejected", "rejected", None),
            ("failed_temporary", "failed", {"severity": "temporary"}),
            ("clicked", "clicked", None),
            ("opened", "opened", None),
            ("delivered", "delivered", None),
        ]

        for status, event_name, extra in fetch_plan:
            events = self.client.fetch_events_single_page(
                event_name,
                begin_str,
                end_str,
                extra=extra,
            )
            for event in events:
                recipient = event.get("recipient")
                if not recipient:
                    continue
                event_tags = event.get("tags") or []
                tag_value = event_tags[0] if event_tags else ""
                record = record_for(recipient, tag_value)
                self._touch(record, event, status)

        rows: list[dict] = []
        for record in records.values():
            record["status"] = record["status"] or "unknown"
            record["first_seen"] = "" if record["first_seen"] is None else str(record["first_seen"])
            record["last_seen"] = "" if record["last_seen"] is None else str(record["last_seen"])
            rows.append(record)

        return rows

    def upsert_csv(self, rows: list[dict], path: str | None = None) -> None:
        target_path = path or self.emails_path
        if not target_path:
            raise ValueError("Provide a target CSV path for MailgunPerRecipient")

        directory = os.path.dirname(target_path) or "."
        ensure_dir(directory)

        fieldnames = [
            "date_utc",
            "tag",
            "recipient",
            "status",
            "smtp_code",
            "smtp_message",
            "message_id",
            "first_seen",
            "last_seen",
        ]

        existing_tags: set[str] = set()
        file_exists = os.path.exists(target_path)
        if file_exists:
            with open(target_path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                existing_tags = {row.get("tag", "") for row in reader}

        incoming_by_tag: dict[str, list[dict[str, str]]] = {}
        for record in rows:
            tag_value = record.get("tag", "")
            if tag_value in MAILGUN_TAGS_EXCLUDE:
                continue
            
            serialised = {field: str(record.get(field, "")) for field in fieldnames}
            incoming_by_tag.setdefault(tag_value, []).append(serialised)

        skipped_tags = sorted({tag for tag in incoming_by_tag if tag in existing_tags})
        new_rows: list[dict[str, str]] = []
        for tag_value, serialised_rows in incoming_by_tag.items():
            if tag_value in existing_tags:
                continue
            new_rows.extend(serialised_rows)

        if not new_rows:
            if skipped_tags:
                formatted = ", ".join(tag or "<none>" for tag in skipped_tags)
                print(
                    f"[info] per-recipient tag(s) already recorded in {target_path}: {formatted}"
                )
            else:
                print(f"[info] no per-recipient rows to append -> {target_path}")
            return

        with open(target_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)

        appended_tags = sorted({row["tag"] for row in new_rows})
        appended_display = ", ".join(tag or "<none>" for tag in appended_tags)
        print(
            f"Appended {len(new_rows)} per-recipient rows for tag(s) {appended_display} -> {target_path}"
        )
        if skipped_tags:
            skipped_display = ", ".join(tag or "<none>" for tag in skipped_tags)
            print(f"[info] skipped existing per-recipient tag(s): {skipped_display}")




def compute_batch_stats(
    day_utc: datetime,
    tag_label: str | None = None,
    client: MailgunEventsClient | None = None,
) -> dict:
    """Return aggregate counts for the UTC day (00:00..23:59:59)."""
    client = client or MailgunEventsClient()
    day = day_utc.date()
    begin = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
    begin_str, end_str = rfc2822(begin), rfc2822(end)

    def _filter_by_tag(events: list[dict]) -> list[dict]:
        if not tag_label:
            return events
        filtered: list[dict] = []
        for event in events:
            tags = event.get("tags") or []
            if not tags or tag_label not in tags:
                continue
            filtered.append(event)
        return filtered

    failed_all = _filter_by_tag(
        client.fetch_events_single_page("failed", begin_str, end_str, limit=100)
    )
    failed_perm = _filter_by_tag(
        client.fetch_events_single_page(
            "failed",
            begin_str,
            end_str,
            limit=100,
            extra={"severity": "permanent"},
        )
    )
    failed_temp = [event for event in failed_all if event.get("severity") == "temporary"]

    dropped = _filter_by_tag(
        client.fetch_events_single_page("dropped", begin_str, end_str, limit=100)
    )
    rejected = _filter_by_tag(
        client.fetch_events_single_page("rejected", begin_str, end_str, limit=100)
    )
    delivered = _filter_by_tag(
        client.fetch_events_single_page("delivered", begin_str, end_str, limit=100)
    )

    not_delivered_total = len(failed_perm) + len(failed_temp) + len(dropped) + len(rejected)
    delivered_count = len(delivered)
    denominator = delivered_count + not_delivered_total
    delivery_rate = (delivered_count / denominator) if denominator > 0 else 0.0

    return {
        "date_utc": day.strftime("%Y-%m-%d"),
        "tag": tag_label or "",
        "failed_permanent": len(failed_perm),
        "failed_temporary": len(failed_temp),
        "dropped": len(dropped),
        "rejected": len(rejected),
        "delivered": delivered_count,
        "not_delivered_total": not_delivered_total,
        "delivery_rate": f"{delivery_rate:.4f}",
    }


def _stats_existing_tags(csv_path: str) -> set[str]:
    """Return tag labels already present in the stats CSV."""
    if not os.path.exists(csv_path):
        return set()
    tags: set[str] = set()
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            tags.add(row.get("tag", ""))
    return tags


def append_batch_stats_row(row: dict, csv_path: str=BATCH_STATS_PATH) -> None:
    """Append a stats row unless the tag is already present."""
    directory = os.path.dirname(csv_path) or "."
    ensure_dir(directory)
    exists = os.path.exists(csv_path)
    existing_tags = _stats_existing_tags(csv_path)
    tag_value = row.get("tag", "")
    if tag_value in existing_tags:
        tag_display = tag_value or "<none>"
        print(f"Skipped stats (tag already present): tag='{tag_display}'")
        return

    fieldnames = [
        "date_utc",
        "tag",
        "failed_permanent",
        "failed_temporary",
        "dropped",
        "rejected",
        "delivered",
        "not_delivered_total",
        "delivery_rate",
    ]

    with open(csv_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)
        time.sleep(0.1)
    print(f"Appended stats -> {csv_path}: {row}")