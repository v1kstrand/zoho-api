from __future__ import annotations

import csv
import email
import imaplib
import json
import os
import re
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Optional

import requests


_HTML_SANITIZE_PATTERNS = {
    "scripts": re.compile(r"(?is)<(script|style).*?>.*?</\1>"),
    "br": re.compile(r"(?is)<br\s*/?>"),
    "tags": re.compile(r"(?is)<[^>]+>")
}


def ensure_mailbox(imap: imaplib.IMAP4, mailbox: Optional[str]) -> None:
    """Create mailbox when missing; ignore errors if it already exists."""
    if not mailbox:
        return
    try:
        imap.create(mailbox)
    except Exception:
        pass


def _html_to_text(html: str) -> str:
    cleaned = _HTML_SANITIZE_PATTERNS["scripts"].sub("", html)
    cleaned = _HTML_SANITIZE_PATTERNS["br"].sub("\n", cleaned)
    cleaned = _HTML_SANITIZE_PATTERNS["tags"].sub(" ", cleaned)
    return cleaned


def message_body_text(msg: email.message.Message) -> str:
    """Best-effort plaintext extraction from a MIME message."""
    plain_parts: list[str] = []
    html_parts: list[str] = []

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        content_type = part.get_content_type()
        try:
            payload = part.get_payload(decode=True) or b""
        except Exception:
            payload = b""

        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except Exception:
            text = payload.decode("utf-8", errors="replace")

        if content_type == "text/plain":
            plain_parts.append(text)
        elif content_type == "text/html":
            html_parts.append(_html_to_text(text))

    for candidate in plain_parts:
        if "\n" in candidate or len(candidate) > 20:
            return candidate
    if plain_parts:
        return plain_parts[0]

    for candidate in html_parts:
        if "\n" in candidate or len(candidate) > 20:
            return candidate
    if html_parts:
        return html_parts[0]

    try:
        return msg.as_string()
    except Exception:
        return ""


def move_message(imap: imaplib.IMAP4, uid: str, dest: Optional[str]) -> None:
    """Copy a message to ``dest`` and flag it deleted in the current mailbox."""
    if not dest:
        return
    try:
        ensure_mailbox(imap, dest)
        imap.uid("COPY", uid, dest)
        imap.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
        imap.expunge()
    except Exception as exc:
        print(f"[imap] move failed for uid={uid}: {exc}")


BREVO_API_KEY = os.getenv("BREVO_API_KEY")
BREVO_TEMPLATE_ID = int(os.getenv("BREVO_TEMPLATE_ID", "2"))
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")
MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN", "for.vdsai.se")
MAILGUN_TEMPLATE = os.getenv("MAILGUN_TEMPLATE_NAME", "outreach_v1")
MAILGUN_API_BASE = os.getenv("MAILGUN_API_BASE", "https://api.eu.mailgun.net")

ACCUM_DIR = "data/email"
DEFAULT_EMAILS_PATH = os.path.join(ACCUM_DIR, "emails.csv")
DEFAULT_STATS_PATH = os.path.join(ACCUM_DIR, "stats.csv")


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
) -> requests.Response:
    """Send a template-based message through Mailgun."""
    api_key = MAILGUN_API_KEY
    if not api_key:
        raise RuntimeError("MAILGUN_API_KEY is not configured.")
    template_name, template_params = template or (MAILGUN_TEMPLATE, {})
    data = {
        "from": "Vikstrand Deep Solutions <info@vdsai.se>",
        "to": recipients,
        "template": template_name,
        "t:variables": json.dumps(template_params),
    }
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
        emails_path: str = DEFAULT_EMAILS_PATH,
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
        if delivery_status.get("message"):
            record["smtp_message"] = delivery_status.get("message")
        if message_id:
            record["message_id"] = message_id

    def compute_rows_for_day(
        self,
        day_utc: datetime,
        *,
        tag_label: str | None = None,
    ) -> list[dict]:
        """Build per-recipient status for the given UTC day."""
        day = day_utc.date()
        begin = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
        begin_str, end_str = rfc2822(begin), rfc2822(end)

        records: dict[tuple[str, str], dict] = {}

        def record_for(recipient: str) -> dict:
            key = (recipient, tag_label or "")
            if key not in records:
                records[key] = {
                    "date_utc": day.strftime("%Y-%m-%d"),
                    "tag": tag_label or "",
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
                if recipient:
                    self._touch(record_for(recipient), event, status)

        rows: list[dict] = []
        for record in records.values():
            record["status"] = record["status"] or "unknown"
            record["first_seen"] = "" if record["first_seen"] is None else str(record["first_seen"])
            record["last_seen"] = "" if record["last_seen"] is None else str(record["last_seen"])
            rows.append(record)

        return rows

    def upsert_csv(self, rows: list[dict], path: str | None = None) -> None:
        path = path or self.emails_path
        directory = os.path.dirname(path) or "."
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

        existing: list[dict] = []
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                existing.extend(reader)
        else:
            with open(path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()

        index = {
            (row["date_utc"], row["tag"], row["recipient"]): idx
            for idx, row in enumerate(existing)
        }

        for record in rows:
            key = (record["date_utc"], record["tag"], record["recipient"])
            serialised = {field: str(record.get(field, "")) for field in fieldnames}
            if key in index:
                existing[index[key]] = serialised
            else:
                existing.append(serialised)

        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing)

        print(f"Upserted {len(rows)} per-recipient rows -> {path}")


def compute_day_stats(
    day_utc: datetime,
    *,
    tag_label: str | None = None,
    client: MailgunEventsClient | None = None,
) -> dict:
    """Return aggregate counts for the UTC day (00:00..23:59:59)."""
    client = client or MailgunEventsClient()
    day = day_utc.date()
    begin = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
    begin_str, end_str = rfc2822(begin), rfc2822(end)

    failed_all = client.fetch_events_single_page("failed", begin_str, end_str, limit=100)
    failed_perm = client.fetch_events_single_page(
        "failed",
        begin_str,
        end_str,
        limit=100,
        extra={"severity": "permanent"},
    )
    failed_temp = [event for event in failed_all if event.get("severity") == "temporary"]

    dropped = client.fetch_events_single_page("dropped", begin_str, end_str, limit=100)
    rejected = client.fetch_events_single_page("rejected", begin_str, end_str, limit=100)
    delivered = client.fetch_events_single_page("delivered", begin_str, end_str, limit=100)

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


def _stats_existing_keys(csv_path: str) -> set[tuple[str, str]]:
    """Return (date_utc, tag) tuples already present in the stats CSV."""
    if not os.path.exists(csv_path):
        return set()
    keys: set[tuple[str, str]] = set()
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            keys.add((row.get("date_utc", ""), row.get("tag", "")))
    return keys


def append_stats_row(csv_path: str, row: dict) -> None:
    """Append a stats row unless (date_utc, tag) already exists."""
    directory = os.path.dirname(csv_path) or "."
    ensure_dir(directory)
    exists = os.path.exists(csv_path)
    keyset = _stats_existing_keys(csv_path)
    key = (row["date_utc"], row["tag"])
    if key in keyset:
        print(f"Skipped stats (already present): date={row['date_utc']} tag='{row['tag']}'")
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
    print(f"Appended stats -> {csv_path}: {row}")


"""if __name__ == "__main__":
    mailgun_client = MailgunEventsClient()
    per_recipient = MailgunPerRecipient(mailgun_client)

    tag_label = "outreach_sep19"
    today_utc = datetime.now(timezone.utc)

    per_email_rows = per_recipient.compute_rows_for_day(today_utc, tag_label=tag_label)
    per_recipient.upsert_csv(per_email_rows, DEFAULT_EMAILS_PATH)

    stats_row = compute_day_stats(today_utc, tag_label=tag_label, client=mailgun_client)
    append_stats_row(DEFAULT_STATS_PATH, stats_row)
"""
