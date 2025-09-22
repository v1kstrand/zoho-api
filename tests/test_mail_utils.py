from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import csv
import email
from datetime import datetime, timezone
from typing import Iterator

import pytest

import app.mail_utils as mail_utils
import app.mailgun_util as mailgun_util
from app.mail_utils import message_body_text, move_message
from app.mailgun_util import (
    MailgunEventsClient,
    MailgunPerRecipient,
    append_stats_row,
    compute_day_stats,
)


def make_message(parts: Iterator[tuple[str, str, str]]):
    msg = email.message.EmailMessage()
    for maintype, subtype, payload in parts:
        msg.add_attachment(payload, maintype=maintype, subtype=subtype)
    return msg


def test_message_body_text_prefers_plaintext():
    msg = email.message.EmailMessage()
    msg.set_type("multipart/alternative")
    msg.add_alternative("<html><body><p>Hello</p></body></html>", subtype="html")
    msg.add_alternative("Hello", subtype="plain")

    assert message_body_text(msg).strip() == "Hello"


def test_message_body_text_falls_back_to_html():
    msg = email.message.EmailMessage()
    msg.set_type("multipart/alternative")
    msg.add_alternative("<html><body><p>Hello <b>World</b></p></body></html>", subtype="html")

    text = message_body_text(msg)
    assert "Hello" in text
    assert "World" in text


def test_move_message_calls_imap_operations(monkeypatch):
    calls = []

    class FakeIMAP:
        def __init__(self):
            self.created = []

        def create(self, mailbox):
            self.created.append(mailbox)

        def uid(self, *args):
            calls.append(args)

        def expunge(self):
            calls.append(("EXPUNGE",))

    imap = FakeIMAP()

    move_message(imap, "42", "Processed/Unsubscribe")

    assert "Processed/Unsubscribe" in imap.created
    assert ("COPY", "42", "Processed/Unsubscribe") in calls
    assert ("STORE", "42", "+FLAGS", "(\\Deleted)") in calls


def test_move_message_no_destination_noop():
    class FakeIMAP:
        def __init__(self):
            self.created = []
            self.calls = []

        def create(self, mailbox):
            self.created.append(mailbox)

        def uid(self, *args):
            self.calls.append(args)

    imap = FakeIMAP()
    move_message(imap, "1", None)

    assert imap.created == []
    assert getattr(imap, "calls") == []


class DummyResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")

    def json(self):
        return self._payload


@pytest.fixture()
def email_csv_path(tmp_path):
    target = Path("data/email/emails.csv")
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.read_bytes() if target.exists() else None
    yield target
    if backup is None:
        if target.exists():
            target.unlink()
    else:
        target.write_bytes(backup)


@pytest.fixture()
def stats_csv_path(tmp_path):
    target = Path("data/email/stats.csv")
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.read_bytes() if target.exists() else None
    yield target
    if backup is None:
        if target.exists():
            target.unlink()
    else:
        target.write_bytes(backup)


@pytest.fixture()
def fake_mailgun_client():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def fetch_events_single_page(self, event, begin_s, end_s, *, limit=100, extra=None):
            self.calls.append((event, begin_s, end_s, limit, extra))
            key = (event, (extra or {}).get("severity") if extra else None)
            if key == ("failed", "permanent"):
                return [
                    {
                        "recipient": "foo@example.com",
                        "timestamp": 30,
                        "delivery-status": {"code": "550", "message": "No such user"},
                        "message": {"headers": {"message-id": "<fail@id>"}},
                    }
                ]
            if key == ("failed", "temporary"):
                return [
                    {
                        "recipient": "bar@example.com",
                        "timestamp": 15,
                        "delivery-status": {"code": "421", "message": "Try again"},
                        "message": {"headers": {"message-id": "<temp@id>"}},
                    }
                ]
            if event == "clicked":
                return [{"recipient": "foo@example.com", "timestamp": 40}]
            if event == "opened":
                return [{"recipient": "bar@example.com", "timestamp": 16}]
            if event == "delivered":
                return [
                    {"recipient": "foo@example.com", "timestamp": 10},
                    {"recipient": "baz@example.com", "timestamp": 12},
                ]
            return []

    return FakeClient()


def test_mailgun_events_client_unauthorized(monkeypatch):
    captured = {}

    def fake_get(url, auth, params, timeout):
        captured["request"] = {"url": url, "auth": auth, "params": params, "timeout": timeout}
        return DummyResponse(401)

    monkeypatch.setattr(mailgun_util.requests, "get", fake_get)

    client = MailgunEventsClient(
        api_base="https://api.mailgun.net",
        domain="example.com",
        api_key="key-test",
    )

    with pytest.raises(PermissionError):
        client.fetch_events_single_page("delivered", "begin", "end")

    assert captured["request"]["url"] == "https://api.mailgun.net/v3/example.com/events"


def test_mailgun_events_client_fetch_success(monkeypatch):
    captured = {}

    def fake_get(url, auth, params, timeout):
        captured["url"] = url
        captured["auth"] = auth
        captured["params"] = params
        captured["timeout"] = timeout
        return DummyResponse(200, {"items": [{"id": 1}]})

    monkeypatch.setattr(mailgun_util.requests, "get", fake_get)

    client = MailgunEventsClient(
        api_base="https://api.mailgun.net",
        domain="example.com",
        api_key="key-test",
    )

    result = client.fetch_events_single_page(
        "failed",
        "begin",
        "end",
        limit=999,
        extra={"severity": "permanent"},
    )

    assert result == [{"id": 1}]
    assert captured["params"]["limit"] == 100  # Mailgun caps at 100 per request
    assert captured["params"]["severity"] == "permanent"
    assert captured["auth"] == ("api", "key-test")


def test_mailgun_per_recipient_compute_rows_for_day(fake_mailgun_client):
    per_recipient = MailgunPerRecipient(client=fake_mailgun_client)

    day = datetime(2024, 1, 5, tzinfo=timezone.utc)
    rows = per_recipient.compute_rows_for_day(day, tag_label="campaign")

    rows_by_recipient = {row["recipient"]: row for row in rows}
    foo = rows_by_recipient["foo@example.com"]
    assert foo["status"] == "failed_permanent"
    assert foo["smtp_code"] == "550"
    assert foo["smtp_message"] == "No such user"
    assert foo["message_id"] == "<fail@id>"
    assert foo["first_seen"] == "10"
    assert foo["last_seen"] == "40"
    assert foo["tag"] == "campaign"
    assert foo["date_utc"] == "2024-01-05"

    bar = rows_by_recipient["bar@example.com"]
    assert bar["status"] == "failed_temporary"
    assert bar["smtp_code"] == "421"
    assert bar["message_id"] == "<temp@id>"
    assert bar["first_seen"] == "15"
    assert bar["last_seen"] == "16"

    baz = rows_by_recipient["baz@example.com"]
    assert baz["status"] == "delivered"
    assert baz["first_seen"] == "12"
    assert baz["last_seen"] == "12"


def test_mailgun_per_recipient_upsert_real_csv(email_csv_path, fake_mailgun_client):
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
    with open(email_csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "date_utc": "2024-09-01",
                "tag": "previous",
                "recipient": "legacy@example.com",
                "status": "delivered",
                "smtp_code": "250",
                "smtp_message": "sent",
                "message_id": "<legacy@id>",
                "first_seen": "1",
                "last_seen": "1",
            }
        )

    per_recipient = MailgunPerRecipient(client=fake_mailgun_client, emails_path=str(email_csv_path))
    day = datetime(2024, 1, 5, tzinfo=timezone.utc)
    rows = per_recipient.compute_rows_for_day(day, tag_label="campaign")
    per_recipient.upsert_csv(rows, str(email_csv_path))

    with open(email_csv_path, newline="", encoding="utf-8") as handle:
        first_snapshot = list(csv.DictReader(handle))

    assert len(first_snapshot) == 4
    data = {row["recipient"]: row for row in first_snapshot}

    assert data["legacy@example.com"]["tag"] == "previous"
    assert data["foo@example.com"]["status"] == "failed_permanent"
    assert data["foo@example.com"]["smtp_code"] == "550"
    assert data["foo@example.com"]["first_seen"] == "10"
    assert data["bar@example.com"]["status"] == "failed_temporary"
    assert data["bar@example.com"]["smtp_code"] == "421"
    assert data["baz@example.com"]["status"] == "delivered"

    per_recipient.upsert_csv(rows, str(email_csv_path))

    with open(email_csv_path, newline="", encoding="utf-8") as handle:
        second_snapshot = list(csv.DictReader(handle))

    assert second_snapshot == first_snapshot



def test_append_stats_row_real_csv(stats_csv_path):
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
    with open(stats_csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "date_utc": "2024-09-01",
                "tag": "campaign",
                "failed_permanent": "1",
                "failed_temporary": "0",
                "dropped": "0",
                "rejected": "0",
                "delivered": "10",
                "not_delivered_total": "1",
                "delivery_rate": "0.9091",
            }
        )

    class FakeStatsClient:
        def fetch_events_single_page(self, event, begin_s, end_s, *, limit=100, extra=None):
            if event == "failed" and extra == {"severity": "permanent"}:
                return [{"recipient": "a", "severity": "permanent"}]
            if event == "failed":
                return [
                    {"recipient": "a", "severity": "permanent"},
                    {"recipient": "b", "severity": "temporary"},
                ]
            if event == "dropped":
                return [{"recipient": "c"}]
            if event == "rejected":
                return []
            if event == "delivered":
                return [{"recipient": "d"} for _ in range(7)]
            return []

    day = datetime(2024, 1, 5, tzinfo=timezone.utc)
    stats_row = compute_day_stats(day, tag_label="campaign_jan", client=FakeStatsClient())
    append_stats_row(str(stats_csv_path), stats_row)
    append_stats_row(str(stats_csv_path), stats_row)

    with open(stats_csv_path, newline="", encoding="utf-8") as handle:
        reader = list(csv.DictReader(handle))

    assert len(reader) == 2
    appended = next(row for row in reader if row["tag"] == "campaign_jan")
    assert appended["failed_permanent"] == "1"
    assert appended["failed_temporary"] == "1"
    assert appended["dropped"] == "1"
    assert appended["delivered"] == "7"
    assert appended["delivery_rate"] == "0.7000"

def test_ensure_mailbox_creates_folder():
    class FakeIMAP:
        def __init__(self):
            self.created = []

        def create(self, mailbox):
            self.created.append(mailbox)

    fake = FakeIMAP()
    mail_utils.ensure_mailbox(fake, 'Archive/2024')
    assert fake.created == ['Archive/2024']


def test_ensure_mailbox_swallows_errors():
    class NoisyIMAP:
        def create(self, mailbox):
            raise RuntimeError('already exists')

    mail_utils.ensure_mailbox(NoisyIMAP(), 'Inbox')  # should not raise
