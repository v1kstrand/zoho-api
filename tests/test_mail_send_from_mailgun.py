import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import app.tasks  # ensure parent package is registered

MODULE_PATH = Path('app/tasks/mail_send_from_mailgun_with details.py')
spec = importlib.util.spec_from_file_location('app.tasks.mailgun_send_task', MODULE_PATH)
mail_task = importlib.util.module_from_spec(spec)
mail_task.__package__ = 'app.tasks'
sys.modules['app.tasks.mailgun_send_task'] = mail_task
spec.loader.exec_module(mail_task)

def test_parse_key_value_pairs():
    mapping = mail_task._parse_key_value_pairs(['k1=v1', 'k2=v2'], option='--foo')
    assert mapping == {'k1': 'v1', 'k2': 'v2'}

    with pytest.raises(ValueError):
        mail_task._parse_key_value_pairs(['novalue'], option='--foo')

    with pytest.raises(ValueError):
        mail_task._parse_key_value_pairs(['foo='], option='--foo')

    with pytest.raises(ValueError):
        mail_task._parse_key_value_pairs(['x=y', 'x=z'], option='--foo')

def test_build_template_params_success():
    contact = {'first': 'Alice', 'last': 'Smith'}
    params = mail_task._build_template_params(
        contact,
        column_map={'first_name': 'first'},
        static_params={'cta': 'book'},
        email='alice@example.com',
    )
    assert params == {'cta': 'book', 'first_name': 'Alice'}

def test_build_template_params_missing_contact():
    with pytest.raises(ValueError) as exc:
        mail_task._build_template_params(
            None,
            column_map={'first_name': 'first'},
            static_params={},
            email='missing@example.com',
        )
    assert 'Contact not found' in str(exc.value)

def test_build_template_params_missing_column():
    contact = {'first': 'Alice'}
    with pytest.raises(ValueError) as exc:
        mail_task._build_template_params(
            contact,
            column_map={'last_name': 'last'},
            static_params={},
            email='alice@example.com',
        )
    assert "Contact column 'last'" in str(exc.value)

def test_send_campaign_dry_run(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first': 'Alice'},
        'b@example.com': {'first': 'Bob'},
    }

    def failing_events_client():
        raise AssertionError('events client should not be constructed during dry-run')

    def failing_send_message(*args, **kwargs):
        raise AssertionError('send_message should not be called during dry-run')

    mail_task.send_campaign(
        template='welcome',
        emails=['a@example.com', 'b@example.com'],
        column_map={'first_name': 'first'},
        static_params={'cta': 'book'},
        tag_label='ttl',
        dry_run=True,
        contact_lookup=contacts.get,
        events_client_factory=failing_events_client,
        per_recipient_factory=lambda client: None,
        send_message=failing_send_message,
    )

    out = capsys.readouterr().out
    assert "would send template 'welcome'" in out
    assert 'Alice' in out and 'Bob' in out

class DummyEventsClient:
    def __init__(self):
        self.created = True

class DummyPerRecipient:
    def __init__(self, client, rows):
        self.client = client
        self.rows = rows
        self.upserted = []
        self.received_day = None
        self.received_tag = None

    def compute_rows_for_day(self, day, *, tag_label=None):
        self.received_day = day
        self.received_tag = tag_label
        return list(self.rows)

    def upsert_csv(self, rows):
        self.upserted.extend(rows)

def test_send_campaign_real_flow(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first': 'Alice'},
        'c@example.com': {'first': 'Carol'},
    }

    sent_payloads = []

    def fake_send_message(recipients, template):
        sent_payloads.append((tuple(recipients), template))

    rows = [
        {'recipient': 'a@example.com', 'status': 'delivered'},
        {'recipient': 'b@example.com', 'status': 'failed'},
    ]

    dummy_client = DummyEventsClient()
    dummy_pr = DummyPerRecipient(dummy_client, rows)

    def fake_events_client():
        return dummy_client

    def fake_per_recipient(client):
        assert client is dummy_client
        return dummy_pr

    fixed_now = datetime(2024, 1, 5, tzinfo=timezone.utc)

    class FakeDateTime:
        @staticmethod
        def now(tz):
            assert tz is timezone.utc
            return fixed_now

    monkeypatch.setattr(mail_task, 'datetime', FakeDateTime)

    mail_task.send_campaign(
        template='welcome',
        emails=['a@example.com', 'c@example.com'],
        column_map={'first_name': 'first'},
        static_params={},
        tag_label='campaign',
        dry_run=False,
        contact_lookup=contacts.get,
        events_client_factory=fake_events_client,
        per_recipient_factory=fake_per_recipient,
        send_message=fake_send_message,
    )

    assert sent_payloads == [
        (('a@example.com',), ('welcome', {'first_name': 'Alice'})),
        (('c@example.com',), ('welcome', {'first_name': 'Carol'})),
    ]

    assert dummy_pr.upserted == [{'recipient': 'a@example.com', 'status': 'delivered'}]
    assert dummy_pr.received_day == fixed_now
    assert dummy_pr.received_tag == 'campaign'

    out = capsys.readouterr().out
    assert 'updated delivery log' in out


def test_send_campaign_skips_missing_contact(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first': 'Alice'},
    }
    sent_payloads = []

    mail_task.send_campaign(
        template='welcome',
        emails=['a@example.com', 'missing@example.com'],
        column_map={'first_name': 'first'},
        static_params={},
        tag_label='campaign',
        dry_run=False,
        contact_lookup=contacts.get,
        events_client_factory=lambda: DummyEventsClient(),
        per_recipient_factory=lambda client: DummyPerRecipient(client, []),
        send_message=lambda recipients, template: sent_payloads.append((tuple(recipients), template)),
    )

    assert len(sent_payloads) == 1
    assert sent_payloads[0][0] == ('a@example.com',)
    out = capsys.readouterr().out
    assert '[skip] Contact not found' in out
