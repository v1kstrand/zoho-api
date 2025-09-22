from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

import app.tasks.mailgun_send_campaign as mail_task


def test_resolve_stage_known():
    config = mail_task.resolve_stage('intro')
    assert config.template == 'v2'
    assert config.column_map == {'first_name': 'first_name', 'auto_number': 'auto_number'}
    assert config.static_params == {}


def test_resolve_stage_unknown():
    with pytest.raises(ValueError):
        mail_task.resolve_stage('missing')


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

    def failing_send_message(*args, **kwargs):
        raise AssertionError('send_message should not be called during dry-run')

    config = mail_task.StageConfig(
        template='welcome',
        column_map={'first_name': 'first'},
        static_params={'cta': 'book'},
    )

    mail_task.send_campaign(
        stage='welcome',
        emails=['a@example.com', 'b@example.com'],
        config=config,
        dry_run=True,
        contact_lookup=contacts.get,
        send_message=failing_send_message,
    )

    out = capsys.readouterr().out
    assert "would send template 'welcome'" in out
    assert 'Alice' in out and 'Bob' in out


def test_send_campaign_real_flow(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first': 'Alice'},
        'c@example.com': {'first': 'Carol'},
    }

    sent_payloads = []

    def fake_send_message(recipients, template):
        sent_payloads.append((tuple(recipients), template))

    config = mail_task.StageConfig(
        template='welcome',
        column_map={'first_name': 'first'},
        static_params={},
    )

    mail_task.send_campaign(
        stage='campaign',
        emails=['a@example.com', 'c@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=fake_send_message,
    )

    assert sent_payloads == [
        (('a@example.com',), ('welcome', {'first_name': 'Alice'})),
        (('c@example.com',), ('welcome', {'first_name': 'Carol'})),
    ]

    out = capsys.readouterr().out
    assert "stage 'campaign' sent template 'welcome'" in out


def test_send_campaign_skips_missing_contact(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first': 'Alice'},
    }
    sent_payloads = []

    config = mail_task.StageConfig(
        template='welcome',
        column_map={'first_name': 'first'},
        static_params={},
    )

    mail_task.send_campaign(
        stage='campaign',
        emails=['a@example.com', 'missing@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=lambda recipients, template: sent_payloads.append((tuple(recipients), template)),
    )

    assert len(sent_payloads) == 1
    assert sent_payloads[0][0] == ('a@example.com',)
    out = capsys.readouterr().out
    assert '[skip] Contact not found for missing@example.com' in out


def test_send_campaign_reports_no_deliveries(capsys):
    contacts = {}

    config = mail_task.StageConfig(
        template='welcome',
        column_map={'first_name': 'first'},
        static_params={},
    )

    mail_task.send_campaign(
        stage='campaign',
        emails=['missing@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=lambda *args, **kwargs: None,
    )

    out = capsys.readouterr().out
    assert '[skip] Contact not found for missing@example.com' in out
    assert '[info] no messages sent' in out
