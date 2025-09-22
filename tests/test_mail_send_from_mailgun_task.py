from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

import app.tasks.mail_send_from_mailgun as task


def test_resolve_stage_known():
    config = task.resolve_stage('intro')
    assert config.template == 'v2'
    assert config.column_map == {'first_name': 'first_name', 'auto_number': 'auto_number'}
    assert config.static_params == {}


def test_resolve_stage_unknown():
    with pytest.raises(ValueError):
        task.resolve_stage('missing')


def test_build_template_params_populates_values():
    contact = {'first_name': 'Alice', 'auto_number': '42'}
    params = task._build_template_params(
        contact,
        column_map={'first_name': 'first_name'},
        static_params={'cta': 'book'},
        email='alice@example.com',
    )
    assert params == {'cta': 'book', 'first_name': 'Alice'}


def test_build_template_params_missing_contact():
    with pytest.raises(ValueError):
        task._build_template_params(
            None,
            column_map={'first_name': 'first_name'},
            static_params={},
            email='missing@example.com',
        )


def test_build_template_params_missing_column():
    contact = {'first_name': 'Alice'}
    with pytest.raises(ValueError):
        task._build_template_params(
            contact,
            column_map={'auto': 'auto_number'},
            static_params={},
            email='alice@example.com',
        )


def test_send_campaign_dry_run(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first_name': 'Alice', 'auto_number': '1'},
        'b@example.com': {'first_name': 'Bob', 'auto_number': '2'},
    }

    calls = []

    def fake_send_message(*args, **kwargs):
        calls.append((args, kwargs))

    config = task.StageConfig(
        template='v2',
        column_map={'first_name': 'first_name'},
        static_params={'cta': 'cta'},
    )

    task.send_campaign(
        stage='intro',
        emails=['a@example.com', 'b@example.com'],
        config=config,
        dry_run=True,
        contact_lookup=contacts.get,
        send_message=fake_send_message,
    )

    out = capsys.readouterr().out
    assert "[dry-run] stage 'intro' would send template 'v2'" in out
    assert "Alice" in out and "Bob" in out
    assert calls == []


def test_send_campaign_real_flow(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first_name': 'Alice', 'auto_number': '1'},
        'c@example.com': {'first_name': 'Carol', 'auto_number': '2'},
    }

    sent = []

    def fake_send_message(recipients, template):
        sent.append((tuple(recipients), template))

    config = task.StageConfig(
        template='v2',
        column_map={'first_name': 'first_name'},
        static_params={},
    )

    task.send_campaign(
        stage='intro',
        emails=['a@example.com', 'c@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=fake_send_message,
    )

    assert sent == [
        (('a@example.com',), ('v2', {'first_name': 'Alice'})),
        (('c@example.com',), ('v2', {'first_name': 'Carol'})),
    ]
    out = capsys.readouterr().out
    assert "[mailgun] stage 'intro' sent template 'v2'" in out


def test_send_campaign_skips_missing_contact(monkeypatch, capsys):
    contacts = {
        'a@example.com': {'first_name': 'Alice', 'auto_number': '1'},
    }
    sent = []

    config = task.StageConfig(
        template='v2',
        column_map={'first_name': 'first_name'},
        static_params={},
    )

    task.send_campaign(
        stage='intro',
        emails=['a@example.com', 'missing@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=lambda recipients, template: sent.append((tuple(recipients), template)),
    )

    assert sent == [(('a@example.com',), ('v2', {'first_name': 'Alice'}))]
    out = capsys.readouterr().out
    assert '[skip] Contact not found for missing@example.com' in out


def test_send_campaign_reports_no_messages(monkeypatch, capsys):
    contacts = {}
    config = task.StageConfig(
        template='v2',
        column_map={'first_name': 'first_name'},
        static_params={},
    )

    task.send_campaign(
        stage='intro',
        emails=['missing@example.com'],
        config=config,
        dry_run=False,
        contact_lookup=contacts.get,
        send_message=lambda *args, **kwargs: None,
    )

    out = capsys.readouterr().out
    assert '[info] no messages sent' in out


def test_main_invokes_send_campaign(monkeypatch):
    called = {}

    def fake_send_campaign(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr(task, 'send_campaign', fake_send_campaign)
    assert task.main(['intro', 'a@example.com', '--dry-run']) == 0
    assert called['stage'] == 'intro'
    assert called['emails'] == ['a@example.com']
    assert called['dry_run'] is True
    assert isinstance(called['config'], task.StageConfig)


def test_main_unknown_stage_exits(monkeypatch):
    with pytest.raises(SystemExit):
        task.main(['unknown', 'a@example.com'])
