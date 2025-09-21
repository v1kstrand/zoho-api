from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import datetime, timezone

import pytest

import app.tasks.mail_get_send_info as get_info


def test_collect_mailgun_day(monkeypatch):
    rows_captured = []
    stats_row = {"date_utc": "2024-01-01", "tag": "tag"}
    client_instance = object()

    def fake_events_client():
        return client_instance

    class FakePerRecipient:
        def __init__(self, client, emails_path):
            assert client is client_instance
            assert emails_path == 'per.csv'
            self.emails_path = emails_path

        def compute_rows_for_day(self, day, *, tag_label=None):
            assert day == datetime(2024, 1, 1, tzinfo=timezone.utc)
            assert tag_label == 'tag'
            return [{'recipient': 'a@example.com'}]

        def upsert_csv(self, rows, path):
            rows_captured.append((rows, path))

    def fake_per_recipient(client, emails_path):
        return FakePerRecipient(client, emails_path)

    def fake_compute_stats(day, *, tag_label=None, client=None):
        assert client is client_instance
        return stats_row

    stats_calls = []

    def fake_append_stats(path, row):
        stats_calls.append((path, row))

    get_info.collect_mailgun_day(
        day_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
        tag_label='tag',
        emails_path='per.csv',
        stats_path='stats.csv',
        events_client_factory=fake_events_client,
        per_recipient_factory=fake_per_recipient,
        compute_stats=fake_compute_stats,
        append_stats=fake_append_stats,
    )

    assert rows_captured == [([{'recipient': 'a@example.com'}], 'per.csv')]
    assert stats_calls == [('stats.csv', stats_row)]


def test_collect_mailgun_day_handles_empty(monkeypatch, capsys):
    def fake_events_client():
        return object()

    class FakePerRecipient:
        def __init__(self, client, emails_path):
            self.client = client
            self.emails_path = emails_path

        def compute_rows_for_day(self, day, *, tag_label=None):
            return []

        def upsert_csv(self, rows, path):
            raise AssertionError('upsert should not be called')

    def fake_per_recipient(client, emails_path):
        return FakePerRecipient(client, emails_path)

    def fake_compute_stats(day, *, tag_label=None, client=None):
        return {"date_utc": "2024-01-01", "tag": tag_label or ''}

    stats_calls = []

    def fake_append_stats(path, row):
        stats_calls.append((path, row))

    get_info.collect_mailgun_day(
        day_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
        tag_label=None,
        emails_path='per.csv',
        stats_path='stats.csv',
        events_client_factory=fake_events_client,
        per_recipient_factory=fake_per_recipient,
        compute_stats=fake_compute_stats,
        append_stats=fake_append_stats,
    )

    out = capsys.readouterr().out
    assert "no per-recipient events" in out
    assert stats_calls == [('stats.csv', {'date_utc': '2024-01-01', 'tag': ''})]


def test_main_requires_paths(monkeypatch):
    # ensure module level defaults are None
    monkeypatch.setattr(get_info, 'BATCH_STATS_PATH', None)
    monkeypatch.setattr(get_info, 'EMAIL_STATS_PATH', None)

    with pytest.raises(SystemExit) as exc:
        get_info.main([])
    assert exc.value.code == 2  # argparse error


def test_main_uses_env_defaults(monkeypatch):
    day = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def fake_parse_day(value):
        assert value == '2024-01-01'
        return day

    calls = {}

    def fake_collect(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(get_info, '_parse_day', fake_parse_day)
    monkeypatch.setattr(get_info, 'collect_mailgun_day', fake_collect)
    monkeypatch.setattr(get_info, 'BATCH_STATS_PATH', 'per.csv')
    monkeypatch.setattr(get_info, 'EMAIL_STATS_PATH', 'stats.csv')

    assert get_info.main(['--day', '2024-01-01', '--tag', 'intro']) == 0
    assert calls['day_utc'] is day
    assert calls['tag_label'] == 'intro'
    assert calls['emails_path'] == 'per.csv'
    assert calls['stats_path'] == 'stats.csv'
