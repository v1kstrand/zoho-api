import email
from email.header import Header

import pytest

from app.tasks import mail_bookings_trigger as bookings


def test_subject_decodes_header():
    msg = email.message.EmailMessage()
    msg['Subject'] = str(Header('VDS Discovery', 'utf-8'))
    assert bookings._subject(msg) == 'VDS Discovery'


def test_looks_like_booking_match():
    assert bookings._looks_like_booking('VDS Discovery Project between David Vikstrand and Jane Doe')
    assert not bookings._looks_like_booking('Random subject line')


def test_ensure_contact_returns_existing(monkeypatch):
    existing = {'email': 'alice@example.com', 'first_name': 'Alice'}
    monkeypatch.setattr(bookings, 'find_contact_by_email', lambda email: existing)
    monkeypatch.setattr(bookings, 'add_contact', lambda payload: pytest.fail('add_contact should not be called'))

    appt = {'customer_email': 'alice@example.com'}
    assert bookings._ensure_contact(appt) is existing


def test_ensure_contact_adds_new_contact(monkeypatch):
    created = {}

    def fake_add_contact(payload):
        created.update(payload)
        return {'email': payload['email'], 'first_name': payload.get('first_name')}

    monkeypatch.setattr(bookings, 'find_contact_by_email', lambda email: None)
    monkeypatch.setattr(bookings, 'add_contact', fake_add_contact)

    appt = {
        'customer_email': 'Bob@example.com',
        'customer_first_name': ' Bob ',
        'customer_last_name': 'Builder ',
    }
    result = bookings._ensure_contact(appt)

    assert created == {'email': 'bob@example.com', 'first_name': 'Bob', 'last_name': 'Builder'}
    assert result['email'] == 'bob@example.com'


def test_mark_contact_booked_updates(monkeypatch):
    called = {}

    def fake_update(email, payload):
        called['email'] = email
        called['payload'] = payload

    monkeypatch.setattr(bookings, 'update_contact', fake_update)

    bookings._mark_contact_booked({'email': 'user@example.com'})

    assert called == {'email': 'user@example.com', 'payload': {'stage': 'Booked'}}


def test_mark_contact_booked_no_email(monkeypatch):
    monkeypatch.setattr(bookings, 'update_contact', lambda *args, **kwargs: pytest.fail('should not update'))
    bookings._mark_contact_booked({})


class DummyIMAP:
    fail_login_attempts = 0

    def __init__(self, host):
        self.host = host
        self.selected = []
        self.created = []

    def login(self, user, password):
        if DummyIMAP.fail_login_attempts:
            DummyIMAP.fail_login_attempts -= 1
            raise RuntimeError('login failed')

    def select(self, mailbox):
        self.selected.append(mailbox)
        return 'OK', None

    def create(self, mailbox):
        self.created.append(mailbox)

    def logout(self):
        pass


def test_imap_connect_with_retry_success(monkeypatch):
    DummyIMAP.fail_login_attempts = 1
    created = []

    monkeypatch.setattr(bookings.imaplib, 'IMAP4_SSL', DummyIMAP)
    monkeypatch.setattr(bookings, 'ensure_mailbox', lambda imap, folder: created.append((imap, folder)))

    imap = bookings._imap_connect_with_retry('imap.test', 'user', 'pass', 'INBOX', ensure_folder='Archive', attempts=3, delay=0)

    assert isinstance(imap, DummyIMAP)
    assert imap.selected == ['INBOX']
    assert created == [(imap, 'Archive')]


def test_imap_connect_with_retry_exhausts_attempts(monkeypatch):
    class FailingIMAP(DummyIMAP):
        def login(self, user, password):
            raise RuntimeError('permanent failure')

    monkeypatch.setattr(bookings.imaplib, 'IMAP4_SSL', FailingIMAP)

    with pytest.raises(RuntimeError):
        bookings._imap_connect_with_retry('imap.test', 'user', 'pass', 'INBOX', attempts=2, delay=0)
