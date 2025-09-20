import email

import pytest

from app.tasks import mail_unsub_poll as unsub


def make_message(subject="", from_addr="User <user@example.com>"):
    msg = email.message.EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_addr
    return msg


def test_addr_from_extracts_address():
    msg = make_message(from_addr='Example User <User@Example.com>')
    assert unsub._addr_from(msg) == 'user@example.com'

    msg = make_message(from_addr='plain@example.com')
    assert unsub._addr_from(msg) == 'plain@example.com'

    msg = make_message(from_addr='')
    assert unsub._addr_from(msg) is None


def test_looks_like_stop_by_subject():
    msg = make_message(subject='Please UNSUBSCRIBE me')
    assert unsub._looks_like_stop(msg, body='Hello')

    msg = make_message(subject='Weekly update')
    assert not unsub._looks_like_stop(msg, body='Hello world')


def test_looks_like_stop_by_body():
    msg = make_message(subject='Just saying hi')
    body = "\nHi there\nstop\nThanks"
    assert unsub._looks_like_stop(msg, body)

    msg = make_message(subject='Hello')
    body = '> quoted\n  \nno keywords here'
    assert not unsub._looks_like_stop(msg, body)


def test_process_once_requires_credentials(monkeypatch):
    monkeypatch.setattr(unsub, 'IMAP_USER', None)
    monkeypatch.setattr(unsub, 'IMAP_PASS', None)
    with pytest.raises(RuntimeError):
        unsub.process_once()
