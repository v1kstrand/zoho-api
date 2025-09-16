import types
import pytest

from app.tasks import mail_bookings_trigger as bookings


def make_contact(contact_id: str):
    return {"id": contact_id}


def test_is_already_booked_true(monkeypatch):
    monkeypatch.setattr(bookings, "search_contact_by_email", lambda email: make_contact("123"))
    monkeypatch.setattr(bookings, "list_records_by_contact_id", lambda cid, fields=None: [{"Stage": "Booked"}])

    appt = {"customer_email": "user@example.com"}

    assert bookings._is_already_booked(appt) is True


def test_is_already_booked_false_when_no_records(monkeypatch):
    monkeypatch.setattr(bookings, "search_contact_by_email", lambda email: make_contact("123"))
    monkeypatch.setattr(bookings, "list_records_by_contact_id", lambda cid, fields=None: [])

    appt = {"customer_email": "user@example.com"}

    assert bookings._is_already_booked(appt) is False


def test_is_already_booked_false_when_contact_missing(monkeypatch):
    monkeypatch.setattr(bookings, "search_contact_by_email", lambda email: None)

    appt = {"customer_email": "user@example.com"}

    assert bookings._is_already_booked(appt) is False


def test_is_already_booked_ignores_non_booked_records(monkeypatch):
    monkeypatch.setattr(bookings, "search_contact_by_email", lambda email: make_contact("123"))
    monkeypatch.setattr(
        bookings,
        "list_records_by_contact_id",
        lambda cid, fields=None: [{"Stage": "New"}, {"Stage": "Dropped"}],
    )

    appt = {"customer_email": "user@example.com"}

    assert bookings._is_already_booked(appt) is False
