from pathlib import Path

import pytest

import app.api_client_csv as contacts


@pytest.fixture()
def temp_store(monkeypatch, tmp_path):
    csv_path = tmp_path / 'contacts.csv'
    store = contacts.ContactStore(csv_path)
    monkeypatch.setattr(contacts, '_store', store)
    yield store


def test_add_and_find_contact(temp_store):
    added = temp_store.add_contact({'email': 'Test@Example.com', 'first_name': 'Alice'})
    assert added['email'] == 'test@example.com'
    assert added['first_name'] == 'Alice'
    assert added['auto_number'] == '1'

    found = temp_store.find_contact_by_email('test@example.com')
    assert found['email'] == 'test@example.com'
    assert found['first_name'] == 'Alice'


def test_update_and_get_field(temp_store):
    temp_store.add_contact({'email': 'user@example.com', 'first_name': 'Bob'})
    updated = temp_store.update_contact_by_email('user@example.com', {'stage': 'Prospect'})
    assert updated['stage'] == 'Prospect'

    stage = temp_store.get_contact_field('user@example.com', 'stage')
    assert stage == 'Prospect'


def test_filter_contacts(temp_store):
    temp_store.add_contact({'email': 'one@example.com', 'first_name': 'One', 'stage': 'New'})
    temp_store.add_contact({'email': 'two@example.com', 'first_name': 'Two', 'stage': 'Hot'})

    filtered = temp_store.filter_contacts({'stage': 'Hot'})
    assert len(filtered) == 1
    assert filtered[0]['email'] == 'two@example.com'


def test_append_contact_note(temp_store):
    temp_store.add_contact({'email': 'note@example.com'})
    contacts.append_contact_note('note@example.com', 'First note')
    contacts.append_contact_note('note@example.com', 'Second note')

    contact = temp_store.find_contact_by_email('note@example.com')
    assert 'First note' in contact['notes']
    assert 'Second note' in contact['notes']
