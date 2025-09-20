import importlib
import sys
import types

import pytest
from fastapi.testclient import TestClient


def load_server(verify_token='secret'):
    api_module = types.ModuleType('app.api_client')
    calls = []

    def fake_post(path, payload):
        calls.append((path, payload))

    api_module.bigin_post = fake_post
    sys.modules['app.api_client'] = api_module

    if 'app.server' in sys.modules:
        del sys.modules['app.server']

    module = importlib.import_module('app.server')
    module.VERIFY_TOKEN = verify_token
    return module, calls


def test_health_endpoint():
    server, _ = load_server()
    client = TestClient(server.app)
    assert client.get('/healthz').json() == {'ok': True}


def test_bigin_webhook_bad_json():
    server, _ = load_server()
    client = TestClient(server.app)
    response = client.post('/bigin-webhook', data='not-json')
    assert response.status_code == 400


def test_bigin_webhook_bad_token():
    server, _ = load_server(verify_token='expected')
    client = TestClient(server.app)
    response = client.post('/bigin-webhook', json={'token': 'wrong'})
    assert response.status_code == 401


def test_bigin_webhook_success():
    server, calls = load_server(verify_token='expected')
    client = TestClient(server.app)
    payload = {'token': 'expected', 'ids': ['1', '2']}
    response = client.post('/bigin-webhook', json=payload)

    assert response.status_code == 200
    assert response.json() == {'ok': True, 'received': ['1', '2'], 'noted': ['1', '2']}
    assert calls == [
        ('Contacts/1/Notes', {'data': [{'Note_Title': 'VDS Webhook', 'Note_Content': 'Auto note: webhook received and processed.'}]}),
        ('Contacts/2/Notes', {'data': [{'Note_Title': 'VDS Webhook', 'Note_Content': 'Auto note: webhook received and processed.'}]}),
    ]
