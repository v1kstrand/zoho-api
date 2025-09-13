# app/api_client.py
import os, json
from pathlib import Path
import requests
from dotenv import load_dotenv  # pip install python-dotenv (global ok)
load_dotenv()

# tokens.json is kept at repo root
TOK_FILE = (Path(__file__).resolve().parent.parent / "tokens.json")
ACCOUNTS = os.environ.get("ACCOUNTS_URL", "https://accounts.zoho.eu")

def _load_tokens():
    return json.loads(TOK_FILE.read_text(encoding="utf-8"))

def _save_tokens(tok):
    TOK_FILE.write_text(json.dumps(tok, indent=2), encoding="utf-8")

def get_access_token():
    """
    Refreshes the Zoho access token using the refresh token in tokens.json.
    Requires env vars: Z_CLIENT_ID, Z_CLIENT_SECRET.
    Returns: (access_token, api_domain)
    """
    tok = _load_tokens()
    r = requests.post(
        f"{ACCOUNTS}/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ["Z_CLIENT_ID"],
            "client_secret": os.environ["Z_CLIENT_SECRET"],
            "refresh_token": tok["refresh_token"],
        },
        timeout=15,
    )
    r.raise_for_status()
    j = r.json()
    tok["access_token"] = j["access_token"]
    if "api_domain" in j:
        tok["api_domain"] = j["api_domain"]
    _save_tokens(tok)
    return tok["access_token"], tok.get("api_domain", "https://www.zohoapis.eu")

def bigin_get(path: str):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
    r = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {at}"}, timeout=15)
    r.raise_for_status()
    return r.json()

def bigin_post(path: str, json_body: dict):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
    r = requests.post(url, json=json_body,
                      headers={"Authorization": f"Zoho-oauthtoken {at}"},
                      timeout=20)
    r.raise_for_status()
    return r.json()

def bigin_delete(path: str):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
    r = requests.delete(url, headers={"Authorization": f"Zoho-oauthtoken {at}"},
                        timeout=20)
    r.raise_for_status()
    return r.json()