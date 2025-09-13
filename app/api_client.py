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

def bigin_put(path: str, json_body: dict):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
    r = requests.put(url, json=json_body,
                     headers={"Authorization": f"Zoho-oauthtoken {at}"},
                     timeout=20)
    r.raise_for_status()
    return r.json()

# Find first contact by email
def search_contact_by_email(email: str):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/Contacts/search"
    r = requests.get(
        url,
        params={"criteria": f"(Email:equals:{email})"},
        headers={"Authorization": f"Zoho-oauthtoken {at}"},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    return data[0] if data else None

# Update fields on a Contact by ID
def update_contact_fields(contact_id: str, fields: dict):
    body = {"data": [{**fields, "id": contact_id}], "trigger": []}  # disable workflows
    return bigin_put("Contacts", body)


# --- Contact getters ---
def get_contact_by_id(contact_id: str):
    """Return the full Contact record (dict) or None if not found."""
    res = bigin_get(f"Contacts/{contact_id}")
    data = res.get("data") or []
    return data[0] if data else None

def get_contact_by_email(email: str):
    """Search by email, then fetch the full record (dict) or None."""
    row = search_contact_by_email(email)
    if not row:
        return None
    return get_contact_by_id(row["id"])

# --- Related list helpers (Deals related to a Contact) ---
def bigin_get_related(module_api: str, record_id: str, related_api: str, params=None):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{module_api}/{record_id}/{related_api}"
    r = requests.get(url, params=params or {},
                     headers={"Authorization": f"Zoho-oauthtoken {at}"}, timeout=20)
    r.raise_for_status()
    return r.json()

def get_deals_for_contact_id(contact_id: str):
    """Return list[dict] of Deals related to a Contact."""
    res = bigin_get_related("Contacts", contact_id, "Deals")
    return res.get("data", [])

def get_deals_for_contact_email(email: str):
    """Look up contact by email, then return its related Deals list."""
    row = search_contact_by_email(email)
    if not row:
        return []
    return get_deals_for_contact_id(row["id"])

# -------- Generic record lookups --------

def get_record_by_id(module_api: str, record_id: str):
    """
    Fetch a single record by ID from any module.
    Example: get_record_by_id("Contacts", "8864...").
    """
    res = bigin_get(f"{module_api}/{record_id}")
    data = res.get("data") or []
    return data[0] if data else None

def search_record_first(module_api: str, field_api: str, value: str, operator: str = "equals"):
    """
    Search the first record in a module by an arbitrary field.
    Example: search_record_first("Contacts", "Email", "user@acme.com")
    """
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{module_api}/search"
    criteria = f"({field_api}:{operator}:{value})"
    r = requests.get(url, params={"criteria": criteria},
                     headers={"Authorization": f"Zoho-oauthtoken {at}"},
                     timeout=20)
    r.raise_for_status()
    data = r.json().get("data", []) or []
    return data[0] if data else None

def search_record_by_email(module_api: str, email: str, email_field_api: str = "Email"):
    """
    Convenience for modules that have an Email field (e.g., Contacts).
    Example: search_record_by_email("Contacts", "user@acme.com")
    """
    return search_record_first(module_api, email_field_api, email)


import requests as _requests

def _discover_deals_contact_lookup_field(module_api: str = "Pipelines") -> str:
    """
    Find the lookup field on Pipelines (Deals) that points to Contacts.
    We scan settings/fields for a lookup whose related module is Contacts.
    Falls back to 'Contact_Name' which is common in Bigin.
    """
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/settings/fields"
    r = requests.get(url, params={"module": module_api},
                     headers={"Authorization": f"Zoho-oauthtoken {at}"}, timeout=20)
    r.raise_for_status()
    for f in r.json().get("fields", []) or []:
        if (f.get("data_type") == "lookup") and ((f.get("lookup") or {}).get("module", {}).get("api_name") == "Contacts"):
            return f["api_name"]
    return "Contact_Name"

def _search_deals_page(contact_id: str, module_api: str, lookup_field: str, page: int, per_page: int):
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{module_api}/search"
    criteria = f"({lookup_field}:equals:{contact_id})"
    r = requests.get(url, params={"criteria": criteria, "page": page, "per_page": per_page},
                     headers={"Authorization": f"Zoho-oauthtoken {at}"}, timeout=20)
    if r.status_code == 204:
        return []
    r.raise_for_status()
    return r.json().get("data", []) or []

def list_deals_by_contact_id(contact_id: str, per_page: int = 200, max_pages: int = 10):
    """
    Return ALL deals for a contact (paginate). Defaults to Pipelines; if that 4xx's,
    retry with module_api='Deals' just in case your org uses that name.
    """
    def _run(module_api: str):
        lookup = _discover_deals_contact_lookup_field(module_api)
        out, page = [], 1
        while page <= max_pages:
            chunk = _search_deals_page(contact_id, module_api, lookup, page, per_page)
            if not chunk:
                break
            out.extend(chunk)
            if len(chunk) < per_page:
                break
            page += 1
        return out

    try:
        return _run("Pipelines")
    except _requests.HTTPError as e:
        # Fallback to 'Deals' if Pipelines isn't your module name
        return _run("Deals")

def first_deal_by_contact_id(contact_id: str):
    deals = list_deals_by_contact_id(contact_id, per_page=1, max_pages=1)
    return deals[0] if deals else None

def list_deals_by_contact_email(email: str):
    row = search_contact_by_email(email)
    if not row:
        return []
    return list_deals_by_contact_id(row["id"])

def first_deal_by_contact_email(email: str):
    row = search_contact_by_email(email)
    if not row:
        return None
    return first_deal_by_contact_id(row["id"])