# app/api_client.py
from __future__ import annotations

import os, json, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv


load_dotenv()

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
TOK_FILE = Path(os.environ.get("TOK_FILE") or (Path(__file__).resolve().parent.parent / "tokens.json"))
ACCOUNTS  = os.environ.get("ACCOUNTS_URL", "https://accounts.zoho.eu")
DEFAULT_API = "https://www.zohoapis.eu"
REQ_TIMEOUT = 20     # seconds
EXPIRY_SKEW = 60     # refresh if <60s left
MAX_RETRIES = 3

JSON = Dict[str, Any]

# ------------------------------------------------------------
# Token storage
# ------------------------------------------------------------
def _load_tokens() -> JSON:
    if not TOK_FILE.exists():
        raise RuntimeError(f"tokens.json not found at {TOK_FILE}")
    return json.loads(TOK_FILE.read_text(encoding="utf-8"))

def _save_tokens(tok: JSON) -> None:
    TOK_FILE.write_text(json.dumps(tok, indent=2), encoding="utf-8")

def _api_base(tok: JSON) -> str:
    return tok.get("api_domain") or os.environ.get("API_BASE") or DEFAULT_API

# ------------------------------------------------------------
# OAuth
# ------------------------------------------------------------


def _refresh_access_token(tok: JSON) -> Tuple[str, str]:
    """Refresh the OAuth access token using the stored refresh token."""
    refresh = (tok.get("refresh_token") or "").strip()
    if not refresh:
        raise RuntimeError("tokens.json missing refresh_token")

    client_id = (os.environ.get("Z_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("Z_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("Set Z_CLIENT_ID and Z_CLIENT_SECRET in environment")

    response = requests.post(
        f"{ACCOUNTS}/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh,
        },
        timeout=REQ_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()

    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("OAuth response did not include access_token")

    tok["access_token"] = access_token
    ttl = int(payload.get("expires_in") or payload.get("expires_in_sec") or 3600)
    tok["expires_at"] = time.time() + ttl
    if payload.get("api_domain"):
        tok["api_domain"] = payload["api_domain"]

    _save_tokens(tok)
    return tok["access_token"], _api_base(tok)


def get_access_token() -> Tuple[str, str]:
    tok = _load_tokens()
    if tok.get("access_token") and tok.get("expires_at", 0) > time.time() + EXPIRY_SKEW:
        return tok["access_token"], _api_base(tok)
    return _refresh_access_token(tok)

def _force_refresh_access_token() -> Tuple[str, str]:
    """Unconditional refresh (used after a 401)."""
    return _refresh_access_token(_load_tokens())

def _auth_headers(at: str) -> JSON:
    return {"Authorization": f"Zoho-oauthtoken {at}"}

# ------------------------------------------------------------
# Low-level request (401 force-refresh + 429 backoff)
# ------------------------------------------------------------
def _api_url(api: str, path: str) -> str:
    return f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"

def _json_or_empty(r: requests.Response) -> JSON:
    return {} if r.status_code == 204 or not r.content else r.json()

def _bigin_request(method: str, path: str, *, params: JSON | None = None, body: JSON | None = None) -> JSON:
    at, api = get_access_token()
    url = _api_url(api, path)

    for attempt in range(MAX_RETRIES):
        r = requests.request(method, url, params=params, json=body, headers=_auth_headers(at), timeout=REQ_TIMEOUT)

        # one hard refresh on first 401
        if r.status_code == 401 and attempt == 0:
            at, api = _force_refresh_access_token()
            url = _api_url(api, path)
            continue

        # gentle backoff for 429 (use Retry-After when present)
        if r.status_code == 429 and attempt < (MAX_RETRIES - 1):
            sleep_s = int(r.headers.get("Retry-After", "2"))
            time.sleep(max(1, min(sleep_s, 10)))
            continue

        r.raise_for_status()
        return _json_or_empty(r)

    # if loop exits without return (shouldn’t happen), raise last
    r.raise_for_status()  # type: ignore[name-defined]
    return {}

# Public thin wrappers
def bigin_get(path: str) -> JSON:                    return _bigin_request("GET", path)
def bigin_post(path: str, json_body: JSON) -> JSON:  return _bigin_request("POST", path, body=json_body)
def bigin_put(path: str, json_body: JSON) -> JSON:   return _bigin_request("PUT", path, body=json_body)
def bigin_delete(path: str) -> JSON:                 return _bigin_request("DELETE", path)

# ------------------------------------------------------------
# Small utilities (DRY)
# ------------------------------------------------------------
def _first(rows: Optional[List[JSON]]) -> Optional[JSON]:
    rows = rows or []
    return rows[0] if rows else None

def _crit(field: str, op: str, val: str) -> str:
    # quote value when it contains spaces or special chars
    needs_quotes = any(c in val for c in (' ', '"', "'", '(', ')', ':', ',', ';'))
    v = f'"{val}"' if needs_quotes else val
    return f"({field}:{op}:{v})"

# ------------------------------------------------------------
# Contacts
# ------------------------------------------------------------
def search_contact_by_email(email: str) -> Optional[JSON]:
    """Return first Contact row by exact Email, or None."""
    res = _bigin_request("GET", "Contacts/search", params={"criteria": _crit("Email", "equals", email)})
    return _first(res.get("data"))

def _contact_id(email: str) -> str:
    row = search_contact_by_email(email)
    if not row:
        raise ValueError(f"No Contact found with Email={email!r}")
    return row["id"]

def get_contact_by_id(contact_id: str) -> Optional[JSON]:
    return _first(bigin_get(f"Contacts/{contact_id}").get("data"))

def get_contact_by_email(email: str) -> Optional[JSON]:
    row = search_contact_by_email(email)
    return get_contact_by_id(row["id"]) if row else None

def update_contact_fields(contact_id: str, fields: JSON) -> JSON:
    """Updates fields on a Contact by id (workflows disabled)."""
    if not isinstance(fields, dict) or not fields:
        raise ValueError("`fields` must be a non-empty dict")
    return update_module_fields("Contacts", contact_id, fields)

# ------------------------------------------------------------
# Generic: related lists & module updates
# ------------------------------------------------------------
def bigin_get_related(module_api: str, record_id: str, related_api: str, params: JSON | None = None) -> JSON:
    return _bigin_request("GET", f"{module_api}/{record_id}/{related_api}", params=params or {})

def update_module_fields(module_api: str, record_id: str, fields: JSON) -> JSON:
    """
    PUT /{module_api} with {"data":[{...,"id":...}], "trigger":[]}
    Works for Contacts, Pipelines, Companies/Accounts, etc.
    """
    
    body = {"data": [{**fields, "id": record_id}]}
    return bigin_put(module_api, body)

# ------------------------------------------------------------
# Records (Pipeline records linked to a Contact)
# ------------------------------------------------------------
def _records_for_contact(contact_id: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[JSON]:
    params: JSON = {"per_page": per_page}
    if fields:
        params["fields"] = ",".join(fields)
    # Prefer v2 name "Pipelines"; fallback to legacy "Deals" if needed
    try:
        res = bigin_get_related("Contacts", contact_id, "Pipelines", params)
        return res.get("data", []) or []
    except requests.HTTPError:
        res = bigin_get_related("Contacts", contact_id, "Deals", params)
        return res.get("data", []) or []

def list_records_by_contact_id(contact_id: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[JSON]:
    return _records_for_contact(contact_id, per_page=per_page, fields=fields)

def first_record_by_contact_id(contact_id: str, *, fields: List[str] | None = None) -> Optional[JSON]:
    return _first(_records_for_contact(contact_id, per_page=1, fields=fields))

def list_records_by_contact_email(email: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[JSON]:
    return list_records_by_contact_id(_contact_id(email), per_page=per_page, fields=fields)

def first_record_by_contact_email(email: str, *, fields: List[str] | None = None) -> Optional[JSON]:
    return first_record_by_contact_id(_contact_id(email), fields=fields)

def update_record_fields(record_id: str, fields: JSON, *, module_api: str = "Pipelines") -> JSON:
    """Update a single pipeline record by id (module default: Pipelines)."""
    if not isinstance(fields, dict) or not fields:
        raise ValueError("`fields` must be a non-empty dict")
    return update_module_fields(module_api, record_id, fields)

def update_records_by_contact_id(
    contact_id: str,
    patch: JSON,
    *,
    module_api: str = "Pipelines",
    first_only: bool = False,
) -> JSON | List[JSON]:
    
    rows = list_records_by_contact_id(contact_id, fields=["id"])
    if not rows:
        create_pipeline_record_for_contact(contact_id)
        time.sleep(1)
        rows = list_records_by_contact_id(contact_id, fields=["id"])

    targets = [row for row in (rows[:1] if first_only else rows) if row.get("id")]
    if not targets:
        return {} if first_only else []

    responses: List[JSON] = [
        update_record_fields(row["id"], patch, module_api=module_api)
        for row in targets
    ]
    return responses[0] if first_only else responses

def update_records_by_contact_email(
    email: str,
    patch: JSON,
    *,
    module_api: str = "Pipelines",
    first_only: bool = False,
) -> JSON | List[JSON]:
    return update_records_by_contact_id(_contact_id(email), patch, module_api=module_api, first_only=first_only)


# --- full record by id (module default: Pipelines) ---
def get_record_by_id(record_id: str, module_api: str = "Pipelines") -> Optional[Dict[str, Any]]:
    res = bigin_get(f"{module_api}/{record_id}")
    data = res.get("data") or []
    return data[0] if data else None

def get_full_records_by_contact_id(contact_id: str, *, first_only: bool = False, module_api: str = "Pipelines") -> Dict[str, Any] | List[Dict[str, Any]]:
    rows = list_records_by_contact_id(contact_id, fields=["id"])
    if not rows:
        return {} if first_only else []
    ids = [r["id"] for r in rows]
    if first_only:
        rec = get_record_by_id(ids[0], module_api=module_api)
        return rec or {}
    return [r for rid in ids if (r := get_record_by_id(rid, module_api=module_api))]

def get_full_records_by_contact_email(email: str, *, first_only: bool = False, module_api: str = "Pipelines") -> Dict[str, Any] | List[Dict[str, Any]]:
    c = search_contact_by_email(email)
    if not c:
        raise ValueError(f"No Contact with Email={email!r}")
    return get_full_records_by_contact_id(c["id"], first_only=first_only, module_api=module_api)

def update_records(
    *,
    email: Optional[str] = None,
    id: Optional[str] = None,            # Contact ID
    module: str = "Pipelines",
    first: bool = False,
    **fields: Any,                       # the fields to update, e.g. Stage="Qualification"
) -> Dict[str, Any]:
    """
    Update pipeline records linked to a Contact.

    Call with either `email` OR `id` (Contact ID), plus any number of field=value kwargs.
    Returns: {"ok": True, "updated": {...}, "result": <api response>}.

    Examples:
        update_records(email="user@example.com", first=True, Stage="Qualification")
        update_records(id="886415000000123456", Description="Updated via kwargs")
        update_records(email="user@example.com", module="Pipelines", Other_Info="Note", Is_Unsub=True)
    """
    if (email is None) == (id is None):
        raise ValueError("Provide exactly one of: email OR id.")
    if not fields:
        raise ValueError("Provide at least one field to update as keyword arguments.")

    patch = dict(fields)

    if email is not None:
        res = update_records_by_contact_email(email, patch, module_api=module, first_only=first)
    else:
        res = update_records_by_contact_id(id, patch, module_api=module, first_only=first)

    return {"ok": True, "updated": patch, "result": res}




def create_pipeline_record_for_contact(contact_id: str) -> Dict[str, Any]:
    """
    Create a Pipeline record linked to the given Contact ID.
    Deal_Name is set to the contact's name.
    """
    # fetch contact to get a nice name
    res = bigin_get(f"Contacts/{contact_id}")
    row = (res.get("data") or [{}])[0]
    contact_name = (
        row.get("Full_Name")
        or " ".join(x for x in [row.get("First_Name"), row.get("Last_Name")] if x)
        or row.get("Email")
        or f"Contact {contact_id}"
    )

    payload = {
        "data": [        {
            "Owner": {
                "id": "886415000000502001"
            },
            "Sub_Pipeline": "Discovery Outreach Standard",
            "Deal_Name": f"Record for {contact_name}",
            "Contact_Name": {
                "id": contact_id,
                "name": contact_name
            },
            "Stage": "New",
            "Pipeline": {'name': 'Discovery Outreach', 'id': '886415000000515415'}
        }],
        "trigger": [],
    }
    return bigin_post("Pipelines", payload)


