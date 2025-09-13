# app/api_client.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

# Load environment for CLI/server runs (safe if .env absent)
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
TOK_FILE = Path(os.environ.get("TOK_FILE") or (Path(__file__).resolve().parent.parent / "tokens.json"))
ACCOUNTS = os.environ.get("ACCOUNTS_URL", "https://accounts.zoho.eu")
DEFAULT_API = "https://www.zohoapis.eu"
REQ_TIMEOUT = 20  # seconds
EXPIRY_SKEW = 60  # refresh if <60s left

# --------------------------------------------------------------------
# Token storage
# --------------------------------------------------------------------
def _load_tokens() -> Dict[str, Any]:
    if not TOK_FILE.exists():
        raise RuntimeError(f"tokens.json not found at {TOK_FILE}")
    return json.loads(TOK_FILE.read_text(encoding="utf-8"))

def _save_tokens(tok: Dict[str, Any]) -> None:
    TOK_FILE.write_text(json.dumps(tok, indent=2), encoding="utf-8")

def _api_base(tok: Dict[str, Any]) -> str:
    return tok.get("api_domain") or os.environ.get("API_BASE") or DEFAULT_API

# --------------------------------------------------------------------
# OAuth: cached access token + refresh on demand
# --------------------------------------------------------------------
def get_access_token() -> Tuple[str, str]:
    tok = _load_tokens()

    # Use cached token if still valid
    if tok.get("access_token") and tok.get("expires_at", 0) > time.time() + EXPIRY_SKEW:
        return tok["access_token"], _api_base(tok)

    # Otherwise refresh
    r = requests.post(
        f"{ACCOUNTS}/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ.get("Z_CLIENT_ID"),
            "client_secret": os.environ.get("Z_CLIENT_SECRET"),
            "refresh_token": tok.get("refresh_token"),
        },
        timeout=REQ_TIMEOUT,
    )
    if not r.ok:
        raise RuntimeError(f"Refresh failed: {r.status_code} {r.text}")

    j = r.json()
    tok["access_token"] = j["access_token"]
    ttl = int(j.get("expires_in") or j.get("expires_in_sec") or 3600)
    tok["expires_at"] = time.time() + ttl
    if "api_domain" in j:
        tok["api_domain"] = j["api_domain"]
    _save_tokens(tok)
    return tok["access_token"], _api_base(tok)

def _auth_headers(at: str) -> Dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {at}"}

# --------------------------------------------------------------------
# Low-level request helper (retry once on 401)
# --------------------------------------------------------------------
def _bigin_request(method: str, path: str, *, params: Dict[str, Any] | None = None,
                   body: Dict[str, Any] | None = None) -> Dict[str, Any]:
    at, api = get_access_token()
    url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
    r = requests.request(method, url, params=params, json=body, headers=_auth_headers(at), timeout=REQ_TIMEOUT)

    # If token expired early, refresh once and retry
    if r.status_code == 401:
        at, api = get_access_token()
        url = f"{api.rstrip('/')}/bigin/v2/{path.lstrip('/')}"
        r = requests.request(method, url, params=params, json=body, headers=_auth_headers(at), timeout=REQ_TIMEOUT)

    r.raise_for_status()
    # Some endpoints can 204 No Content; standardize to {}
    return {} if r.status_code == 204 or not r.content else r.json()

# Public thin wrappers (kept for compatibility)
def bigin_get(path: str) -> Dict[str, Any]:
    return _bigin_request("GET", path)

def bigin_post(path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
    return _bigin_request("POST", path, body=json_body)

def bigin_put(path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
    return _bigin_request("PUT", path, body=json_body)

def bigin_delete(path: str) -> Dict[str, Any]:
    return _bigin_request("DELETE", path)

# --------------------------------------------------------------------
# Contacts: search, read, update
# --------------------------------------------------------------------
def search_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Return first Contact row by exact Email, or None."""
    res = _bigin_request("GET", "Contacts/search", params={"criteria": f"(Email:equals:{email})"})
    data = res.get("data") or []
    return data[0] if data else None

def get_contact_by_id(contact_id: str) -> Optional[Dict[str, Any]]:
    res = bigin_get(f"Contacts/{contact_id}")
    data = res.get("data") or []
    return data[0] if data else None

def get_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    row = search_contact_by_email(email)
    return get_contact_by_id(row["id"]) if row else None

def update_contact_fields(contact_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    """Updates fields on a Contact by id (workflows disabled)."""
    body = {"data": [{**fields, "id": contact_id}], "trigger": []}
    return bigin_put("Contacts", body)


# Records

# --- related-list GET (generic) ---
def bigin_get_related(module_api: str, record_id: str, related_api: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return _bigin_request("GET", f"{module_api}/{record_id}/{related_api}", params=params or {})

# --- records linked to a Contact (tries Pipelines then Deals for back-compat) ---
def _records_for_contact(contact_id: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"per_page": per_page}
    if fields:
        params["fields"] = ",".join(fields)
    # Prefer v2 module name
    try:
        res = bigin_get_related("Contacts", contact_id, "Pipelines", params)
        return res.get("data", []) or []
    except requests.HTTPError:
        # Fallback for orgs still exposing "Deals" related list
        res = bigin_get_related("Contacts", contact_id, "Deals", params)
        return res.get("data", []) or []

def list_records_by_contact_id(contact_id: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[Dict[str, Any]]:
    return _records_for_contact(contact_id, per_page=per_page, fields=fields)

def first_record_by_contact_id(contact_id: str, *, fields: List[str] | None = None) -> Optional[Dict[str, Any]]:
    rows = _records_for_contact(contact_id, per_page=1, fields=fields)
    return rows[0] if rows else None

def list_records_by_contact_email(email: str, *, per_page: int = 200, fields: List[str] | None = None) -> List[Dict[str, Any]]:
    row = search_contact_by_email(email)
    return list_records_by_contact_id(row["id"], per_page=per_page, fields=fields) if row else []

def first_record_by_contact_email(email: str, *, fields: List[str] | None = None) -> Optional[Dict[str, Any]]:
    row = search_contact_by_email(email)
    return first_record_by_contact_id(row["id"], fields=fields) if row else None

# --- update a single record by id (module defaults to Pipelines) ---
def update_record_fields(record_id: str, fields: Dict[str, Any], *, module_api: str = "Pipelines") -> Dict[str, Any]:
    """
    Update fields on a Pipeline record by id.
    Bigin v2 supports PUT /{module_api} with {"data":[{...,"id":...}], "trigger":[]}.
    """
    if not isinstance(fields, dict) or not fields:
        raise ValueError("`fields` must be a non-empty dict")
    body = {"data": [{**fields, "id": record_id}], "trigger": []}
    return bigin_put(module_api, body)

# --- update records linked to a contact (all or first only) ---
def update_records_by_contact_id(
    contact_id: str,
    patch: Dict[str, Any],
    *,
    module_api: str = "Pipelines",   # change to "Deals" if your org still uses that API name
    first_only: bool = False,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    rows = list_records_by_contact_id(contact_id, fields=["id"])
    if not rows:
        return [] if not first_only else {}
    targets = rows[:1] if first_only else rows
    out: List[Dict[str, Any]] = []
    for r in targets:
        rid = r.get("id")
        if not rid:
            continue
        out.append(update_record_fields(rid, patch, module_api=module_api))
    return out[0] if first_only else out

def update_records_by_contact_email(
    email: str,
    patch: Dict[str, Any],
    *,
    module_api: str = "Pipelines",
    first_only: bool = False,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    row = search_contact_by_email(email)
    if not row:
        raise ValueError(f"No Contact with Email={email!r}")
    return update_records_by_contact_id(row["id"], patch, module_api=module_api, first_only=first_only)

