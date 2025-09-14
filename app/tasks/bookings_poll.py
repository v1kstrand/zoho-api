# app/tasks/bookings_poll.py
from __future__ import annotations
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

# Reuse your Bigin client bits
from ..api_client import (
    get_access_token,
    _auth_headers,          # ok to import; it's in your project
    REQ_TIMEOUT,
    bigin_get,
    bigin_post,
    bigin_put,
    search_contact_by_email,
)

# -----------------------------
# Settings (override via .env)
# -----------------------------
STATE_FILE = Path(".bookings_state.json")

# Booking → Pipeline mapping
FIELD_BOOKING_ID = "Booking_Id"      # custom field on Pipelines
DEFAULT_STAGE_CONFIRMED = "Booked"
DEFAULT_STAGE_CANCELED  = "Dropped"
PIPELINES_MODULE        = "Pipelines"

# Poll window
LOOKBACK_DEFAULT_HOURS  = 24
PER_PAGE                = 100
STATUS_FILTER           = "confirmed"   # "" for all

# -----------------------------
# Helpers: Bookings API
# -----------------------------
def _bookings_base() -> str:
    """Bookings base reusing your data center (eu/us) from the access token."""
    _, api = get_access_token()  # e.g. https://www.zohoapis.eu
    return api.rstrip("/") + "/bookings/v1/json"

def _en_gmt_fmt(ts: dt.datetime) -> str:
    """Zoho Bookings expects dd-MMM-yyyy HH:mm:ss (English month names)."""
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    # Bookings timestamps are interpreted in Org TZ; sending UTC is fine for a window query
    return f"{ts.day:02d}-{months[ts.month-1]}-{ts.year} {ts:%H:%M:%S}"

def fetch_appointments(
    *,
    start: Optional[dt.datetime] = None,
    end:   Optional[dt.datetime] = None,
    status: Optional[str] = None,
    per_page: int = PER_PAGE,
) -> Iterator[Dict[str, Any]]:
    """
    Yield appointments from Zoho Bookings within [start, end].
    """
    at, _ = get_access_token()
    url = _bookings_base() + "/fetchappointment"
    page = 1

    payload: Dict[str, Any] = {"per_page": str(per_page), "page": str(page)}
    if start:  payload["from_time"] = _en_gmt_fmt(start)
    if end:    payload["to_time"]   = _en_gmt_fmt(end)
    if status: payload["status"]    = status

    while True:
        payload["page"] = str(page)
        r = requests.post(url, headers=_auth_headers(at), data=payload, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        rv = (j.get("response") or {}).get("returnvalue") or {}
        rows = rv.get("response") or []
        for row in rows:
            yield row
        if not rv.get("next_page_available"):
            break
        page += 1

# -----------------------------
# Helpers: state, extractors
# -----------------------------
def _load_cursor() -> dt.datetime:
    if STATE_FILE.exists():
        try:
            obj = json.loads(STATE_FILE.read_text())
            return dt.datetime.fromisoformat(obj["last"])
        except Exception:
            pass
    # default: look back N hours
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=LOOKBACK_DEFAULT_HOURS)

def _save_cursor(t: dt.datetime) -> None:
    STATE_FILE.write_text(json.dumps({"last": t.isoformat()}))

def _first_nonempty(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return default

def _parse_iso(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

# -----------------------------
# Bigin upserts (Contacts & Records)
# -----------------------------
def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Find or create a Contact from a Bookings appointment.
    """
    email = (_first_nonempty(appt, ["customer_email", "email", "mail"]) or "").strip().lower()
    if not email:
        return None

    c = search_contact_by_email(email)
    if c:
        return c

    # Create contact (minimal required fields: Last_Name; add what we can)
    full_name = _first_nonempty(appt, ["customer_name", "name"], "")
    first, last = "", ""
    if full_name:
        parts = full_name.strip().split()
        first = parts[0]
        last = " ".join(parts[1:]) or parts[0]
    else:
        # fallback: use email local part as last name
        last = email.split("@", 1)[0]

    phone = _first_nonempty(appt, ["customer_phone", "phone"])

    body = {
        "data": [{
            "Email": email,
            "First_Name": first or None,
            "Last_Name":  last or "Unknown",
            "Phone": phone or None,
        }],
        "trigger": [],
    }
    res = bigin_post("Contacts", body)
    data = (res.get("data") or [])
    if data and data[0].get("status") == "success":
        # Read back for full shape
        return search_contact_by_email(email)
    return None

def _search_pipeline_by_booking_id(booking_id: str) -> Optional[Dict[str, Any]]:
    """
    Try to find an existing pipeline record using our custom Booking_Id field.
    """
    if not booking_id:
        return None
    # criteria query via querystring
    # note: booking_id assumed to be simple (no spaces). Quote if needed.
    res = bigin_get(f"{PIPELINES_MODULE}/search?criteria=({FIELD_BOOKING_ID}:equals:{booking_id})")
    rows = res.get("data") or []
    return rows[0] if rows else None

def _booking_key(appt: dict) -> str:
    # Prefer official id; fallback to service+start composite so we still dedupe
    bid = (appt.get("booking_id") or appt.get("id") or appt.get("appointment_id") or "").strip()
    if bid:
        return bid
    svc = (appt.get("service_name") or appt.get("service") or "").strip()
    iso = (appt.get("iso_start_time") or appt.get("start_time_iso") or "").strip()
    return f"{svc}|{iso}"  # good enough if booking_id is absent

def _upsert_pipeline_record(contact: dict, appt: dict) -> dict:
    contact_id = contact["id"]
    email = contact.get("Email")
    key = _booking_key(appt)

    iso_start  = (appt.get("iso_start_time") or appt.get("start_time_iso") or "")
    service    = (appt.get("service_name") or appt.get("service") or "")
    staff      = (appt.get("staff_name") or appt.get("staff") or "")
    status     = (appt.get("status") or "").lower()

    # Map booking status -> Stage (UI/ops only; not used for dedupe)
    stage = None
    if status in {"confirmed", "accepted"}:
        stage = DEFAULT_STAGE_CONFIRMED  # "Booked"
    elif status in {"canceled", "cancelled", "declined", "rejected", "noshow", "no_show"}:
        stage = DEFAULT_STAGE_CANCELED   # "Dropped"

    fields = {
        FIELD_BOOKING_ID: key,  # store our key (official booking_id or composite)
        "Description": f"Booking: {service} @ {iso_start} (staff: {staff}) | status={status}",
    }
    if stage:
        fields["Stage"] = stage

    # 1) Upsert by Booking_Id (or composite fallback)
    res = bigin_get(f"{PIPELINES_MODULE}/search?criteria=({FIELD_BOOKING_ID}:equals:{key})")
    rows = res.get("data") or []
    if rows:
        rid = rows[0]["id"]
        return bigin_put(PIPELINES_MODULE, {"data": [{**fields, "id": rid}], "trigger": []})

    # 2) Create new record linked to contact
    deal_name = f"{service or 'Booking'} {iso_start[:16].replace('T',' ')} - {email or contact_id}".strip()
    payload = {
        "data": [{
            "Deal_Name": deal_name,
            "Contact_Name": {"id": contact_id},
            **fields,
        }],
        "trigger": [],
    }
    return bigin_post(PIPELINES_MODULE, payload)

# -----------------------------
# Main poller
# -----------------------------
def poll_new():
    """
    Fetch appointments since last cursor, upsert Contacts + Pipeline records.
    """
    start = _load_cursor()
    end   = dt.datetime.now(dt.timezone.utc)

    latest = start
    count_appointments = 0
    created_or_updated = 0

    for appt in fetch_appointments(start=start, end=end, status=(STATUS_FILTER or None)):
        count_appointments += 1

        # ensure contact
        c = _ensure_contact(appt)
        if not c:
            continue

        # upsert pipeline record
        res = _upsert_pipeline_record(c, appt)
        if res.get("data"):
            created_or_updated += 1

        # advance cursor using the freshest of booked_on/start time
        booked_on = _parse_iso(_first_nonempty(appt, ["booked_on", "created_time", "created_at"]))
        start_time = _parse_iso(_first_nonempty(appt, ["iso_start_time", "start_time_iso"]))
        for t in (booked_on, start_time):
            if t and t > latest:
                latest = t

    # persist new cursor
    _save_cursor(latest)

    print(json.dumps({
        "ok": True,
        "window": {"from": start.isoformat(), "to": end.isoformat()},
        "seen": count_appointments,
        "upserts": created_or_updated,
        "cursor": latest.isoformat(),
    }, indent=2, ensure_ascii=False))

# Script entry point (optional)
if __name__ == "__main__":
    poll_new()
