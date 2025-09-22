# app/tasks/mail_bookings_trigger.py
from __future__ import annotations
import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from ..api_client import add_contact, find_contact_by_email, update_contact, get_contact_field
from ..cal_util import get_bookings_created_within
from ..mailgun_util import send_mailgun_message

load_dotenv()

if DRY_RUN := os.environ["BOOKING_DRY_RUN"].strip().lower() == "true":
    print("Dry run enabled")

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = {
        "email" : appt["email"].lower(),
        "first_name" : appt["name"]["firstName"],
        "last_name" : appt["name"]["lastName"],
        "company_name" : appt.get("Company", "")
        }
    
    if existing := find_contact_by_email(payload["email"]):
        return update_contact(existing["email"], payload)
    return add_contact(payload)

def process_bookings_once(verbose: bool = True) -> int:
    """Scan for booking mails and ensure corresponding contacts exist."""
    handled = 0
    try:
        bookings = get_bookings_created_within()
    except Exception as exc:
        print(f"[error] failed to get bookings: {exc}")
        return handled
    
    for booking in bookings:
        try:
            appt = booking["bookingFieldsResponses"]
            contact = _ensure_contact(appt)
            email = contact["email"].lower()
            stage_value = get_contact_field(email, field="stage")
            
            if stage_value.strip().lower() == "booked":
                if verbose:
                    print(f"[skip] already booked for {email}")
                continue

            if DRY_RUN:
                continue
            
            update_contact(email, {"stage": "booked"})
            send_mailgun_message([email], ["form_v1", {}], "intro_form")
            
            handled += 1
            if verbose:
                print(f"[ok] updated booking stage for {email}")
        except Exception as exc:
            if verbose:
                print(f"[error] failed to update booking stage for {email}: {exc}")
                
    if verbose:
        print(f"[done] handled {handled} booking email(s).")
    return handled

if __name__ == "__main__":
    process_bookings_once()
