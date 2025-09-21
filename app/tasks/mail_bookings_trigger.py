# app/tasks/mail_bookings_trigger.py
from __future__ import annotations

import email
import os
import re
from email.header import decode_header, make_header
from typing import Any, Dict, Optional

from dotenv import load_dotenv
load_dotenv()

from ..api_client_csv import add_contact, find_contact_by_email, update_contact, get_contact_field
from ..parse_mail import parse_mail
from ..mail_utils import imap_connect_with_retry, message_body_text, move_message


# ---------------------------------------------------------------------
# Config / Environment
# ---------------------------------------------------------------------
IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")             # required
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")         # required
BOOKING_FOLDER = os.getenv("ZOHO_IMAP_FOLDER")
MOVE_BOOKING_TO = os.getenv("BOOKING_MOVE_TO")
DRY_RUN = os.getenv("BOOKING_DRY_RUN").lower() == "true"
if DRY_RUN:
    print("Dry run enabled")

BOOKING_SUBJECT_HINT = re.compile(
    r"""^(?:\s*(?:re|fwd)\s*:\s*)*              # optional Re:/Fwd:
        vds\s+discovery\s+project\s+between\s+
        david\s+vikstrand\s+and\s+              # fixed prefix
        (?P<cust>.+?)\s*$                       # capture the remainder (customer name)
    """,
    re.I | re.X,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    email_addr = (appt.get("customer_email") or "").strip()
    if not email_addr:
        return None

    email_norm = email_addr.lower()
    existing = find_contact_by_email(email_norm)
    if existing:
        return existing

    payload: Dict[str, Any] = {
        "email": email_norm,
        "first_name": (appt.get("customer_first_name") or "").strip() or None,
        "last_name": (appt.get("customer_last_name") or "").strip() or None,
    }
    return add_contact({k: v for k, v in payload.items() if v is not None})

def _subject(msg: email.message.Message) -> str:
    return str(make_header(decode_header(msg.get("Subject", ""))))


def _looks_like_booking(subj: str) -> bool:
    return bool(BOOKING_SUBJECT_HINT.search(subj))

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def process_bookings_once(verbose: bool = True) -> int:
    """Scan for booking mails and ensure corresponding contacts exist."""
    assert IMAP_USER and IMAP_PASS, "Set ZOHO_IMAP_USER and ZOHO_IMAP_PASSWORD in environment"

    imap = imap_connect_with_retry(
        IMAP_HOST,
        IMAP_USER,
        IMAP_PASS,
        BOOKING_FOLDER,
        ensure_folder=MOVE_BOOKING_TO,
        verbose=verbose,
    )
    handled = 0

    try:
        typ, data = imap.uid("SEARCH", None, "(UNSEEN)")
        uids = (data[0].decode().split() if typ == "OK" and data and data[0] else [])
        if verbose:
            print(f"[imap] {len(uids)} unseen in {BOOKING_FOLDER}")

        for uid in uids:
            processed = False
            try:
                typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
                if typ != "OK" or not fetch or not fetch[0]:
                    continue

                msg = email.message_from_bytes(fetch[0][1])
                subj = _subject(msg)
                if not _looks_like_booking(subj):
                    continue

                body = message_body_text(msg)
                appt = parse_mail(body)
                if verbose:
                    print("[info] appointment parsed")
                    
                contact = _ensure_contact(appt)
                stage_value = get_contact_field(contact.get("email", ""), field="stage") if contact else ""
                if stage_value.strip().lower() == "booked":
                    processed = True
                    if verbose:
                        who = appt.get("customer_email") or appt.get("customer_name")
                        print(f"[skip] already booked for {who}")
                    continue

                if not (appt.get("customer_email") or "").strip():
                    if verbose:
                        print("[warn] booking mail missing customer email; skipping")
                    continue

                if DRY_RUN:
                    continue
                
                if contact:
                    who = contact.get("email")
                    update_contact(who, {"stage": "Booked"})
                    handled += 1
                    processed = True
                    if verbose:
                        print(f"[ok] updated booking stage for {who}")
                else:
                    if verbose:
                        print("[warn] could not ensure contact (likely no email in appointment)")
            except Exception as exc:
                if verbose:
                    print(f"[error] failed to handle booking uid {uid}: {exc}")
            finally:
                if MOVE_BOOKING_TO and processed:
                    move_message(imap, uid, MOVE_BOOKING_TO)

    finally:
        try:
            imap.close()
        except Exception:
            pass
        try:
            imap.logout()
        except Exception:
            pass

    if verbose:
        print(f"[done] handled {handled} booking email(s).")
    return handled


if __name__ == "__main__":
    process_bookings_once()
