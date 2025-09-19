# app/tasks/mail_bookings_trigger.py
from __future__ import annotations

import email
import imaplib
import os
import re
import time
from email.header import decode_header, make_header
from typing import Any, Dict, Optional

from dotenv import load_dotenv
load_dotenv()

from ..api_client_csv import find_contact_by_email, upsert_contact, update_contact, get_contact_field
from ..parse_mail import parse_mail
from ..mail_utils import ensure_mailbox, message_body_text, move_message

load_dotenv()

# ---------------------------------------------------------------------
# Config / Environment
# ---------------------------------------------------------------------
IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")             # required
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")         # required

BOOKING_FOLDER = os.getenv("BOOKING_FOLDER", "INBOX")
MOVE_BOOKING_TO = os.getenv("BOOKING_MOVE_TO", None)

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

def _imap_connect_with_retry(
    host: str,
    user: str,
    password: str,
    select_folder: str,
    *,
    ensure_folder: Optional[str] = None,
    attempts: int = 3,
    delay: float = 2.0,
    verbose: bool = True,
) -> imaplib.IMAP4_SSL:
    last: Optional[BaseException] = None
    for i in range(attempts):
        try:
            imap = imaplib.IMAP4_SSL(host)
            imap.login(user, password)
            if ensure_folder:
                ensure_mailbox(imap, ensure_folder)
            typ, _ = imap.select(select_folder)
            if typ != "OK":
                ensure_mailbox(imap, select_folder)
                imap.select(select_folder)
            return imap
        except BaseException as e:
            last = e
            if verbose:
                print(f"[imap] connect attempt {i+1}/{attempts} failed: {e}")
            time.sleep(delay)
    assert last is not None
    raise last


def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    email_addr = (appt.get("customer_email") or "").strip()
    if not email_addr:
        return None

    payload: Dict[str, Any] = {
        "email": email_addr,
        "first_name": (appt.get("customer_first_name") or "").strip() or None,
        "last_name": (appt.get("customer_last_name") or "").strip() or None,
        #"phone": (appt.get("customer_phone") or "").strip() or None,
    }
    try:
        return upsert_contact({k: v for k, v in payload.items() if v is not None})
    except Exception as exc:
        print(f"[error] failed to upsert contact for {email_addr}: {exc}")
        return None


def _is_already_booked(appt: Dict[str, Any]) -> bool:
    email_addr = (appt.get("customer_email") or "").strip()
    if not email_addr:
        return False
    contact = find_contact_by_email(email_addr)
    if not contact:
        return False
    return (contact.get("stage") or "").strip().lower() == "booked"


def _mark_contact_booked(contact: Dict[str, Any]) -> None:
    contact_id = contact.get("id")
    if not contact_id:
        return
    try:
        update_contact(contact_id, {"stage": "Booked"})
    except Exception as exc:
        print(f"[warn] failed to set stage=Booked for {contact_id}: {exc}")


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

    imap = _imap_connect_with_retry(
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
                if verbose:
                    print(subj)
                if not _looks_like_booking(subj):
                    continue

                body = message_body_text(msg)
                appt = parse_mail(body)
                if verbose:
                    print("[info] appointment parsed")

                if _is_already_booked(appt):
                    processed = True
                    if verbose:
                        who = appt.get("customer_email") or appt.get("customer_name")
                        print(f"[skip] already booked for {who}")
                    continue

                if not (appt.get("customer_email") or "").strip():
                    if verbose:
                        print("[warn] booking mail missing customer email; skipping")
                    continue

                contact = _ensure_contact(appt)
                if contact:
                    _mark_contact_booked(contact)
                    handled += 1
                    processed = True
                    if verbose:
                        who = contact.get("Email") or contact.get("Contact Id")
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
