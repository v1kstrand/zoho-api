# app/tasks/mail_bookings_trigger.py
from __future__ import annotations

import email
import imaplib
import os
import re
import time
from email.header import decode_header, make_header
from typing import Any, Dict, Optional

# Reuse your API client + shared settings
from ..api_client import (
    bigin_post,
    search_contact_by_email,
    update_records_by_contact_id,
    update_contact_fields,
    list_records_by_contact_id,
)
from ..parse_mail import parse_mail
from ..mail_utils import ensure_mailbox, message_body_text, move_message

# ---------------------------------------------------------------------
# Config / Environment
# ---------------------------------------------------------------------
IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")             # required
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")         # required

BOOKING_FOLDER = os.getenv("BOOKING_FOLDER", "INBOX")
MOVE_BOOKING_TO = os.getenv("BOOKING_MOVE_TO", "Processed/Bookings")     # e.g., ""

BOOKING_SUBJECT_HINT = re.compile(
    r"""^(?:\s*(?:re|fwd)\s*:\s*)*              # optional Re:/Fwd:
        vds\s+discovery\s+project\s+between\s+
        david\s+vikstrand\s+and\s+              # fixed prefix
        (?P<cust>.+?)\s*$                       # capture the remainder (customer name)
    """,
    re.I | re.X,
)


# ---------------------------------------------------------------------
# IMAP helpers
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


# ---------------------------------------------------------------------
# Bigin upserts (Contacts & Records)
# ---------------------------------------------------------------------
def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Find or create a Contact from a Bookings appointment dict shaped by get_appointment().
    Expected keys in appt: email, first_name, last_name, phone.
    """
    num_tries = 3
    for _ in range(num_tries):
        try:
            email_addr = (appt.get("customer_email") or "").strip()
            if not email_addr:
                return None

            if c := search_contact_by_email(email_addr):
                return c

            contact_payload = {
                key: value
                for key, value in (
                    ("Email", email_addr),
                    ("First_Name", (appt.get("customer_first_name") or "").strip() or None),
                    ("Last_Name", (appt.get("customer_last_name") or "").strip() or None),
                    ("Phone", (appt.get("customer_phone") or "").strip() or None),
                )
                if value
            }
            body = {
                "data": [contact_payload],
                "trigger": [],
            }
            res = bigin_post("Contacts", body)
            data = (res.get("data") or [])

            # Poll for the freshly created record to become searchable
            contact: Optional[Dict[str, Any]] = None
            for _ in range(20):
                contact = search_contact_by_email(email_addr)
                if contact:
                    break
                time.sleep(10)
            else:
                raise ValueError(f"Could not find Contact with Email={email_addr}")

            if data and data[0].get("status") == "success":
                return contact
            return None
        except Exception as e:
            print(f"[error] Exception in _ensure_contact: {e}")
            time.sleep(10)


def _is_already_booked(appt: Dict[str, Any]) -> bool:
    try:
        email_addr = (appt.get("customer_email") or "").strip()
        if not email_addr:
            return False

        contact = search_contact_by_email(email_addr)
        if not contact or not contact.get("id"):
            return False

        records = list_records_by_contact_id(contact["id"], fields=["Stage"])
        for record in records:
            stage = (record.get("Stage") or "").strip().lower()
            if stage == "booked":
                return True
        return False
    except Exception as e:
        print(f"[error] Exception in _is_already_booked: {e}")


def _upsert_pipeline_record(contact: Dict[str, Any]) -> None:
    """
    Minimal: mark contact + their related records as Booked.
    """
    try:
        contact_id = contact["id"]
        update_contact_fields(contact_id, {"Status": "Booked"})
        update_records_by_contact_id(contact_id, {"Stage": "Booked"})
    except Exception as e:
        print(f"[error] Exception in _upsert_pipeline_record: {e}")
        

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def _subject(msg: email.message.Message) -> str:
    return str(make_header(decode_header(msg.get("Subject", ""))))


def _looks_like_booking(subj: str) -> bool:
    return bool(BOOKING_SUBJECT_HINT.search(subj))

def process_bookings_once(verbose: bool = True) -> int:
    """
    Scan BOOKING_FOLDER for unseen booking mails, upsert Contact + Pipeline, and
    (optionally) move processed mails. Returns the count of handled booking mails.
    """
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
                body = message_body_text(msg)

                if not _looks_like_booking(subj):
                    continue

                appt = parse_mail(body)
                if verbose:
                    print("[info] appt successfully parsed")
                
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

                c = _ensure_contact(appt)
                if c:
                    _upsert_pipeline_record(c)
                    handled += 1
                    processed = True
                    if verbose:
                        who = c.get("Email") or c.get("email") or c.get("id")
                        print(f"[ok] upserted pipeline for {who}")
                else:
                    if verbose:
                        print("[warn] could not ensure contact (likely no email in appointment)")
            except Exception as e:
                if verbose:
                    print(f"[error] failed to handle booking uid {uid}: {e}")
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
