# app/tasks/mail_bookings_trigger.py
from __future__ import annotations

import email
import imaplib
import os
import re
import time
from email.header import decode_header, make_header
from typing import Any, Dict,  Optional

import requests

# Reuse your API client + shared settings
from ..api_client import (get_access_token, 
                          REQ_TIMEOUT,     
                            bigin_post,
                            search_contact_by_email,
                            create_pipeline_record_for_contact,
                            get_contact_by_email,
                            update_records_by_contact_id,
                            update_contact_fields)

# ---------------------------------------------------------------------
# Config / Environment
# ---------------------------------------------------------------------
IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")            # required
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")        # required

BOOKING_FOLDER  = os.getenv("BOOKING_FOLDER", "INBOX")
MOVE_BOOKING_TO = os.getenv("BOOKING_MOVE_TO", None) #"Processed/Bookings")

BOOKING_SUBJECT_HINT = re.compile(
    r"^(?=.*\b(?:booking|appointment|bokning|möte)\b)(?=.*\b(?:new|confirmed|scheduled|ny|bekräftad|schemalagd)\b)",
    re.I,
)
# -----------------------------
# Bigin upserts (Contacts & Records)
# -----------------------------
def _ensure_contact(appt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Find or create a Contact from a Bookings appointment.
    """    
    if c := search_contact_by_email(appt["email"]):
        return c

    body = {
        "data": [{
            "Email": appt["email"],
            "First_Name": appt["first_name"],
            "Last_Name":  appt["last_name"],
            "Phone": appt.get("phone"),
        }],
        "trigger": [],
    }
    res = bigin_post("Contacts", body)
    
    data = (res.get("data") or [])
    contact = None
    for _ in range(20):
        contact = search_contact_by_email(appt["email"])
        time.sleep(10)
        if contact:
            break
        print("sleep:", contact, appt["email"])
    else:
        raise ValueError(f"Could not find Contact with Email={appt['email']}")
    create_pipeline_record_for_contact(contact["id"], "Booked")
    if data and data[0].get("status") == "success":
        return contact
    return None


def _upsert_pipeline_record(contact) -> dict:
    contact_id = contact["id"]
    update_contact_fields(contact_id, {"Status": "Booked"})
    update_records_by_contact_id(contact_id, {"Stage": "Booked"})
    
# ---------------------------------------------------------------------
# IMAP helpers
# ---------------------------------------------------------------------
def _ensure_mailbox(imap: imaplib.IMAP4_SSL, mailbox: str) -> None:
    try:
        imap.create(mailbox)
    except Exception:
        pass

def _imap_connect_with_retry(host: str, user: str, password: str, select_folder: str,
                             *, ensure_mailbox: Optional[str] = None,
                             attempts: int = 3, delay: float = 2.0,
                             verbose: bool = True) -> imaplib.IMAP4_SSL:
    last: Optional[BaseException] = None
    for i in range(attempts):
        try:
            imap = imaplib.IMAP4_SSL(host)
            imap.login(user, password)
            if ensure_mailbox:
                _ensure_mailbox(imap, ensure_mailbox)
            typ, _ = imap.select(select_folder)
            if typ != "OK":
                _ensure_mailbox(imap, select_folder)
                imap.select(select_folder)
            return imap
        except BaseException as e:
            last = e
            if verbose:
                print(f"[imap] connect attempt {i+1}/{attempts} failed: {e}")
            time.sleep(delay)
    assert last is not None
    raise last


def _get_text(msg: email.message.Message) -> str:
    """Best-effort plaintext extraction from MIME message."""
    parts: list[str] = []
    for p in msg.walk():
        if p.get_content_maintype() == "multipart":
            continue
        ctype = p.get_content_type()
        try:
            payload = p.get_payload(decode=True) or b""
        except Exception:
            payload = b""
        charset = p.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except Exception:
            text = payload.decode("utf-8", errors="replace")
        if ctype == "text/plain":
            parts.append(text)
        elif ctype == "text/html":
            parts.append(re.sub(r"<[^>]+>", " ", text))  # rough strip
    if parts:
        for t in parts:
            if "\n" in t or len(t) > 20:
                return t
        return parts[0]
    try:
        return msg.as_string()
    except Exception:
        return ""

def _move_message(imap: imaplib.IMAP4_SSL, uid: str, dest: str) -> None:
    try:
        _ensure_mailbox(imap, dest)
        imap.uid("COPY", uid, dest)
        imap.uid("STORE", uid, "+FLAGS", r"(\Deleted)")
        imap.expunge()
    except Exception as e:
        print(f"[imap] move failed for uid={uid}: {e}")

# ---------------------------------------------------------------------
# Mail classification & parsing
# ---------------------------------------------------------------------
def _subject(msg: email.message.Message) -> str:
    return str(make_header(decode_header(msg.get("Subject", ""))))

def _looks_like_booking(subj: str) -> bool:
    return bool(BOOKING_SUBJECT_HINT.search(subj))

def _extract_booking_id(body: str) -> Optional[str]:
    m = re.search(r"The invoice number is:\s*(.*?)\s*\.", body, re.I | re.S)
    if not m:
        return None
    bid = m.group(1).strip()
    return f"#{bid}" if not bid.startswith("#") else bid




# ---------------------------------------------------------------------
# Bookings helpers
# ---------------------------------------------------------------------
def get_appointment(booking_id: str) -> Dict[str, Any]:
    """
    Fetch a single appointment by booking_id using GET (like:
    curl -G --data-urlencode "booking_id=#VI-00027").
    Returns {} if not found.
    """
    at, api = get_access_token()
    url = api.rstrip("/") + "/bookings/v1/json/getappointment"
    headers = {"Authorization": f"Zoho-oauthtoken {at}"}  # no JSON content-type
    r = requests.get(url, headers=headers, params={"booking_id": booking_id}, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    
    email = j["response"]["returnvalue"]["customer_email"]
    company = j["response"]["returnvalue"]["customer_more_info"]["Company"]
    full_name = j["response"]["returnvalue"]["customer_name"]
    phone = j["response"]["returnvalue"]["customer_contact_no"]
    full_name = full_name.strip().split()
    first_name = full_name[0]
    last_name = " ".join(full_name[1:])
    join_link = j["response"]["returnvalue"]["meeting_info"]["join_link"]
    
    appt = {
        "email": email,
        "company": company,
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "join_link": join_link
    }
    return appt


def send_email_invite(appt):
    # TODO : Complete this (later not now)
    join_link = appt["join_link"]
    return
    
    

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def process_bookings_once(verbose: bool = True) -> int:
    """
    Scan BOOKING_FOLDER for unseen booking mails, upsert Contact + Pipeline, and move
    each processed mail to MOVE_BOOKING_TO. Returns the count of handled booking mails.
    """
    assert IMAP_USER and IMAP_PASS, "Set ZOHO_IMAP_USER and ZOHO_IMAP_PASSWORD in environment"

    imap = _imap_connect_with_retry(
        IMAP_HOST, IMAP_USER, IMAP_PASS, BOOKING_FOLDER,
        ensure_mailbox=MOVE_BOOKING_TO, verbose=verbose
    )
    handled = 0

    try:
        typ, data = imap.uid("SEARCH", None, "(UNSEEN)")
        uids = (data[0].decode().split() if typ == "OK" and data and data[0] else [])
        if verbose:
            print(f"[imap] {len(uids)} unseen in {BOOKING_FOLDER}")

        for uid in uids:
            try:
                typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
                if typ != "OK" or not fetch or not fetch[0]:
                    continue

                msg = email.message_from_bytes(fetch[0][1])
                subj = _subject(msg)
                body = _get_text(msg)

                if not _looks_like_booking(subj):
                    continue

                # Data we might extract from the mail itself
                bid = _extract_booking_id(body)
                appt: Optional[Dict[str, Any]] = None

                # (1) Booking ID path
                if bid:
                    if verbose:
                        print(f"[bookings] Booking ID in mail: {bid}")
                    try:
                        appt = get_appointment(bid)
                    except Exception as e:
                        if verbose:
                            print(f"[bookings] get_appointment({bid}) failed: {e}")
                            
                    
                    c = _ensure_contact(appt)
                    if c:
                        _upsert_pipeline_record(c)
                        handled += 1
                        if verbose:
                            who = c.get("Email") or c.get("email") or c.get("id")
                            print(f"[ok] upserted pipeline for {who}")
                    else:
                        if verbose:
                            print("[warn] could not ensure contact (likely no email in appointment)")
                else:
                    if verbose:
                        print(f"[skip] no appointment match for uid={uid} subj={subj!r}")

            finally:
                if MOVE_BOOKING_TO:
                    _move_message(imap, uid, MOVE_BOOKING_TO)

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
