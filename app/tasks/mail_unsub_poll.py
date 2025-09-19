# app/tasks/mail_unsub_poll.py
from __future__ import annotations

import email
import imaplib
import os
import re
import time
from email.header import decode_header, make_header
from typing import Optional

from ..api_client_csv import append_contact_note, find_contact_by_email, update_contact, get_contact_field
from ..mail_utils import ensure_mailbox, message_body_text, move_message

IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")
IMAP_FOLDER = os.getenv("ZOHO_IMAP_FOLDER", "INBOX")
MOVE_TO = os.getenv("ZOHO_IMAP_MOVE_TO", "Processed/Unsubscribe")
DRY_RUN = os.getenv("UNSUB_DRY_RUN", "false").lower() == "true"

STOP_KEYWORDS = {"stop", "unsubscribe", "avregistrera", "sluta", "sluta skicka"}
SUBJECT_HINT = re.compile(r"\b(stop|unsubscribe|avregistrera|sluta)\b", re.I)


def _addr_from(msg: email.message.Message) -> Optional[str]:
    """Return the sender email (lowercased) extracted from the header."""
    from_hdr = str(make_header(decode_header(msg.get("From", ""))))
    match = re.search(r"<([^>]+)>", from_hdr)
    addr = (match.group(1) if match else from_hdr).strip().lower()
    return addr or None


def _looks_like_stop(msg: email.message.Message, body: str) -> bool:
    """Heuristic check for STOP/UNSUBSCRIBE intent in subject or body."""
    subject = str(make_header(decode_header(msg.get("Subject", "")))).lower()
    if SUBJECT_HINT.search(subject):
        return True

    for raw_line in body.splitlines():
        line = raw_line.strip().lower()
        if not line or line.startswith(">"):
            continue
        if line in STOP_KEYWORDS:
            return True
    return False


def process_once(verbose: bool = False) -> None:
    """Scan for STOP mails and mark matching contacts as unsubscribed."""
    if not IMAP_USER or not IMAP_PASS:
        raise RuntimeError("Set ZOHO_IMAP_USER and ZOHO_IMAP_PASSWORD in environment")

    imap = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        imap.login(IMAP_USER, IMAP_PASS)
        imap.select(IMAP_FOLDER)
        if MOVE_TO:
            ensure_mailbox(imap, MOVE_TO)

        typ, data = imap.uid("SEARCH", None, "(UNSEEN)")
        uids = data and data[0].decode().split() if typ == "OK" and data else []
        if verbose:
            print(f"[imap] {len(uids)} unseen messages in {IMAP_FOLDER}")

        handled = 0
        for uid in uids:
            typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
            if typ != "OK" or not fetch or not fetch[0]:
                continue

            msg = email.message_from_bytes(fetch[0][1])
            sender = _addr_from(msg)
            body = message_body_text(msg)
            if not sender or not _looks_like_stop(msg, body):
                continue

            if verbose:
                print(f"[STOP] {sender}")

            handled += 1
            if DRY_RUN:
                continue

            contact = find_contact_by_email(sender)
            if not contact or not contact.get("id"):
                if verbose:
                    print(f"[warn] no contact for {sender}")
                continue

            cid = contact["id"]
            update_contact(cid, {"unsub": True})
            if get_contact_field(cid, "stage") != "booked":
                update_contact(cid, {"stage": "dropped"})
            
            note = (
                f"Unsubscribed via email STOP from {sender} at "
                f"{time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            append_contact_note(cid, note)

            if MOVE_TO:
                move_message(imap, uid, MOVE_TO)

        if verbose:
            print(f"Handled {handled} STOP email(s).")

    finally:
        try:
            imap.close()
        except Exception:
            pass
        try:
            imap.logout()
        except Exception:
            pass


if __name__ == "__main__":
    process_once(verbose=True)

