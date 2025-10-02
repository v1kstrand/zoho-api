# app/tasks/mail_unsub_poll.py
from __future__ import annotations

import email
import os
import re
import time
from email.header import decode_header, make_header
from typing import Optional

from dotenv import load_dotenv

from ..api_client import append_contact_note, find_contact_by_email, update_contact, get_contact_field
from ..mail_utils import imap_connect_with_retry, message_body_text, move_message, IMAP_FOLDER

load_dotenv()

MOVE_TO = os.environ["UNSUB_IMAP_MOVE_TO"]
stop_kw = os.environ["UNSUB_STOP_KEYWORDS"]
STOP_KEYWORDS = {word.strip().lower() for word in stop_kw.split(",") if word.strip()}
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


def unsub_process_once(verbose: bool = True) -> None:
    """Scan for STOP mails and mark matching contacts as unsubscribed."""

    imap = imap_connect_with_retry(
        ensure_folder=MOVE_TO,
        verbose=verbose,
    )

    try:
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

            contact = find_contact_by_email(sender)
            if not contact:
                if verbose:
                    print(f"[warn] no contact for {sender}")
                continue

            update_contact(sender, {"unsub": "True"})
            stage_value = get_contact_field(sender, "stage").strip().lower()
            if stage_value not in {"booked", "dropped"}:
                update_contact(sender, {"stage": "dropped"})

            note = f"Unsubscribed at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            append_contact_note(sender, note)

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

