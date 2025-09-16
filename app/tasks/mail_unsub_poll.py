# app/tasks/mail_unsub_poll.py
from __future__ import annotations
from itertools import islice

import imaplib, email, re, time, os
from email.header import decode_header, make_header
from typing import Optional

# Reuse your API client
from ..api_client import (
    search_contact_by_email,
    update_contact_fields,
    bigin_post,
    list_records_by_contact_id,
    bigin_put,
)
from ..mail_utils import ensure_mailbox, message_body_text, move_message

IMAP_HOST = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER = os.environ.get("ZOHO_IMAP_USER")  # e.g. info@yourdomain.com
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD")  # app-specific password recommended
IMAP_FOLDER = os.getenv("ZOHO_IMAP_FOLDER", "INBOX")  # or create/use 'Unsubscribe'
MOVE_TO = os.getenv("ZOHO_IMAP_MOVE_TO", "Processed/Unsubscribe")  # auto-created if missing
DRY_RUN = os.getenv("UNSUB_DRY_RUN", "false").lower() == "true"

# Simple patterns: exact "stop", "unsubscribe", Swedish variants, etc.
LINE_PATTERNS = [
    r"^\s*stop\s*$",
    r"^\s*unsubscribe\s*$",
    r"^\s*avregistrera\s*$",
    r"^\s*sluta\s*$",
    r"^\s*sluta skicka\s*$",
]
SUBJECT_HINT = r"\b(stop|unsubscribe|avregistrera|sluta)\b"


def _addr_from(msg: email.message.Message) -> Optional[str]:
    from_hdr = str(make_header(decode_header(msg.get("From", ""))))
    m = re.search(r"<([^>]+)>", from_hdr)
    return (m.group(1) if m else from_hdr).strip().lower() or None


def _matches_stop(msg: email.message.Message, body: str) -> bool:
    # quick subject hint
    subj = str(make_header(decode_header(msg.get("Subject", "")))).lower()
    if re.search(SUBJECT_HINT, subj):
        return True
    # scan lines (trim quotes)
    for raw in body.splitlines():
        line = raw.strip().lower()
        # ignore quoted history
        if line.startswith(">"):
            continue
        if any(re.match(p, line) for p in LINE_PATTERNS):
            return True
    return False


def _note(contact_id: str, content: str) -> None:
    try:
        bigin_post(f"Contacts/{contact_id}/Notes", {"data": [{"Note_Content": content}]})
    except Exception:
        pass  # non-fatal


def _chunks(seq, n: int = 100):
    it = iter(seq)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


def process_once(verbose: bool = True) -> None:
    assert IMAP_USER and IMAP_PASS, "Set ZOHO_IMAP_USER and ZOHO_IMAP_PASSWORD in environment"
    logged_in_tries = 10
    imap: Optional[imaplib.IMAP4_SSL] = None

    while logged_in_tries > 0:
        try:
            imap = imaplib.IMAP4_SSL(IMAP_HOST)
            imap.login(IMAP_USER, IMAP_PASS)
            imap.select(IMAP_FOLDER)
            ensure_mailbox(imap, MOVE_TO)
            break
        except Exception as e:
            if verbose:
                print(f"Login failed: {e}")
            time.sleep(3)
            logged_in_tries -= 1

    if not imap:
        return

    try:
        typ, data = imap.uid("SEARCH", None, "(UNSEEN)")
        uids = (data[0].decode().split() if typ == "OK" and data and data[0] else [])
        handled = 0

        for uid in uids:
            typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
            if typ != "OK" or not fetch or not fetch[0]:
                continue

            raw = fetch[0][1]
            msg = email.message_from_bytes(raw)
            sender = _addr_from(msg)
            body = message_body_text(msg)

            if not sender:
                continue

            if not _matches_stop(msg, body):
                continue  # not a STOP mail; leave it

            if verbose:
                print(f"[STOP] from {sender} (uid {uid})")

            if not DRY_RUN:
                contact = search_contact_by_email(sender)
                if contact and contact.get("id"):
                    update_contact_fields(contact["id"], {"Email_Opt_Out": True})

                    records = list_records_by_contact_id(contact["id"], fields=["id", "Stage"])

                    def _norm(value: Optional[str]) -> str:
                        return (value or "").strip().lower()

                    to_drop = [
                        {"id": r["id"], "Stage": "Dropped"}
                        for r in records
                        if r.get("id") and _norm(r.get("Stage")) not in {"booked", "dropped"}
                    ]

                    if to_drop:
                        successes, failures = 0, []
                        for batch in _chunks(to_drop, 100):
                            try:
                                resp = bigin_put("Pipelines", {"data": batch, "trigger": []})
                                for item in resp.get("data", []):
                                    if item.get("status") == "success":
                                        successes += 1
                                    else:
                                        failures.append(
                                            {
                                                "id": item.get("details", {}).get("id"),
                                                "error": item.get("message"),
                                            }
                                        )
                            except Exception as exc:
                                failures.extend([{"id": row["id"], "error": str(exc)} for row in batch])

                    _note(
                        contact["id"],
                        f"Unsubscribed via email STOP from {sender} at {time.strftime('%Y-%m-%d %H:%M:%S')}",
                    )

                move_message(imap, uid, MOVE_TO)

            handled += 1

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
    process_once()
