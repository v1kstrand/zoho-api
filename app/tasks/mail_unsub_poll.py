# app/tasks/mail_unsub_poll.py
from __future__ import annotations
import imaplib, email, re, sys, json, time, os
from email.header import decode_header, make_header
from typing import Optional
from pathlib import Path

# Reuse your API client
from ..api_client import (search_contact_by_email,
                          update_contact_fields,
                          bigin_post,
                          list_records_by_contact_id,
                          bigin_put)

IMAP_HOST   = os.getenv("ZOHO_IMAP_HOST", "imap.zoho.eu")
IMAP_USER   = os.environ.get("ZOHO_IMAP_USER")           # e.g. info@yourdomain.com
IMAP_PASS   = os.environ.get("ZOHO_IMAP_PASSWORD")       # app-specific password recommended
IMAP_FOLDER = os.getenv("ZOHO_IMAP_FOLDER", "INBOX")     # or create/use 'Unsubscribe'
MOVE_TO     = os.getenv("ZOHO_IMAP_MOVE_TO", "Processed/Unsubscribe")  # auto-created if missing
DRY_RUN     = os.getenv("UNSUB_DRY_RUN", "false").lower() == "true"

STATE_FILE  = Path(os.getenv("UNSUB_STATE", ".unsub_seen.json"))

# Simple patterns: exact "stop", "unsubscribe", Swedish variants, etc.
LINE_PATTERNS = [
    r"^\s*stop\s*$", r"^\s*unsubscribe\s*$",
    r"^\s*avregistrera\s*$", r"^\s*sluta\s*$", r"^\s*sluta skicka\s*$"
]
SUBJECT_HINT = r"\b(stop|unsubscribe|avregistrera|sluta)\b"

def _load_state() -> set[str]:
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text()))
        except Exception:
            return set()
    return set()

def _save_state(s: set[str]) -> None:
    STATE_FILE.write_text(json.dumps(sorted(s), indent=2))

def _get_text(msg: email.message.Message) -> str:
    # prefer text/plain, fallback to stripping html tags roughly
    for part in msg.walk():
        ctype = part.get_content_type()
        if part.get_content_maintype() == "multipart":
            continue
        try:
            payload = part.get_payload(decode=True) or b""
        except Exception:
            payload = b""
        text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
        if ctype == "text/plain":
            return text
        if ctype == "text/html":
            html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", text)
            text = re.sub(r"(?is)<br\s*/?>", "\n", html)
            text = re.sub(r"(?is)<[^>]+>", "", text)
            return text
    return ""

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

def _ensure_mailbox(imap: imaplib.IMAP4_SSL, name: str):
    typ, _ = imap.list()
    existing = [ln.decode().split(' "/" ')[-1].strip('"') for ln in (_ or [])]
    if name not in existing:
        imap.create(name)

def _move_message(imap: imaplib.IMAP4_SSL, uid: str, dest: str):
    # UID COPY, then mark deleted in source, EXPUNGE
    imap.uid("COPY", uid, dest)
    imap.uid("STORE", uid, "+FLAGS", r"(\Deleted)")
    imap.expunge()

def _note(contact_id: str, content: str):
    try:
        bigin_post(f"Contacts/{contact_id}/Notes", {"data": [{"Note_Content": content}]})
    except Exception:
        pass  # non-fatal
    
from itertools import islice

def _chunks(seq, n=100):
    it = iter(seq)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

def process_once(verbose: bool = True):
    assert IMAP_USER and IMAP_PASS, "Set ZOHO_IMAP_USER and ZOHO_IMAP_PASSWORD in environment"
    seen = _load_state()
    logged_in_tries = 10
    imap = None
    while logged_in_tries > 0:
        try:
            imap = imaplib.IMAP4_SSL(IMAP_HOST)
            imap.login(IMAP_USER, IMAP_PASS)
            imap.select(IMAP_FOLDER)
            _ensure_mailbox(imap, MOVE_TO)
            break
        except Exception as e:
            if verbose:
                print(f"Login failed: {e}")
            time.sleep(3)
            logged_in_tries -= 1
            
    if logged_in_tries == 0:
        try: imap.close()
        except Exception: pass
        try: imap.logout()
        except Exception: pass
        return

    # Pick unseen + small subject pre-filter to save bandwidth
    typ, data = imap.uid("SEARCH", None, '(UNSEEN)')
    uids = (data[0].decode().split() if typ == "OK" and data and data[0] else [])
    handled = 0

    for uid in uids:
        if uid in seen:
            continue
        typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
        if typ != "OK" or not fetch or not fetch[0]:
            continue
        raw = fetch[0][1]
        msg = email.message_from_bytes(raw)
        sender = _addr_from(msg)
        body = _get_text(msg)

        if not sender:
            seen.add(uid)
            continue

        if not _matches_stop(msg, body):
            continue  # not a STOP mail; leave it

        if verbose:
            print(f"[STOP] from {sender} (uid {uid})")

        if not DRY_RUN:
            # Find contact and opt-out
            c = search_contact_by_email(sender)
            if c and c.get("id"):
                update_contact_fields(c["id"], {"Email_Opt_Out": True})

                records = list_records_by_contact_id(c["id"], fields=["id", "Stage"])
                def _norm(s): return (s or "").strip().lower()
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
                                    failures.append({"id": item.get("details", {}).get("id"), "error": item.get("message")})
                        except Exception as e:
                            failures.extend([{"id": row["id"], "error": str(e)} for row in batch])
                
                _note(c["id"], f"Unsubscribed via email STOP from {sender} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            _move_message(imap, uid, MOVE_TO)

        seen.add(uid)
        handled += 1

    _save_state(seen)
    imap.close()
    imap.logout()
    if verbose:
        print(f"Handled {handled} STOP email(s).")

if __name__ == "__main__":
    process_once()
