from __future__ import annotations

import email
import imaplib
import re
import time
from typing import Optional
import os

IMAP_HOST = "imap.zoho.eu"
IMAP_USER = os.environ["ZOHO_IMAP_USER"]
IMAP_PASS = os.environ["ZOHO_IMAP_PASSWORD"]
IMAP_FOLDER = os.environ["ZOHO_IMAP_FOLDER"]

_HTML_SANITIZE_PATTERNS = {
    "scripts": re.compile(r"(?is)<(script|style).*?>.*?</\1>"),
    "br": re.compile(r"(?is)<br\s*/?>"),
    "tags": re.compile(r"(?is)<[^>]+>")
}


__all__ = [
    "ensure_mailbox",
    "imap_connect_with_retry",
    "message_body_text",
    "move_message",
]


def ensure_mailbox(imap: imaplib.IMAP4, mailbox: Optional[str]) -> None:
    """Create mailbox when missing; ignore errors if it already exists."""
    if not mailbox:
        return
    try:
        imap.create(mailbox)
    except Exception:
        pass


def imap_connect_with_retry(
    host: str = IMAP_HOST,
    user: str = IMAP_USER,
    password: str = IMAP_PASS,
    select_folder: str = IMAP_FOLDER,
    ensure_folder: Optional[str] = None,
    attempts: int = 3,
    delay: float = 2.0,
    verbose: bool = True,
) -> imaplib.IMAP4_SSL:
    """Log in to IMAP with retries, ensuring target mailboxes exist."""
    last: Optional[BaseException] = None
    for attempt in range(attempts):
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
        except BaseException as exc:
            last = exc
            if verbose:
                print(f"[imap] connect attempt {attempt + 1}/{attempts} failed: {exc}")
            if attempt + 1 < attempts:
                time.sleep(delay)
    assert last is not None
    raise last


def _html_to_text(html: str) -> str:
    cleaned = _HTML_SANITIZE_PATTERNS["scripts"].sub("", html)
    cleaned = _HTML_SANITIZE_PATTERNS["br"].sub("\n", cleaned)
    cleaned = _HTML_SANITIZE_PATTERNS["tags"].sub(" ", cleaned)
    return cleaned


def message_body_text(msg: email.message.Message) -> str:
    """Best-effort plaintext extraction from a MIME message."""
    plain_parts: list[str] = []
    html_parts: list[str] = []

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        content_type = part.get_content_type()
        try:
            payload = part.get_payload(decode=True) or b""
        except Exception:
            payload = b""

        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except Exception:
            text = payload.decode("utf-8", errors="replace")

        if content_type == "text/plain":
            plain_parts.append(text)
        elif content_type == "text/html":
            html_parts.append(_html_to_text(text))

    for candidate in plain_parts:
        if "\n" in candidate or len(candidate) > 20:
            return candidate
    if plain_parts:
        return plain_parts[0]

    for candidate in html_parts:
        if "\n" in candidate or len(candidate) > 20:
            return candidate
    if html_parts:
        return html_parts[0]

    try:
        return msg.as_string()
    except Exception:
        return ""


def move_message(imap: imaplib.IMAP4, uid: str, dest: Optional[str]) -> None:
    """Copy a message to ``dest`` and flag it deleted in the current mailbox."""
    if not dest:
        return
    try:
        ensure_mailbox(imap, dest)
        imap.uid("COPY", uid, dest)
        imap.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
        imap.expunge()
    except Exception as exc:
        print(f"[imap] move failed for uid={uid}: {exc}")
