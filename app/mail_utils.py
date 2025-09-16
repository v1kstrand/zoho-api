from __future__ import annotations

import email
import imaplib
import re
from typing import Optional

_HTML_SANITIZE_PATTERNS = {
    "scripts": re.compile(r"(?is)<(script|style).*?>.*?</\\1>"),
    "br": re.compile(r"(?is)<br\\s*/?>"),
    "tags": re.compile(r"(?is)<[^>]+>")
}


def ensure_mailbox(imap: imaplib.IMAP4, mailbox: Optional[str]) -> None:
    """Create mailbox when missing; ignore errors if it already exists."""
    if not mailbox:
        return
    try:
        imap.create(mailbox)
    except Exception:
        pass


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

