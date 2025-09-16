import email

from app.mail_utils import message_body_text, move_message


def make_message(parts):
    msg = email.message.EmailMessage()
    for maintype, subtype, payload in parts:
        msg.add_attachment(payload, maintype=maintype, subtype=subtype)
    return msg


def test_message_body_text_prefers_plaintext():
    msg = email.message.EmailMessage()
    msg.set_type("multipart/alternative")
    msg.add_alternative("<html><body><p>Hello</p></body></html>", subtype="html")
    msg.add_alternative("Hello", subtype="plain")

    assert message_body_text(msg).strip() == "Hello"


def test_message_body_text_falls_back_to_html():
    msg = email.message.EmailMessage()
    msg.set_type("multipart/alternative")
    msg.add_alternative("<html><body><p>Hello <b>World</b></p></body></html>", subtype="html")

    text = message_body_text(msg)
    assert "Hello" in text
    assert "World" in text

def test_move_message_calls_imap_operations(monkeypatch):
    calls = []

    class FakeIMAP:
        def __init__(self):
            self.created = []

        def create(self, mailbox):
            self.created.append(mailbox)

        def uid(self, *args):
            calls.append(args)

    imap = FakeIMAP()

    move_message(imap, "42", "Processed/Unsubscribe")

    assert "Processed/Unsubscribe" in imap.created
    assert ("COPY", "42", "Processed/Unsubscribe") in calls
    assert ("STORE", "42", "+FLAGS", "(\\Deleted)") in calls


def test_move_message_no_destination_noop():
    class FakeIMAP:
        def __init__(self):
            self.created = []
            self.calls = []

        def create(self, mailbox):
            self.created.append(mailbox)

        def uid(self, *args):
            self.calls.append(args)

    imap = FakeIMAP()
    move_message(imap, "1", None)

    assert imap.created == []
    assert getattr(imap, "calls") == []

