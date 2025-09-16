import email

from app.tasks.mail_unsub_poll import _addr_from, _matches_stop, _chunks


def make_message(subject: str = "", from_header: str = "user@example.com"):
    msg = email.message.EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_header
    msg.set_content("stop")
    return msg


def test_addr_from_handles_simple_address():
    msg = make_message()
    assert _addr_from(msg) == "user@example.com"


def test_addr_from_parses_name_address():
    msg = make_message(from_header="User Name <user@example.com>")
    assert _addr_from(msg) == "user@example.com"


def test_matches_stop_subject_hit():
    msg = make_message(subject="Please unsubscribe")
    assert _matches_stop(msg, "") is True


def test_matches_stop_body_hit():
    msg = make_message(subject="General question")
    body = "Hello\nSTOP\nThanks"
    assert _matches_stop(msg, body) is True


def test_matches_stop_ignores_quoted_lines():
    msg = make_message(subject="General question")
    body = "> stop\nThanks"
    assert _matches_stop(msg, body) is False


def test_chunks_iterates_in_batches():
    data = list(range(5))
    batches = list(_chunks(data, n=2))
    assert batches == [[0, 1], [2, 3], [4]]
